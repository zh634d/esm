/*
Copyright 2016 Medcl (m AT medcl.net)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/cheggaaa/pb"
	"io"
	"io/ioutil"
	"reflect"
	"strings"
	"sync"
	"time"

	log "github.com/cihub/seelog"
)

type BulkOperation uint8

const (
	opIndex BulkOperation = iota
	opDelete
)

// SYNC_BATCH_SIZE syncByMap 分批大小：每批处理多少条 src 文档
// 越大 → dst scroll 次数越少（更快），但内存越高
// 越小 → 内存越低，但 dst 需要重复 scroll 更多次
const SYNC_BATCH_SIZE = 200000

func (op BulkOperation) String() string {
	switch op {
	case opIndex:
		return "opIndex"
	case opDelete:
		return "opDelete"
	default:
		return fmt.Sprintf("unknown:%d", op)
	}
}

func (m *Migrator) recoveryIndexSettings(sourceIndexRefreshSettings map[string]interface{}) {
	//update replica and refresh_interval
	for name, interval := range sourceIndexRefreshSettings {
		tempIndexSettings := getEmptyIndexSettings()
		tempIndexSettings["settings"].(map[string]interface{})["index"].(map[string]interface{})["refresh_interval"] = interval
		//tempIndexSettings["settings"].(map[string]interface{})["index"].(map[string]interface{})["number_of_replicas"] = 1
		m.TargetESAPI.UpdateIndexSettings(name, tempIndexSettings)
		if m.Config.Refresh {
			m.TargetESAPI.Refresh(name)
		}
	}
}

func (m *Migrator) ClusterVersion(host string, auth *Auth, proxy string) (*ClusterVersion, []error) {

	url := fmt.Sprintf("%s", host)
	resp, body, errs := Get(url, auth, proxy)

	if resp != nil && resp.Body != nil {
		io.Copy(ioutil.Discard, resp.Body)
		defer resp.Body.Close()
	}

	if errs != nil {
		log.Error(errs)
		return nil, errs
	}

	log.Debug(body)

	version := &ClusterVersion{}
	err := json.Unmarshal([]byte(body), version)

	if err != nil {
		log.Error(body, errs)
		return nil, errs
	}
	return version, nil
}

func (m *Migrator) ParseEsApi(isSource bool, host string, authStr string, proxy string, compress bool) ESAPI {
	var auth *Auth = nil
	if len(authStr) > 0 && strings.Contains(authStr, ":") {
		authArray := strings.Split(authStr, ":")
		auth = &Auth{User: authArray[0], Pass: authArray[1]}
		if isSource {
			m.SourceAuth = auth
		} else {
			m.TargetAuth = auth
		}
	}

	esVersion, errs := m.ClusterVersion(host, auth, proxy)
	if errs != nil {
		return nil
	}

	esInfo := "dest"
	if isSource {
		esInfo = "source"
	}

	log.Infof("%s es version: %s", esInfo, esVersion.Version.Number)
	if strings.HasPrefix(esVersion.Version.Number, "7.") {
		log.Debug("es is V7,", esVersion.Version.Number)
		api := new(ESAPIV7)
		api.Host = host
		api.Compress = compress
		api.Auth = auth
		api.HttpProxy = proxy
		api.Version = esVersion
		return api
		//migrator.SourceESAPI = api
	} else if strings.HasPrefix(esVersion.Version.Number, "6.") {
		log.Debug("es is V6,", esVersion.Version.Number)
		api := new(ESAPIV6)
		api.Host = host
		api.Compress = compress
		api.Auth = auth
		api.HttpProxy = proxy
		api.Version = esVersion
		return api
		//migrator.SourceESAPI = api
	} else if strings.HasPrefix(esVersion.Version.Number, "5.") {
		log.Debug("es is V5,", esVersion.Version.Number)
		api := new(ESAPIV5)
		api.Host = host
		api.Compress = compress
		api.Auth = auth
		api.HttpProxy = proxy
		api.Version = esVersion
		return api
		//migrator.SourceESAPI = api
	} else {
		log.Debug("es is not V5,", esVersion.Version.Number)
		api := new(ESAPIV0)
		api.Host = host
		api.Compress = compress
		api.Auth = auth
		api.HttpProxy = proxy
		api.Version = esVersion
		return api
	}
}

func (m *Migrator) ClusterReady(api ESAPI) (*ClusterHealth, bool) {
	health := api.ClusterHealth()

	if !m.Config.WaitForGreen {
		return health, true
	}

	if health.Status == "red" {
		return health, false
	}

	if m.Config.WaitForGreen == false && health.Status == "yellow" {
		return health, true
	}

	if health.Status == "green" {
		return health, true
	}

	return health, false
}

func (m *Migrator) NewBulkWorker(docCount *int, pb *pb.ProgressBar, wg *sync.WaitGroup) {

	log.Debug("start es bulk worker")

	bulkItemSize := 0
	mainBuf := bytes.Buffer{}
	docBuf := bytes.Buffer{}
	docEnc := json.NewEncoder(&docBuf)

	idleDuration := 5 * time.Second
	idleTimeout := time.NewTimer(idleDuration)
	defer idleTimeout.Stop()

	taskTimeOutDuration := 5 * time.Minute
	taskTimeout := time.NewTimer(taskTimeOutDuration)
	defer taskTimeout.Stop()

READ_DOCS:
	for {
		idleTimeout.Reset(idleDuration)
		taskTimeout.Reset(taskTimeOutDuration)
		select {
		case docI, open := <-m.DocChan:
			var err error
			log.Trace("read doc from channel,", docI)
			// this check is in case the document is an error with scroll stuff
			if status, ok := docI["status"]; ok {
				if status.(int) == 404 {
					log.Error("error: ", docI["response"])
					continue
				}
			}

			// sanity check
			for _, key := range []string{"_index", "_type", "_source", "_id"} {
				if _, ok := docI[key]; !ok {
					break READ_DOCS
				}
			}

			var tempDestIndexName string
			var tempTargetTypeName string
			tempDestIndexName = docI["_index"].(string)
			tempTargetTypeName = docI["_type"].(string)

			if m.Config.TargetIndexName != "" {
				tempDestIndexName = m.Config.TargetIndexName
			}

			if m.Config.OverrideTypeName != "" {
				tempTargetTypeName = m.Config.OverrideTypeName
			}

			doc := Document{
				Index:  tempDestIndexName,
				Type:   tempTargetTypeName,
				source: docI["_source"].(map[string]interface{}),
				Id:     docI["_id"].(string),
			}

			if m.Config.RegenerateID {
				doc.Id = ""
			}

			if m.Config.RenameFields != "" {
				kvs := strings.Split(m.Config.RenameFields, ",")
				for _, i := range kvs {
					fvs := strings.Split(i, ":")
					oldField := strings.TrimSpace(fvs[0])
					newField := strings.TrimSpace(fvs[1])
					if oldField == "_type" {
						doc.source[newField] = docI["_type"].(string)
					} else {
						v := doc.source[oldField]
						doc.source[newField] = v
						delete(doc.source, oldField)
					}
				}
			}

			// add doc "_routing" if exists
			if _, ok := docI["_routing"]; ok {
				str, ok := docI["_routing"].(string)
				if ok && str != "" {
					doc.Routing = str
				}
			}

			// if channel is closed flush and gtfo
			if !open {
				goto WORKER_DONE
			}

			// sanity check
			if len(doc.Index) == 0 || len(doc.Type) == 0 {
				log.Errorf("failed decoding document: %+v", doc)
				continue
			}

			// encode the doc and and the _source field for a bulk request
			post := map[string]Document{
				"index": doc,
			}
			if err = docEnc.Encode(post); err != nil {
				log.Error(err)
			}
			if err = docEnc.Encode(doc.source); err != nil {
				log.Error(err)
			}

			// append the doc to the main buffer
			mainBuf.Write(docBuf.Bytes())
			// reset for next document
			bulkItemSize++
			(*docCount)++
			docBuf.Reset()

			// if we approach the 100mb es limit, flush to es and reset mainBuf
			if mainBuf.Len()+docBuf.Len() > (m.Config.BulkSizeInMB * 1024 * 1024) {
				goto CLEAN_BUFFER
			}

		case <-idleTimeout.C:
			log.Debug("5s no message input")
			goto CLEAN_BUFFER
		case <-taskTimeout.C:
			log.Warn("5m no message input, close worker")
			goto WORKER_DONE
		}

		goto READ_DOCS

	CLEAN_BUFFER:
		m.TargetESAPI.Bulk(&mainBuf)
		log.Trace("clean buffer, and execute bulk insert")
		pb.Add(bulkItemSize)
		bulkItemSize = 0
		if m.Config.SleepSecondsAfterEachBulk > 0 {
			time.Sleep(time.Duration(m.Config.SleepSecondsAfterEachBulk) * time.Second)
		}
	}
WORKER_DONE:
	if docBuf.Len() > 0 {
		mainBuf.Write(docBuf.Bytes())
		bulkItemSize++
	}
	m.TargetESAPI.Bulk(&mainBuf)
	log.Trace("bulk insert")
	pb.Add(bulkItemSize)
	bulkItemSize = 0
	wg.Done()
}

func (m *Migrator) bulkRecords(bulkOp BulkOperation, dstEsApi ESAPI, targetIndex string, targetType string, diffDocMaps map[string]interface{}) error {
	//var err error
	docCount := 0
	bulkItemSize := 0
	mainBuf := bytes.Buffer{}
	docBuf := bytes.Buffer{}
	docEnc := json.NewEncoder(&docBuf)

	//var tempDestIndexName string
	//var tempTargetTypeName string

	for docId, docData := range diffDocMaps {
		docI := docData.(map[string]interface{})
		log.Debugf("now will bulk %s docId=%s, docData=%+v", bulkOp, docId, docData)
		//tempDestIndexName = docI["_index"].(string)
		//tempTargetTypeName = docI["_type"].(string)
		var strOperation string
		doc := Document{
			Index: targetIndex,
			Type:  targetType,
			Id:    docId, // docI["_id"].(string),
		}

		switch bulkOp {
		case opIndex:
			doc.source = docI // docI["_source"].(map[string]interface{}),
			strOperation = "index"
		case opDelete:
			strOperation = "delete"
			//do nothing
		}

		// encode the doc and and the _source field for a bulk request

		post := map[string]Document{
			strOperation: doc,
		}
		_ = Verify(docEnc.Encode(post))
		if bulkOp == opIndex {
			_ = Verify(docEnc.Encode(doc.source))
		}
		// append the doc to the main buffer
		mainBuf.Write(docBuf.Bytes())
		// reset for next document
		bulkItemSize++
		docCount++
		docBuf.Reset()
	}

	if mainBuf.Len() > 0 {
		_ = Verify(dstEsApi.Bulk(&mainBuf))
	}
	return nil
}

// SyncBetweenIndex 根据源 ES 版本自动选择同步策略
func (m *Migrator) SyncBetweenIndex(srcEsApi ESAPI, dstEsApi ESAPI, cfg *Config) {
	srcVersion := srcEsApi.ClusterVersion().Version.Number
	if strings.HasPrefix(srcVersion, "5.") {
		log.Infof("source ES version %s, using map-based sync (no sort required)", srcVersion)
		m.syncByMap(srcEsApi, dstEsApi, cfg)
	} else {
		log.Infof("source ES version %s, using sorted-pointer sync (optimized for large indexes)", srcVersion)
		m.syncBySortedPointer(srcEsApi, dstEsApi, cfg)
	}
}

// syncByMap 分批 Map 比较，不依赖排序，适用于 ES 5.x（避免 _uid fielddata 报错）
// 内存优化：三阶段处理，避免全量 src 文档同时驻留内存
//
//	Phase 1: scroll src → 只收集 _id 到 srcIdSet（轻量级）
//	Phase 2: scroll dst → 删除 dst 中不在 srcIdSet 的文档
//	Phase 3: scroll src 分批（每批 SYNC_BATCH_SIZE 条），每批 scroll dst 全量比较
func (m *Migrator) syncByMap(srcEsApi ESAPI, dstEsApi ESAPI, cfg *Config) {
	srcType := ""
	addCount := 0
	updateCount := 0
	deleteCount := 0
	srcRecordIndex := 0
	dstRecordIndex := 0
	batchSize := cfg.DocBufferCount

	// ========== Phase 1: scroll src，只收集 _id 到 srcIdSet ==========
	log.Info("Phase 1: scrolling source to collect _id set: ", cfg.SourceIndexNames)
	srcIdSet := make(map[string]bool)
	{
		srcScroll, scrollErr := srcEsApi.NewScroll(cfg.SourceIndexNames, cfg.ScrollTime, cfg.DocBufferCount, cfg.Query,
			"", 0, cfg.ScrollSliceSize, cfg.Fields) // 不排序，避免 ES 5.x _uid fielddata 报错
		if scrollErr != nil {
			log.Infof("can not scroll for source index: %s, reason:%s", cfg.SourceIndexNames, scrollErr.Error())
			return
		}
		srcTotal := srcScroll.GetHitsTotal()
		log.Infof("src total count=%d", srcTotal)
		srcBar := pb.New(1).Prefix("Phase1-Ids")
		srcBar.Total = int64(srcTotal)
		srcBar.Start()

		for {
			docs := srcScroll.GetDocs()
			for _, srcDocI := range docs {
				docMap := srcDocI.(map[string]interface{})
				srcId := docMap["_id"].(string)
				srcIdSet[srcId] = true
				srcRecordIndex++
				if srcType == "" {
					srcType = docMap["_type"].(string)
				}
			}
			srcBar.Add(len(docs))
			if len(docs) == 0 || len(docs) < cfg.DocBufferCount {
				break
			}
			srcScroll = VerifyWithResult(srcEsApi.NextScroll(cfg.ScrollTime, srcScroll.GetScrollId())).(ScrollAPI)
		}
		_ = Verify(srcEsApi.DeleteScroll(srcScroll.GetScrollId()))
		srcBar.FinishPrint("Phase 1 End")
	}
	log.Infof("Phase 1 done: collected %d unique _ids, srcType=%s", len(srcIdSet), srcType)

	// ========== Phase 2: scroll dst，删除 dst 中不在 srcIdSet 的文档 ==========
	log.Info("Phase 2: scrolling dest to find deletions: ", cfg.TargetIndexName)
	{
		dstScroll, scrollErr := dstEsApi.NewScroll(cfg.TargetIndexName, cfg.ScrollTime, cfg.DocBufferCount, cfg.Query,
			"", 0, cfg.ScrollSliceSize, cfg.Fields) // 不排序
		if scrollErr != nil {
			log.Infof("can not scroll for dest index: %s, reason:%s, skip deletion phase", cfg.TargetIndexName, scrollErr.Error())
		} else {
			log.Infof("dst total count=%d", dstScroll.GetHitsTotal())
			dstBar := pb.New(1).Prefix("Phase2-Del")
			dstBar.Total = int64(dstScroll.GetHitsTotal())
			dstBar.Start()

			deleteBatch := make(map[string]interface{})
			for {
				docs := dstScroll.GetDocs()
				if len(docs) == 0 {
					break
				}
				dstRecordIndex += len(docs)

				for _, dstDocI := range docs {
					docMap := dstDocI.(map[string]interface{})
					destId := docMap["_id"].(string)

					if !srcIdSet[destId] {
						// dst 有这个 doc 但 src 没有 → 删除
						deleteBatch[destId] = map[string]interface{}{
							"_id":   destId,
							"_type": docMap["_type"],
						}
						deleteCount++
					}

					// 分批执行删除
					if len(deleteBatch) >= batchSize {
						log.Debugf("batch bulk delete %d records", len(deleteBatch))
						_ = Verify(m.bulkRecords(opDelete, dstEsApi, cfg.TargetIndexName, srcType, deleteBatch))
						deleteBatch = make(map[string]interface{})
					}
				}
				dstBar.Add(len(docs))

				if len(docs) < cfg.DocBufferCount {
					break
				}
				dstScroll = VerifyWithResult(dstEsApi.NextScroll(cfg.ScrollTime, dstScroll.GetScrollId())).(ScrollAPI)
			}
			// flush 剩余删除
			if len(deleteBatch) > 0 {
				log.Debugf("batch bulk delete %d records (final)", len(deleteBatch))
				_ = Verify(m.bulkRecords(opDelete, dstEsApi, cfg.TargetIndexName, srcType, deleteBatch))
			}
			_ = Verify(dstEsApi.DeleteScroll(dstScroll.GetScrollId()))
			dstBar.FinishPrint("Phase 2 End")
		}
	}
	log.Infof("Phase 2 done: deleted %d docs from dst", deleteCount)

	// ========== Phase 3: 分批 scroll src + 全量 scroll dst 比较 ==========
	log.Info("Phase 3: batched src scroll + dst comparison")
	{
		srcScroll, scrollErr := srcEsApi.NewScroll(cfg.SourceIndexNames, cfg.ScrollTime, cfg.DocBufferCount, cfg.Query,
			"", 0, cfg.ScrollSliceSize, cfg.Fields) // 不排序
		if scrollErr != nil {
			log.Infof("can not scroll for source index (phase 3): %s, reason:%s", cfg.SourceIndexNames, scrollErr.Error())
			return
		}

		srcBatchMap := make(map[string]interface{}) // 当前批次的 src 文档 {_id: _source}
		srcBatchCount := 0
		batchNum := 0

		// processSrcBatch: 对当前 srcBatchMap scroll dst 全量比较
		processSrcBatch := func() {
			batchNum++
			log.Infof("Phase 3 batch %d: srcBatchMap size=%d, scrolling dst for comparison", batchNum, len(srcBatchMap))

			// scroll dst 全量，与本批 src 比较
			dstScroll, scrollErr := dstEsApi.NewScroll(cfg.TargetIndexName, cfg.ScrollTime, cfg.DocBufferCount, cfg.Query,
				"", 0, cfg.ScrollSliceSize, cfg.Fields)
			if scrollErr != nil {
				log.Infof("can not scroll dest index (phase 3 batch %d): %s, treating all src as new", batchNum, scrollErr.Error())
				// dst 不可用，直接把本批全部当新增
				addBuf := make(map[string]interface{})
				for id, src := range srcBatchMap {
					addBuf[id] = src
					if len(addBuf) >= batchSize {
						_ = Verify(m.bulkRecords(opIndex, dstEsApi, cfg.TargetIndexName, srcType, addBuf))
						addCount += len(addBuf)
						addBuf = make(map[string]interface{})
					}
				}
				if len(addBuf) > 0 {
					_ = Verify(m.bulkRecords(opIndex, dstEsApi, cfg.TargetIndexName, srcType, addBuf))
					addCount += len(addBuf)
				}
				return
			}

			updateBatch := make(map[string]interface{})
			for {
				docs := dstScroll.GetDocs()
				if len(docs) == 0 {
					break
				}

				for _, dstDocI := range docs {
					docMap := dstDocI.(map[string]interface{})
					destId := docMap["_id"].(string)
					dstSource := docMap["_source"]

					if srcSource, found := srcBatchMap[destId]; found {
						// src 本批也有这个 doc，比较内容
						if !reflect.DeepEqual(srcSource, dstSource) {
							updateBatch[destId] = srcSource
							updateCount++
						}
						// 匹配完成，从 srcBatchMap 删除（剩余的才是新增）
						delete(srcBatchMap, destId)
					}

					// 分批执行更新
					if len(updateBatch) >= batchSize {
						log.Debugf("batch bulk index %d records (update)", len(updateBatch))
						_ = Verify(m.bulkRecords(opIndex, dstEsApi, cfg.TargetIndexName, srcType, updateBatch))
						updateBatch = make(map[string]interface{})
					}
				}

				if len(docs) < cfg.DocBufferCount {
					break
				}
				dstScroll = VerifyWithResult(dstEsApi.NextScroll(cfg.ScrollTime, dstScroll.GetScrollId())).(ScrollAPI)
			}
			// flush 剩余更新
			if len(updateBatch) > 0 {
				log.Debugf("batch bulk index %d records (update, final)", len(updateBatch))
				_ = Verify(m.bulkRecords(opIndex, dstEsApi, cfg.TargetIndexName, srcType, updateBatch))
			}
			_ = Verify(dstEsApi.DeleteScroll(dstScroll.GetScrollId()))

			// srcBatchMap 剩余 = src 有但 dst 没有 → 新增
			if len(srcBatchMap) > 0 {
				addBuf := make(map[string]interface{})
				for id, src := range srcBatchMap {
					addBuf[id] = src
					if len(addBuf) >= batchSize {
						_ = Verify(m.bulkRecords(opIndex, dstEsApi, cfg.TargetIndexName, srcType, addBuf))
						addCount += len(addBuf)
						addBuf = make(map[string]interface{})
					}
				}
				if len(addBuf) > 0 {
					_ = Verify(m.bulkRecords(opIndex, dstEsApi, cfg.TargetIndexName, srcType, addBuf))
					addCount += len(addBuf)
				}
			}
			log.Infof("Phase 3 batch %d done: add=%d, update=%d (cumulative)", batchNum, addCount, updateCount)
		}

		// 逐条读 src，积累到 SYNC_BATCH_SIZE 后处理一批
		for {
			docs := srcScroll.GetDocs()
			for _, srcDocI := range docs {
				docMap := srcDocI.(map[string]interface{})
				srcId := docMap["_id"].(string)
				srcSource := docMap["_source"]
				srcBatchMap[srcId] = srcSource
				srcBatchCount++

				if srcBatchCount >= SYNC_BATCH_SIZE {
					processSrcBatch()
					srcBatchMap = make(map[string]interface{})
					srcBatchCount = 0
				}
			}
			if len(docs) == 0 || len(docs) < cfg.DocBufferCount {
				break
			}
			srcScroll = VerifyWithResult(srcEsApi.NextScroll(cfg.ScrollTime, srcScroll.GetScrollId())).(ScrollAPI)
		}
		_ = Verify(srcEsApi.DeleteScroll(srcScroll.GetScrollId()))

		// 处理最后一批（不足 SYNC_BATCH_SIZE 的剩余部分）
		if len(srcBatchMap) > 0 {
			processSrcBatch()
		}
	}

	if cfg.SleepSecondsAfterEachBulk > 0 {
		time.Sleep(time.Duration(cfg.SleepSecondsAfterEachBulk) * time.Second)
	}

	log.Infof("sync %s(%d) to %s(%d), add=%d, update=%d, delete=%d",
		cfg.SourceIndexNames, srcRecordIndex, cfg.TargetIndexName, dstRecordIndex,
		addCount, updateCount, deleteCount)
}

// syncBySortedPointer 双指针比较，依赖 _id 排序，内存效率高，适用于 ES 6.x/7.x
func (m *Migrator) syncBySortedPointer(srcEsApi ESAPI, dstEsApi ESAPI, cfg *Config) {
	srcDocMaps := make(map[string]interface{})
	dstDocMaps := make(map[string]interface{})
	diffDocMaps := make(map[string]interface{})

	srcRecordIndex := 0
	dstRecordIndex := 0
	srcType := ""
	var srcScroll ScrollAPI = nil
	var dstScroll ScrollAPI = nil
	var emptyScroll = &EmptyScroll{}
	lastSrcId := ""
	lastDestId := ""
	needScrollSrc := true
	needScrollDest := true

	addCount := 0
	updateCount := 0
	deleteCount := 0

	srcBar := pb.New(1).Prefix("Progress")

	for {
		if srcScroll == nil {
			var err error
			srcScroll, err = srcEsApi.NewScroll(cfg.SourceIndexNames, cfg.ScrollTime, cfg.DocBufferCount, cfg.Query,
				cfg.SortField, 0, cfg.ScrollSliceSize, cfg.Fields)
			if err != nil {
				log.Infof("can not scroll for source index: %s, reason:%s", cfg.SourceIndexNames, err.Error())
				return
			}
			log.Infof("src total count=%d", srcScroll.GetHitsTotal())
			srcBar.Total = int64(srcScroll.GetHitsTotal())
			srcBar.Start()
		} else if needScrollSrc {
			srcScroll = VerifyWithResult(srcEsApi.NextScroll(cfg.ScrollTime, srcScroll.GetScrollId())).(ScrollAPI)
		}

		if dstScroll == nil {
			var err error
			dstScroll, err = dstEsApi.NewScroll(cfg.TargetIndexName, cfg.ScrollTime, cfg.DocBufferCount, cfg.Query,
				cfg.SortField, 0, cfg.ScrollSliceSize, cfg.Fields)
			if err != nil {
				log.Infof("can not scroll for dest index: %s, reason:%s", cfg.TargetIndexName, err.Error())
				dstScroll = emptyScroll
			} else {
				log.Infof("dst total count=%d", dstScroll.GetHitsTotal())
			}
		} else if needScrollDest {
			dstScroll = VerifyWithResult(dstEsApi.NextScroll(cfg.ScrollTime, dstScroll.GetScrollId())).(ScrollAPI)
		}

		// 从目标 index 中查询,并放入 destMap
		if needScrollDest {
			for idx, dstDocI := range dstScroll.GetDocs() {
				destId := dstDocI.(map[string]interface{})["_id"].(string)
				dstSource := dstDocI.(map[string]interface{})["_source"]
				lastDestId = destId
				log.Debugf("dst [%d]: dstId=%s", dstRecordIndex+idx, destId)

				if srcSource, found := srcDocMaps[destId]; found {
					delete(srcDocMaps, destId)
					if !reflect.DeepEqual(srcSource, dstSource) {
						diffDocMaps[destId] = srcSource
						updateCount++
					}
				} else {
					dstDocMaps[destId] = dstSource
				}
			}
			dstRecordIndex += len(dstScroll.GetDocs())
		}

		// 将 src 的当前批次查出并放入 map
		if needScrollSrc {
			for idx, srcDocI := range srcScroll.GetDocs() {
				srcId := srcDocI.(map[string]interface{})["_id"].(string)
				srcSource := srcDocI.(map[string]interface{})["_source"]
				srcType = srcDocI.(map[string]interface{})["_type"].(string)
				lastSrcId = srcId
				log.Debugf("src [%d]: srcId=%s", srcRecordIndex+idx, srcId)

				if len(lastDestId) == 0 {
					diffDocMaps[srcId] = srcSource
					addCount++
				} else if dstSource, ok := dstDocMaps[srcId]; ok {
					if !reflect.DeepEqual(srcSource, dstSource) {
						diffDocMaps[srcId] = srcSource
						updateCount++
					}
					delete(dstDocMaps, srcId)
				} else {
					if srcId < lastDestId {
						diffDocMaps[srcId] = srcSource
						addCount++
					} else {
						srcDocMaps[srcId] = srcSource
					}
				}
				srcBar.Increment()
			}
			srcRecordIndex += len(srcScroll.GetDocs())
		}

		if len(diffDocMaps) > 0 {
			log.Debugf("now will bulk index %d records", len(diffDocMaps))
			_ = Verify(m.bulkRecords(opIndex, dstEsApi, cfg.TargetIndexName, srcType, diffDocMaps))
			diffDocMaps = make(map[string]interface{})
		}

		if lastSrcId == lastDestId {
			needScrollSrc = true
			needScrollDest = true
		} else if len(lastDestId) == 0 || (lastSrcId < lastDestId || (needScrollDest == true && len(dstScroll.GetDocs()) == 0)) {
			needScrollSrc = true
			needScrollDest = false
		} else if lastSrcId > lastDestId || (needScrollSrc == true && len(srcScroll.GetDocs()) == 0) {
			needScrollSrc = false
			needScrollDest = true
		} else {
			panic("TODO:")
		}

		log.Debugf("lastSrcId=%s, lastDestId=%s, "+
			"needScrollSrc=%t, len(srcScroll.GetDocs()=%d, "+
			"needScrollDest=%t, len(dstScroll.GetDocs())=%d",
			lastSrcId, lastDestId,
			needScrollSrc, len(srcScroll.GetDocs()),
			needScrollDest, len(dstScroll.GetDocs()))

		if (!needScrollSrc || (len(srcScroll.GetDocs()) == 0 || len(srcScroll.GetDocs()) < cfg.DocBufferCount)) &&
			(!needScrollDest || (len(dstScroll.GetDocs()) == 0 || len(dstScroll.GetDocs()) < cfg.DocBufferCount)) {
			log.Debugf("can not find more, will quit, and index %d, delete %d", len(srcDocMaps), len(dstDocMaps))

			if len(srcDocMaps) > 0 {
				addCount += len(srcDocMaps)
				_ = Verify(m.bulkRecords(opIndex, dstEsApi, cfg.TargetIndexName, srcType, srcDocMaps))
			}
			if len(dstDocMaps) > 0 {
				deleteCount += len(dstDocMaps)
				_ = Verify(m.bulkRecords(opDelete, dstEsApi, cfg.TargetIndexName, srcType, dstDocMaps))
			}
			break
		}

		if cfg.SleepSecondsAfterEachBulk > 0 {
			time.Sleep(time.Duration(cfg.SleepSecondsAfterEachBulk) * time.Second)
		}
	}
	_ = Verify(srcEsApi.DeleteScroll(srcScroll.GetScrollId()))
	_ = Verify(dstEsApi.DeleteScroll(dstScroll.GetScrollId()))

	srcBar.FinishPrint("Source End")

	log.Infof("sync %s(%d) to %s(%d), add=%d, update=%d, delete=%d",
		cfg.SourceIndexNames, srcRecordIndex, cfg.TargetIndexName, dstRecordIndex,
		addCount, updateCount, deleteCount)
}
