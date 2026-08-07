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

// syncByMap 纯 Map 比较，不依赖排序，适用于 ES 5.x（避免 _uid fielddata 报错）
func (m *Migrator) syncByMap(srcEsApi ESAPI, dstEsApi ESAPI, cfg *Config) {
	// 内存优化：
	// - srcDocMaps: 存全量 src 文档（必须，用于比较）
	// - 不存 dstDocMaps：边 scroll dst 边比较，匹配后立即从 srcDocMaps 删除释放内存
	// - batchIndex/batchDelete: 分批 bulk 写入，不攒全量差异
	srcDocMaps := make(map[string]interface{}) // _id => _source（随比较逐步缩小）

	srcRecordIndex := 0
	dstRecordIndex := 0
	srcType := ""

	addCount := 0
	updateCount := 0
	deleteCount := 0

	batchSize := cfg.DocBufferCount

	// ========== Step 1: scroll src 全量，放入 srcDocMaps ==========
	log.Info("start scrolling source index: ", cfg.SourceIndexNames)
	srcBar := pb.New(1).Prefix("Source")
	{
		srcScroll, scrollErr := srcEsApi.NewScroll(cfg.SourceIndexNames, cfg.ScrollTime, cfg.DocBufferCount, cfg.Query,
			"", 0, cfg.ScrollSliceSize, cfg.Fields) // 不排序，避免 ES 5.x _uid fielddata 报错
		if scrollErr != nil {
			log.Infof("can not scroll for source index: %s, reason:%s", cfg.SourceIndexNames, scrollErr.Error())
			return
		}
		log.Infof("src total count=%d", srcScroll.GetHitsTotal())
		srcBar.Total = int64(srcScroll.GetHitsTotal())
		srcBar.Start()

		for {
			docs := srcScroll.GetDocs()
			for _, srcDocI := range docs {
				docMap := srcDocI.(map[string]interface{})
				srcId := docMap["_id"].(string)
				srcSource := docMap["_source"]
				srcType = docMap["_type"].(string)
				srcDocMaps[srcId] = srcSource
				srcRecordIndex++
				log.Debugf("src [%d]: srcId=%s", srcRecordIndex, srcId)
			}
			srcBar.Add(len(docs))
			if len(docs) == 0 || len(docs) < cfg.DocBufferCount {
				break
			}
			srcScroll = VerifyWithResult(srcEsApi.NextScroll(cfg.ScrollTime, srcScroll.GetScrollId())).(ScrollAPI)
		}
		_ = Verify(srcEsApi.DeleteScroll(srcScroll.GetScrollId()))
	}
	srcBar.FinishPrint("Source End")
	log.Infof("source scroll done, total=%d, srcDocMaps size=%d", srcRecordIndex, len(srcDocMaps))

	// ========== Step 2: scroll dst 分批，边比较边 bulk ==========
	log.Info("start scrolling dest index: ", cfg.TargetIndexName)
	{
		dstScroll, scrollErr := dstEsApi.NewScroll(cfg.TargetIndexName, cfg.ScrollTime, cfg.DocBufferCount, cfg.Query,
			"", 0, cfg.ScrollSliceSize, cfg.Fields) // 不排序
		if scrollErr != nil {
			log.Infof("can not scroll for dest index: %s, reason:%s, will treat as empty", cfg.TargetIndexName, scrollErr.Error())
		} else {
			log.Infof("dst total count=%d", dstScroll.GetHitsTotal())

			for {
				docs := dstScroll.GetDocs()
				if len(docs) == 0 {
					break
				}
				dstRecordIndex += len(docs)

				// 每批 dst 文档的增/改/删缓冲区
				batchIndex := make(map[string]interface{})
				batchDelete := make(map[string]interface{})

				for _, dstDocI := range docs {
					docMap := dstDocI.(map[string]interface{})
					destId := docMap["_id"].(string)
					dstSource := docMap["_source"]
					log.Debugf("dst [%d]: dstId=%s", dstRecordIndex, destId)

					if srcSource, found := srcDocMaps[destId]; found {
						// src 也有这个 doc，比较内容
						if !reflect.DeepEqual(srcSource, dstSource) {
							// 内容不同，需要更新（用 src 版本覆盖 dst）
							batchIndex[destId] = srcSource
							updateCount++
						}
						// 匹配完成，从 srcDocMaps 删除释放内存
						delete(srcDocMaps, destId)
					} else {
						// src 没有这个 doc，需要删除（只存 _id + _type，不存 _source）
						batchDelete[destId] = map[string]interface{}{
							"_id":   destId,
							"_type": docMap["_type"],
						}
						deleteCount++
					}
				}

				// 批量写入更新
				if len(batchIndex) > 0 {
					log.Debugf("batch bulk index %d records (update)", len(batchIndex))
					_ = Verify(m.bulkRecords(opIndex, dstEsApi, cfg.TargetIndexName, srcType, batchIndex))
				}

				// 批量删除
				if len(batchDelete) > 0 {
					log.Debugf("batch bulk delete %d records", len(batchDelete))
					_ = Verify(m.bulkRecords(opDelete, dstEsApi, cfg.TargetIndexName, srcType, batchDelete))
				}

				log.Debugf("dst batch done, srcDocMaps remaining=%d", len(srcDocMaps))

				if len(docs) < cfg.DocBufferCount {
					break
				}
				dstScroll = VerifyWithResult(dstEsApi.NextScroll(cfg.ScrollTime, dstScroll.GetScrollId())).(ScrollAPI)
			}
			_ = Verify(dstEsApi.DeleteScroll(dstScroll.GetScrollId()))
		}
	}
	log.Infof("dest scroll done, total=%d, srcDocMaps remaining=%d (these are new docs)", dstRecordIndex, len(srcDocMaps))

	// ========== Step 3: srcDocMaps 剩余 = src 有但 dst 没有 → 新增 ==========
	if len(srcDocMaps) > 0 {
		addCount = len(srcDocMaps)
		// 分批写入，避免单次 bulk 过大
		addBuf := make(map[string]interface{})
		for id, src := range srcDocMaps {
			addBuf[id] = src
			if len(addBuf) >= batchSize {
				log.Debugf("batch bulk index %d records (add)", len(addBuf))
				_ = Verify(m.bulkRecords(opIndex, dstEsApi, cfg.TargetIndexName, srcType, addBuf))
				addBuf = make(map[string]interface{})
			}
		}
		if len(addBuf) > 0 {
			log.Debugf("batch bulk index %d records (add, final)", len(addBuf))
			_ = Verify(m.bulkRecords(opIndex, dstEsApi, cfg.TargetIndexName, srcType, addBuf))
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
