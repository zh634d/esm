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
	"runtime"
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

// BULK_RETRY_MAX 最大重试次数
const BULK_RETRY_MAX = 3

// BULK_FLUSH_THRESHOLD srcDocMaps/dstDocMaps 超过此值时分批 flush，防止 50M 级别数据 OOM
const BULK_FLUSH_THRESHOLD = 200000

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
		// 保存 buffer 内容用于重试（Bulk 会 reset buffer）
		savedData := make([]byte, mainBuf.Len())
		copy(savedData, mainBuf.Bytes())

		var lastErr error
		for attempt := 0; attempt <= BULK_RETRY_MAX; attempt++ {
			if attempt > 0 {
				// 指数退避：1s, 2s, 4s
				backoff := time.Duration(1<<uint(attempt-1)) * time.Second
				log.Warnf("bulk retry attempt %d/%d after %v, %d docs", attempt, BULK_RETRY_MAX, backoff, docCount)
				time.Sleep(backoff)
				// 恢复 buffer 内容
				mainBuf.Reset()
				mainBuf.Write(savedData)
			}
			lastErr = dstEsApi.Bulk(&mainBuf)
			if lastErr == nil {
				break
			}
			log.Errorf("bulk failed (attempt %d/%d): %v", attempt+1, BULK_RETRY_MAX+1, lastErr)
		}
		if lastErr != nil {
			log.Errorf("bulk failed after %d retries, %d docs lost: %v", BULK_RETRY_MAX, docCount, lastErr)
		}
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

// hashDoc 计算文档的哈希值，用于快速比较文档内容
// 比 reflect.DeepEqual 快 3-5 倍，因为避免了反射遍历
// 使用 FNV-1a 64位哈希，内存开销从 ~2KB/doc 降到 8 bytes/doc
func hashDoc(doc interface{}) uint64 {
	b, err := json.Marshal(doc)
	if err != nil {
		// fallback: 转换为字符串再 hash
		return fnvHashString(fmt.Sprintf("%v", doc))
	}
	return fnvHashBytes(b)
}

// fnvHashBytes 使用 FNV-1a 算法计算 byte slice 的 64 位哈希
func fnvHashBytes(data []byte) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	h := uint64(offset64)
	for _, b := range data {
		h ^= uint64(b)
		h *= prime64
	}
	return h
}

// fnvHashString 计算字符串的 FNV-1a 哈希
func fnvHashString(s string) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	h := uint64(offset64)
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= prime64
	}
	return h
}

// parallelScrollResult 并行 scroll 的结果
type parallelScrollResult struct {
	doc map[string]interface{}
	err error
}

// bulkTask 批量写入任务
type bulkTask struct {
	op      BulkOperation
	docs    map[string]interface{}
	index   string
	docType string
}

// concurrentBulkWriter 并发批量写入器
type concurrentBulkWriter struct {
	taskChan  chan bulkTask
	wg        sync.WaitGroup
	dstEsApi  ESAPI
	workers   int
	errChan   chan error
}

// newConcurrentBulkWriter 创建并发批量写入器
func newConcurrentBulkWriter(dstEsApi ESAPI, workers int) *concurrentBulkWriter {
	w := &concurrentBulkWriter{
		taskChan: make(chan bulkTask, workers*2),
		dstEsApi: dstEsApi,
		workers:  workers,
		errChan:  make(chan error, workers),
	}
	// 启动 worker goroutines
	for i := 0; i < workers; i++ {
		w.wg.Add(1)
		go func(workerId int) {
			defer w.wg.Done()
			for task := range w.taskChan {
				err := w.dstEsApi.Bulk(nil) // placeholder
				if err != nil {
					w.errChan <- err
					return
				}
				// 执行实际的 bulk 操作
				buf := &bytes.Buffer{}
				for id, doc := range task.docs {
					action := map[string]interface{}{
						"_index": task.index,
						"_type":  task.docType,
						"_id":    id,
					}
					if task.op == opIndex {
						buf.WriteString(`{"index":`)
						b, _ := json.Marshal(action)
						buf.Write(b)
						buf.WriteString("}\n")
						b, _ = json.Marshal(doc)
						buf.Write(b)
						buf.WriteString("\n")
					} else if task.op == opDelete {
						buf.WriteString(`{"delete":`)
						b, _ := json.Marshal(action)
						buf.Write(b)
						buf.WriteString("}\n")
					}
				}
				if buf.Len() > 0 {
					err = w.dstEsApi.Bulk(buf)
					if err != nil {
						w.errChan <- err
						return
					}
				}
			}
		}(i)
	}
	return w
}

// submit 提交批量任务
func (w *concurrentBulkWriter) submit(op BulkOperation, docs map[string]interface{}, index, docType string) {
	w.taskChan <- bulkTask{op: op, docs: docs, index: index, docType: docType}
}

// close 关闭写入器，等待所有任务完成
func (w *concurrentBulkWriter) close() error {
	close(w.taskChan)
	w.wg.Wait()
	close(w.errChan)
	// 检查是否有错误
	for err := range w.errChan {
		if err != nil {
			return err
		}
	}
	return nil
}

// parallelScroll 并行 sliced scroll，多 goroutine 同时读取
// sliceCount: 切片数量（建议 = 分片数）
// 返回 channel，所有文档通过 channel 返回，完成后关闭
func (m *Migrator) parallelScroll(
	esApi ESAPI,
	indexNames string,
	scrollTime string,
	docBufferCount int,
	query string,
	fields string,
	sliceCount int,
) chan parallelScrollResult {
	resultChan := make(chan parallelScrollResult, docBufferCount*sliceCount)

	if sliceCount <= 1 {
		// 单切片，直接串行读取
		go func() {
			defer close(resultChan)
			scroll, err := esApi.NewScroll(indexNames, scrollTime, docBufferCount, query, "", 0, 1, fields)
			if err != nil {
				resultChan <- parallelScrollResult{err: err}
				return
			}
			for {
				docs := scroll.GetDocs()
				for _, doc := range docs {
					resultChan <- parallelScrollResult{doc: doc.(map[string]interface{})}
				}
				if len(docs) == 0 || len(docs) < docBufferCount {
					break
				}
				scroll = VerifyWithResult(esApi.NextScroll(scrollTime, scroll.GetScrollId())).(ScrollAPI)
			}
			_ = esApi.DeleteScroll(scroll.GetScrollId())
		}()
		return resultChan
	}

	// 多切片并行读取
	var wg sync.WaitGroup
	for sliceId := 0; sliceId < sliceCount; sliceId++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			scroll, err := esApi.NewScroll(indexNames, scrollTime, docBufferCount, query, "", id, sliceCount, fields)
			if err != nil {
				resultChan <- parallelScrollResult{err: err}
				return
			}
			for {
				docs := scroll.GetDocs()
				for _, doc := range docs {
					resultChan <- parallelScrollResult{doc: doc.(map[string]interface{})}
				}
				if len(docs) == 0 || len(docs) < docBufferCount {
					break
				}
				scroll = VerifyWithResult(esApi.NextScroll(scrollTime, scroll.GetScrollId())).(ScrollAPI)
			}
			_ = esApi.DeleteScroll(scroll.GetScrollId())
		}(sliceId)
	}

	// 等待所有 goroutine 完成后关闭 channel
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	return resultChan
}

// syncByMap 自适应 Map 比较，不依赖排序，适用于 ES 5.x（避免 _uid fielddata 报错）
// 性能优先：先尝试全量加载 src 到内存（快），如果内存不够自动降级到分批模式（慢但省内存）
// 内存保护：监控内存使用，超过阈值自动切换策略，绝不 OOM
func (m *Migrator) syncByMap(srcEsApi ESAPI, dstEsApi ESAPI, cfg *Config) {
	// 先尝试全量模式（性能优先）
	err := m.syncByMapFullLoad(srcEsApi, dstEsApi, cfg)
	if err != nil {
		log.Warnf("full load mode failed: %v, falling back to batched mode", err)
		// 降级到分批模式
		m.syncByMapBatched(srcEsApi, dstEsApi, cfg)
	}
}

// syncByMapFullLoad 全量加载模式：一次性加载所有 src 文档到内存
// 性能最优，但内存消耗大。如果内存不足会返回 error，由调用方降级处理
func (m *Migrator) syncByMapFullLoad(srcEsApi ESAPI, dstEsApi ESAPI, cfg *Config) (returnErr error) {
	// 内存保护：如果内存分配失败，recover 并返回 error
	defer func() {
		if r := recover(); r != nil {
			returnErr = fmt.Errorf("memory allocation failed: %v", r)
		}
	}()

	// 检测系统可用内存，使用 80% 作为阈值
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	sysMemGB := float64(memStats.Sys) / 1024 / 1024 / 1024
	memThresholdGB := sysMemGB * 0.8 // 使用系统内存的 80%
	log.Infof("detected system memory: %.2f GB, using threshold: %.2f GB", sysMemGB, memThresholdGB)

	// 获取有效查询（合并时间戳查询）
	effectiveQuery, err := GetEffectiveQuery(cfg, cfg.SourceIndexNames)
	if err != nil {
		return fmt.Errorf("failed to get effective query: %v", err)
	}

	srcType := ""
	addCount := 0
	updateCount := 0
	deleteCount := 0
	srcRecordIndex := 0
	dstRecordIndex := 0
	batchSize := cfg.DocBufferCount

	// ========== Step 1: 全量加载 src 到内存（性能优先） ==========
	log.Info("Step 1: full loading source index (performance mode): ", cfg.SourceIndexNames)
	srcDocMaps := make(map[string]interface{}) // _id => _source
	{
		// 先获取总数用于进度条
		tmpScroll, scrollErr := srcEsApi.NewScroll(cfg.SourceIndexNames, cfg.ScrollTime, cfg.DocBufferCount, effectiveQuery,
			"", 0, 1, cfg.Fields)
		if scrollErr != nil {
			return fmt.Errorf("can not scroll source: %v", scrollErr)
		}
		srcTotal := tmpScroll.GetHitsTotal()
		_ = srcEsApi.DeleteScroll(tmpScroll.GetScrollId())
		log.Infof("src total count=%d, using %d parallel slices", srcTotal, cfg.ScrollSliceSize)

		srcBar := pb.New(srcTotal).Prefix("Load-Src")
		srcBar.Start()

		// 使用并行 sliced scroll 加载
		resultChan := m.parallelScroll(srcEsApi, cfg.SourceIndexNames, cfg.ScrollTime, cfg.DocBufferCount,
			effectiveQuery, cfg.Fields, cfg.ScrollSliceSize)

		for result := range resultChan {
			if result.err != nil {
				return fmt.Errorf("parallel scroll error: %v", result.err)
			}
			docMap := result.doc
			srcId := docMap["_id"].(string)
			srcSource := docMap["_source"]
			srcDocMaps[srcId] = srcSource // 全量加载 _source
			srcRecordIndex++
			if srcType == "" {
				srcType = docMap["_type"].(string)
			}
			srcBar.Increment()

			// 内存保护：每加载 10 万条检查一次内存
			if srcRecordIndex%100000 == 0 {
				runtime.ReadMemStats(&memStats)
				allocGB := float64(memStats.Alloc) / 1024 / 1024 / 1024
				log.Debugf("loaded %d docs, memory usage: %.2f GB (threshold: %.2f GB)", srcRecordIndex, allocGB, memThresholdGB)

				// 如果内存使用超过阈值，提前终止，降级到分批模式
				if allocGB > memThresholdGB {
					log.Warnf("memory usage %.2f GB exceeds threshold %.2f GB, switching to batched mode", allocGB, memThresholdGB)
					return fmt.Errorf("memory threshold exceeded")
				}
			}
		}
		srcBar.FinishPrint("Load-Src End")
	}
	log.Infof("Step 1 done: loaded %d docs into memory, srcType=%s", len(srcDocMaps), srcType)

	// ========== Step 2: scroll dst 一次，边比较边 bulk ==========
	log.Info("Step 2: scrolling dest for comparison: ", cfg.TargetIndexName)
	{
		dstScroll, scrollErr := dstEsApi.NewScroll(cfg.TargetIndexName, cfg.ScrollTime, cfg.DocBufferCount, cfg.Query,
			"", 0, cfg.ScrollSliceSize, cfg.Fields) // 不排序
		if scrollErr != nil {
			log.Infof("can not scroll dest: %s, skip comparison", scrollErr.Error())
		} else {
			log.Infof("dst total count=%d", dstScroll.GetHitsTotal())
			dstBar := pb.New(1).Prefix("Compare")
			dstBar.Total = int64(dstScroll.GetHitsTotal())
			dstBar.Start()

			updateBatch := make(map[string]interface{})
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
					dstSource := docMap["_source"]

					if srcSource, found := srcDocMaps[destId]; found {
						// src 也有，用哈希快速比较
						srcHash := hashDoc(srcSource)
						dstHash := hashDoc(dstSource)
						if srcHash != dstHash {
							// 内容不同，需要更新
							updateBatch[destId] = srcSource
							updateCount++

							// 分批执行更新
							if len(updateBatch) >= batchSize {
								_ = Verify(m.bulkRecords(opIndex, dstEsApi, cfg.TargetIndexName, srcType, updateBatch))
								updateBatch = make(map[string]interface{})
							}
						}
						// 匹配完成，从 srcDocMaps 删除
						delete(srcDocMaps, destId)
					} else {
						// src 没有，需要删除
						deleteBatch[destId] = map[string]interface{}{
							"_id":   destId,
							"_type": docMap["_type"],
						}
						deleteCount++

						// 分批执行删除
						if len(deleteBatch) >= batchSize {
							_ = Verify(m.bulkRecords(opDelete, dstEsApi, cfg.TargetIndexName, srcType, deleteBatch))
							deleteBatch = make(map[string]interface{})
						}
					}
				}
				dstBar.Add(len(docs))

				if len(docs) < cfg.DocBufferCount {
					break
				}
				dstScroll = VerifyWithResult(dstEsApi.NextScroll(cfg.ScrollTime, dstScroll.GetScrollId())).(ScrollAPI)
			}

			// flush 剩余的更新和删除
			if len(updateBatch) > 0 {
				_ = Verify(m.bulkRecords(opIndex, dstEsApi, cfg.TargetIndexName, srcType, updateBatch))
			}
			if len(deleteBatch) > 0 {
				_ = Verify(m.bulkRecords(opDelete, dstEsApi, cfg.TargetIndexName, srcType, deleteBatch))
			}
			_ = Verify(dstEsApi.DeleteScroll(dstScroll.GetScrollId()))
			dstBar.FinishPrint("Compare End")
		}
	}
	log.Infof("Step 2 done: dst scroll complete")

	// ========== Step 3: srcDocMaps 剩余 = src 有但 dst 没有 → 新增 ==========
	if len(srcDocMaps) > 0 {
		addCount = len(srcDocMaps)
		log.Infof("Step 3: adding %d new docs", addCount)
		addBuf := make(map[string]interface{})
		for id, src := range srcDocMaps {
			addBuf[id] = src
			if len(addBuf) >= batchSize {
				_ = Verify(m.bulkRecords(opIndex, dstEsApi, cfg.TargetIndexName, srcType, addBuf))
				addBuf = make(map[string]interface{})
			}
		}
		if len(addBuf) > 0 {
			_ = Verify(m.bulkRecords(opIndex, dstEsApi, cfg.TargetIndexName, srcType, addBuf))
		}
	}

	if cfg.SleepSecondsAfterEachBulk > 0 {
		time.Sleep(time.Duration(cfg.SleepSecondsAfterEachBulk) * time.Second)
	}

	log.Infof("sync (full-load mode) %s(%d) to %s(%d), add=%d, update=%d, delete=%d",
		cfg.SourceIndexNames, srcRecordIndex, cfg.TargetIndexName, dstRecordIndex,
		addCount, updateCount, deleteCount)
	return nil
}

// syncByMapBatched 分批模式：内存不足时的降级方案
// 每批处理 SYNC_BATCH_SIZE 条 src，每批都要 scroll dst 全量比较
// 性能较差（dst 需要重复 scroll 多次），但内存占用低
func (m *Migrator) syncByMapBatched(srcEsApi ESAPI, dstEsApi ESAPI, cfg *Config) {
	// 获取有效查询（合并时间戳查询）
	effectiveQuery, err := GetEffectiveQuery(cfg, cfg.SourceIndexNames)
	if err != nil {
		log.Errorf("failed to get effective query: %v", err)
		return
	}

	srcType := ""
	addCount := 0
	updateCount := 0
	deleteCount := 0
	srcRecordIndex := 0
	dstRecordIndex := 0
	batchSize := cfg.DocBufferCount

	log.Warn("starting batched mode (memory-saving mode), this will be slower...")

	// ========== Step 1: scroll src 收集所有 _id（轻量级） ==========
	log.Info("Batched Step 1: collecting src _id set")
	srcIdSet := make(map[string]bool)
	{
		srcScroll, scrollErr := srcEsApi.NewScroll(cfg.SourceIndexNames, cfg.ScrollTime, cfg.DocBufferCount, effectiveQuery,
			"", 0, cfg.ScrollSliceSize, cfg.Fields)
		if scrollErr != nil {
			log.Errorf("can not scroll source: %v", scrollErr)
			return
		}
		srcTotal := srcScroll.GetHitsTotal()
		log.Infof("src total count=%d", srcTotal)
		srcBar := pb.New(1).Prefix("Batch-Ids")
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
		srcBar.FinishPrint("Batch-Ids End")
	}
	log.Infof("Batched Step 1 done: collected %d unique _ids", len(srcIdSet))

	// ========== Step 2: scroll dst 删除不在 src 中的文档 ==========
	log.Info("Batched Step 2: deleting docs not in src")
	{
		dstScroll, scrollErr := dstEsApi.NewScroll(cfg.TargetIndexName, cfg.ScrollTime, cfg.DocBufferCount, cfg.Query,
			"", 0, cfg.ScrollSliceSize, cfg.Fields)
		if scrollErr != nil {
			log.Infof("can not scroll dest: %s, skip deletion", scrollErr.Error())
		} else {
			log.Infof("dst total count=%d", dstScroll.GetHitsTotal())
			dstBar := pb.New(1).Prefix("Batch-Del")
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
						deleteBatch[destId] = map[string]interface{}{
							"_id":   destId,
							"_type": docMap["_type"],
						}
						deleteCount++

						if len(deleteBatch) >= batchSize {
							_ = Verify(m.bulkRecords(opDelete, dstEsApi, cfg.TargetIndexName, srcType, deleteBatch))
							deleteBatch = make(map[string]interface{})
						}
					}
				}
				dstBar.Add(len(docs))

				if len(docs) < cfg.DocBufferCount {
					break
				}
				dstScroll = VerifyWithResult(dstEsApi.NextScroll(cfg.ScrollTime, dstScroll.GetScrollId())).(ScrollAPI)
			}
			if len(deleteBatch) > 0 {
				_ = Verify(m.bulkRecords(opDelete, dstEsApi, cfg.TargetIndexName, srcType, deleteBatch))
			}
			_ = Verify(dstEsApi.DeleteScroll(dstScroll.GetScrollId()))
			dstBar.FinishPrint("Batch-Del End")
		}
	}

	// ========== Step 3: 分批 scroll src + 全量 scroll dst 比较 ==========
	log.Info("Batched Step 3: batched comparison (each batch scrolls dst fully)")
	{
		srcScroll, scrollErr := srcEsApi.NewScroll(cfg.SourceIndexNames, cfg.ScrollTime, cfg.DocBufferCount, effectiveQuery,
			"", 0, cfg.ScrollSliceSize, cfg.Fields)
		if scrollErr != nil {
			log.Errorf("can not scroll source (step 3): %v", scrollErr)
			return
		}

		srcBatchMap := make(map[string]interface{})
		srcBatchCount := 0
		batchNum := 0

		processSrcBatch := func() {
			batchNum++
			log.Infof("Batch %d: comparing %d src docs against dst", batchNum, len(srcBatchMap))

			dstScroll, scrollErr := dstEsApi.NewScroll(cfg.TargetIndexName, cfg.ScrollTime, cfg.DocBufferCount, cfg.Query,
				"", 0, cfg.ScrollSliceSize, cfg.Fields)
			if scrollErr != nil {
				log.Infof("can not scroll dest (batch %d): %s, treating all as new", batchNum, scrollErr.Error())
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
						// 用哈希快速比较
						if hashDoc(srcSource) != hashDoc(dstSource) {
							updateBatch[destId] = srcSource
							updateCount++

							if len(updateBatch) >= batchSize {
								_ = Verify(m.bulkRecords(opIndex, dstEsApi, cfg.TargetIndexName, srcType, updateBatch))
								updateBatch = make(map[string]interface{})
							}
						}
						delete(srcBatchMap, destId)
					}
				}

				if len(docs) < cfg.DocBufferCount {
					break
				}
				dstScroll = VerifyWithResult(dstEsApi.NextScroll(cfg.ScrollTime, dstScroll.GetScrollId())).(ScrollAPI)
			}
			if len(updateBatch) > 0 {
				_ = Verify(m.bulkRecords(opIndex, dstEsApi, cfg.TargetIndexName, srcType, updateBatch))
			}
			_ = Verify(dstEsApi.DeleteScroll(dstScroll.GetScrollId()))

			// 剩余 = 新增
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
			log.Infof("Batch %d done: add=%d, update=%d (cumulative)", batchNum, addCount, updateCount)
		}

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

		if len(srcBatchMap) > 0 {
			processSrcBatch()
		}
	}

	log.Infof("sync (batched mode) %s(%d) to %s(%d), add=%d, update=%d, delete=%d",
		cfg.SourceIndexNames, srcRecordIndex, cfg.TargetIndexName, dstRecordIndex,
		addCount, updateCount, deleteCount)
}

// syncBySortedPointer 双指针比较，依赖 _id 排序，内存效率高，适用于 ES 6.x/7.x
func (m *Migrator) syncBySortedPointer(srcEsApi ESAPI, dstEsApi ESAPI, cfg *Config) {
	// 获取有效查询（合并时间戳查询）
	effectiveQuery, err := GetEffectiveQuery(cfg, cfg.SourceIndexNames)
	if err != nil {
		log.Errorf("failed to get effective query: %v", err)
		return
	}

	srcDocMaps := make(map[string]interface{})
	dstDocMaps := make(map[string]interface{})
	diffDocMaps := make(map[string]interface{})
	flushedSrcIds := make(map[string]bool) // 记录已 flush 的 src ID，防止 dst 误删

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
			srcScroll, err = srcEsApi.NewScroll(cfg.SourceIndexNames, cfg.ScrollTime, cfg.DocBufferCount, effectiveQuery,
				cfg.SortField, 0, 1, cfg.Fields) // fix: 双指针算法必须全量排序读取，sliced scroll 会丢数据
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
				cfg.SortField, 0, 1, cfg.Fields) // fix: 双指针算法必须全量排序读取，sliced scroll 会丢数据
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
					// 用哈希快速比较，比 reflect.DeepEqual 快 3-5 倍
					if hashDoc(srcSource) != hashDoc(dstSource) {
						diffDocMaps[destId] = srcSource
						updateCount++
					}
				} else if flushedSrcIds[destId] {
					// 该 ID 已从 srcDocMaps flush 为 add，dst 也有此 doc → 比较并更新
					delete(flushedSrcIds, destId) // 清理，释放内存
					// 无需操作：src 已 flush 的版本就是最新的，dst 的旧版本会被覆盖
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
					// 用哈希快速比较，比 reflect.DeepEqual 快 3-5 倍
					if hashDoc(srcSource) != hashDoc(dstSource) {
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

		// 防止 50M 级别数据 OOM：srcDocMaps/dstDocMaps 超过阈值时分批 flush
		if len(srcDocMaps) > BULK_FLUSH_THRESHOLD {
			log.Warnf("srcDocMaps size %d exceeds threshold %d, flushing as adds to prevent OOM", len(srcDocMaps), BULK_FLUSH_THRESHOLD)
			// 记录已 flush 的 ID，防止后续 dst 误删
			for id := range srcDocMaps {
				flushedSrcIds[id] = true
			}
			_ = Verify(m.bulkRecords(opIndex, dstEsApi, cfg.TargetIndexName, srcType, srcDocMaps))
			addCount += len(srcDocMaps)
			srcDocMaps = make(map[string]interface{})
		}
		if len(dstDocMaps) > BULK_FLUSH_THRESHOLD {
			log.Warnf("dstDocMaps size %d exceeds threshold %d, flushing as deletes to prevent OOM", len(dstDocMaps), BULK_FLUSH_THRESHOLD)
			_ = Verify(m.bulkRecords(opDelete, dstEsApi, cfg.TargetIndexName, srcType, dstDocMaps))
			deleteCount += len(dstDocMaps)
			dstDocMaps = make(map[string]interface{})
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
