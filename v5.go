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
	log "github.com/cihub/seelog"
	"strings"
)

type ESAPIV5 struct {
	ESAPIV0
}

func (s *ESAPIV5) NewScroll(indexNames string, scrollTime string, docBufferCount int, query string, sort string,
	slicedId int, maxSlicedCount int, fields string) (scroll ScrollAPI, err error) {
	url := fmt.Sprintf("%s/%s/_search?scroll=%s&size=%d", s.Host, indexNames, scrollTime, docBufferCount)

	var jsonBody []byte
	if len(query) > 0 || maxSlicedCount > 0 || len(fields) > 0 {
		queryBody := map[string]interface{}{}

		if len(fields) > 0 {
			if !strings.Contains(fields, ",") {
				queryBody["_source"] = fields
			} else {
				queryBody["_source"] = strings.Split(fields, ",")
			}
		}

		if len(query) > 0 {
			// 检测是否是结构化 JSON 查询（以 { 开头）
			trimmedQuery := strings.TrimSpace(query)
			if strings.HasPrefix(trimmedQuery, "{") {
				// 结构化查询，直接使用
				var parsedQuery map[string]interface{}
				if err := json.Unmarshal([]byte(query), &parsedQuery); err == nil {
					// 如果已经是完整的 query body（包含 "query" 字段），直接使用
					if _, hasQuery := parsedQuery["query"]; hasQuery {
						queryBody = parsedQuery
						// 保留 fields 设置
						if len(fields) > 0 {
							if !strings.Contains(fields, ",") {
								queryBody["_source"] = fields
							} else {
								queryBody["_source"] = strings.Split(fields, ",")
							}
						}
					} else {
						// 只是 query 部分，包装一下
						queryBody["query"] = parsedQuery
					}
				} else {
					// 解析失败，降级为 query_string
					log.Warnf("failed to parse structured query, falling back to query_string: %v", err)
					queryBody["query"] = map[string]interface{}{
						"query_string": map[string]interface{}{
							"query": query,
						},
					}
				}
			} else {
				// 传统 query_string
				queryBody["query"] = map[string]interface{}{
					"query_string": map[string]interface{}{
						"query": query,
					},
				}
			}
		}

		if len(sort) > 0 {
			sortFields := make([]string, 0)
			sortFields = append(sortFields, sort)
			queryBody["sort"] = sortFields
		}

		if maxSlicedCount > 1 {
			log.Tracef("sliced scroll, %d of %d", slicedId, maxSlicedCount)
			queryBody["slice"] = map[string]interface{}{}
			queryBody["slice"].(map[string]interface{})["id"] = slicedId
			queryBody["slice"].(map[string]interface{})["max"] = maxSlicedCount
		}

		jsonBody, err = json.Marshal(queryBody)
		if err != nil {
			log.Error(err)
		}
	}

	log.Debugf("scroll query body: %s", string(jsonBody))

	body, err := Request(s.Compress, "POST", url, s.Auth, bytes.NewBuffer(jsonBody), s.HttpProxy)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	scroll = &Scroll{}
	err = DecodeJson(body, scroll)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	return scroll, err
}

func (s *ESAPIV5) NextScroll(scrollTime string, scrollId string) (ScrollAPI, error) {
	// 使用 POST + body 传递 scroll_id，避免 URL 过长
	url := fmt.Sprintf("%s/_search/scroll", s.Host)
	body := fmt.Sprintf(`{"scroll":"%s","scroll_id":"%s"}`, scrollTime, scrollId)

	respBody, err := Request(s.Compress, "POST", url, s.Auth, bytes.NewBufferString(body), s.HttpProxy)

	// decode elasticsearch scroll response
	scroll := &Scroll{}
	err = DecodeJson(respBody, &scroll)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	return scroll, nil
}
