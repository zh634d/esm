package main

import (
	"encoding/json"
	"fmt"
	log "github.com/cihub/seelog"
	"regexp"
	"strings"
	"time"
)

// TimestampFieldMapping 索引模式到时间戳字段的映射
type TimestampFieldMapping struct {
	Pattern   string // 索引模式，支持通配符 *
	Field     string // 时间戳字段名
	Regex     *regexp.Regexp
	IsLiteral bool // 是否是精确匹配（无通配符）
}

// ParseTimestampFields 解析时间戳字段配置
// 格式: "index_pattern=field,index_pattern2=field2"
// 支持通配符: "callrecord_*=system.beginTime"
func ParseTimestampFields(config string) ([]TimestampFieldMapping, error) {
	if config == "" {
		return nil, nil
	}

	var mappings []TimestampFieldMapping
	pairs := strings.Split(config, ",")

	for _, pair := range pairs {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}

		parts := strings.SplitN(pair, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid timestamp field mapping: %s, expected format: index_pattern=field", pair)
		}

		pattern := strings.TrimSpace(parts[0])
		field := strings.TrimSpace(parts[1])

		if pattern == "" || field == "" {
			return nil, fmt.Errorf("invalid timestamp field mapping: %s", pair)
		}

		mapping := TimestampFieldMapping{
			Pattern: pattern,
			Field:   field,
		}

		// 检查是否包含通配符
		if strings.Contains(pattern, "*") {
			// 将通配符转换为正则表达式
			regexPattern := "^" + regexp.QuoteMeta(pattern) + "$"
			regexPattern = strings.ReplaceAll(regexPattern, "\\*", ".*")
			regex, err := regexp.Compile(regexPattern)
			if err != nil {
				return nil, fmt.Errorf("invalid pattern %s: %v", pattern, err)
			}
			mapping.Regex = regex
			mapping.IsLiteral = false
		} else {
			mapping.IsLiteral = true
		}

		mappings = append(mappings, mapping)
		log.Debugf("parsed timestamp field mapping: pattern=%s, field=%s, literal=%v", pattern, field, mapping.IsLiteral)
	}

	return mappings, nil
}

// FindTimestampField 查找索引名对应的时间戳字段
func FindTimestampField(indexName string, mappings []TimestampFieldMapping) string {
	if mappings == nil {
		return ""
	}

	for _, mapping := range mappings {
		if mapping.IsLiteral {
			if indexName == mapping.Pattern {
				return mapping.Field
			}
		} else {
			if mapping.Regex.MatchString(indexName) {
				return mapping.Field
			}
		}
	}

	return ""
}

// ParseTimeRange 解析时间范围配置
// 支持格式:
// - "10m" - 最近 10 分钟
// - "1h" - 最近 1 小时
// - "1d" - 最近 1 天
// - "2026-08-20T10:00:00/2026-08-20T11:00:00" - 绝对时间范围
func ParseTimeRange(timeRange string) (gte string, lt string, err error) {
	if timeRange == "" {
		return "", "", nil
	}

	// 检查是否是绝对时间范围（包含 /）
	if strings.Contains(timeRange, "/") {
		parts := strings.SplitN(timeRange, "/", 2)
		if len(parts) != 2 {
			return "", "", fmt.Errorf("invalid time range format: %s", timeRange)
		}
		return parts[0], parts[1], nil
	}

	// 解析相对时间
	duration, err := parseDuration(timeRange)
	if err != nil {
		return "", "", fmt.Errorf("invalid time range: %s, %v", timeRange, err)
	}

	now := time.Now().UTC()
	start := now.Add(-duration)

	// 格式化为 ES 日期格式
	gte = start.Format("2006-01-02T15:04:05.000Z")
	lt = now.Format("2006-01-02T15:04:05.000Z")

	return gte, lt, nil
}

// parseDuration 解析自定义时间格式
// 支持: 10s, 10m, 10h, 10d
func parseDuration(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty duration")
	}

	// 先尝试标准 Go duration
	if d, err := time.ParseDuration(s); err == nil {
		return d, nil
	}

	// 自定义格式：支持 d (天)
	if strings.HasSuffix(s, "d") {
		numStr := strings.TrimSuffix(s, "d")
		var days int
		_, err := fmt.Sscanf(numStr, "%d", &days)
		if err != nil {
			return 0, fmt.Errorf("invalid duration: %s", s)
		}
		return time.Duration(days) * 24 * time.Hour, nil
	}

	return 0, fmt.Errorf("unsupported duration format: %s", s)
}

// BuildTimestampQuery 构建时间戳范围查询
func BuildTimestampQuery(timestampField string, gte string, lt string) (string, error) {
	if timestampField == "" || gte == "" {
		return "", nil
	}

	// 构建 range query
	rangeQuery := map[string]interface{}{
		"range": map[string]interface{}{
			timestampField: map[string]interface{}{
				"gte": gte,
				"lt":  lt,
			},
		},
	}

	queryBytes, err := json.Marshal(rangeQuery)
	if err != nil {
		return "", fmt.Errorf("failed to marshal timestamp query: %v", err)
	}

	return string(queryBytes), nil
}

// MergeQueries 合并用户查询和时间戳查询
// 返回完整的 query body（包含 "query" 字段），而不是 query_string
func MergeQueries(userQuery string, timestampQuery string) string {
	if userQuery == "" && timestampQuery == "" {
		return ""
	}

	// 构建完整的 query body
	var queryMap map[string]interface{}

	if userQuery == "" {
		// 只有时间戳查询
		queryMap = map[string]interface{}{
			"query": json.RawMessage(timestampQuery),
		}
	} else if timestampQuery == "" {
		// 只有用户查询
		queryMap = map[string]interface{}{
			"query": json.RawMessage(userQuery),
		}
	} else {
		// 两个查询都用，使用 bool must 合并
		queryMap = map[string]interface{}{
			"query": map[string]interface{}{
				"bool": map[string]interface{}{
					"must": []interface{}{
						json.RawMessage(userQuery),
						json.RawMessage(timestampQuery),
					},
				},
			},
		}
	}

	mergedBytes, err := json.Marshal(queryMap)
	if err != nil {
		log.Errorf("failed to merge queries: %v", err)
		return ""
	}

	return string(mergedBytes)
}

// GetEffectiveQuery 获取有效的查询（合并时间戳查询）
// 返回完整的 ES query body（JSON 格式），包括 "query" 字段
func GetEffectiveQuery(cfg *Config, indexName string) (string, error) {
	// 解析时间戳字段配置
	if cfg.TimestampFields == "" {
		// 没有时间戳配置，返回用户查询（如果有）
		if cfg.Query != "" {
			return cfg.Query, nil
		}
		return "", nil
	}

	mappings, err := ParseTimestampFields(cfg.TimestampFields)
	if err != nil {
		return "", err
	}

	// 查找当前索引的时间戳字段
	timestampField := FindTimestampField(indexName, mappings)
	if timestampField == "" {
		log.Debugf("no timestamp field mapping for index: %s, using original query", indexName)
		if cfg.Query != "" {
			return cfg.Query, nil
		}
		return "", nil
	}

	log.Infof("found timestamp field for index %s: %s", indexName, timestampField)

	// 解析时间范围
	gte, lt, err := ParseTimeRange(cfg.TimeRange)
	if err != nil {
		return "", fmt.Errorf("failed to parse time range: %v", err)
	}

	if gte == "" {
		if cfg.Query != "" {
			return cfg.Query, nil
		}
		return "", nil
	}

	log.Infof("syncing index %s with time range: %s to %s", indexName, gte, lt)

	// 构建时间戳范围查询（只返回 range 部分，不包含外层 "query"）
	timestampQuery, err := BuildTimestampQuery(timestampField, gte, lt)
	if err != nil {
		return "", err
	}

	// 合并查询
	mergedQuery := MergeQueries(cfg.Query, timestampQuery)
	log.Debugf("merged query for index %s: %s", indexName, mergedQuery)

	return mergedQuery, nil
}
