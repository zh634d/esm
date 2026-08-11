# ESM - Elasticsearch Migration Tool

Elasticsearch 跨版本数据迁移工具，支持全量同步和增量同步（`--sync` 模式）。

**本版本在原版基础上增加了双版本同步策略，针对 ES 5.x 的 `_uid` fielddata 问题做了专项适配。**

## 功能特性

- 跨版本迁移（ES 5.x / 6.x / 7.x 互迁）
- 增量同步（`--sync` 模式）：自动比较源端和目标端差异，仅同步变更数据
- **双版本同步策略**：根据 ES 版本自动选择最优同步算法
- 索引名映射（`-y`）
- 复制索引 settings 和 mappings
- HTTP Basic Auth 认证
- 支持 Sliced Scroll（ES 5.0+）
- 支持 HTTP 代理
- 支持数据过滤（`-q`）、字段过滤（`--fields` / `--skip`）
- 支持字段重命名（`--rename`）
- 支持本地文件导入/导出

## 架构

### 整体架构

```
┌─────────────┐          ┌──────────────────────┐          ┌─────────────┐
│  Source ES  │──scroll──│        ESM           │──bulk───│  Target ES  │
│  (5.x/6.x/ │          │                      │          │  (5.x/6.x/ │
│   7.x)      │          │  ┌────────────────┐  │          │   7.x)      │
└─────────────┘          │  │ SyncBetweenIndex│  │          └─────────────┘
                         │  │   (dispatcher)  │  │
                         │  └───────┬────────┘  │
                         │          │            │
                         │  ┌───────┴────────┐  │
                         │  │ version detect  │  │
                         │  └───────┬────────┘  │
                         │     ┌────┴────┐      │
                         │     ▼         ▼      │
                         │  ES 5.x   ES 6/7.x  │
                         │  syncBy   syncBy     │
                         │  Map      Sorted     │
                         │           Pointer    │
                         └──────────────────────┘
```

### 双版本同步策略

`SyncBetweenIndex` 是增量同步的入口函数，根据源 ES 版本号自动分发到两种实现：

```go
func (m *Migrator) SyncBetweenIndex(...) {
    srcVersion := srcEsApi.ClusterVersion().Version.Number
    if strings.HasPrefix(srcVersion, "5.") {
        m.syncByMap(...)      // ES 5.x
    } else {
        m.syncBySortedPointer(...)  // ES 6.x / 7.x
    }
}
```

#### syncBySortedPointer（ES 6.x / 7.x）

**双指针算法**：src 和 dst 均按 `_id` 排序，用两个指针同步推进比较。

```
src: [A] [B] [C] [D] [E]    ← 按 _id 排序
dst: [A] [C] [D]             ← 按 _id 排序

A==A → 比较内容，相同则跳过
B<C  → B 在 dst 中不存在 → 新增
C==C → 比较内容，不同则更新
D==D → 比较内容
E>无 → E 在 dst 中不存在 → 新增
```

| 特点 | 说明 |
|------|------|
| 内存 | `O(batch)` — 只存当前批次 |
| 排序 | 需要 `_id` 排序（ES 6.x+ 用 doc_values，无 fielddata 问题） |
| 适用 | 大数据量场景（百万级以上） |

#### syncByMap（ES 5.x）

**纯 Map 比较**：不排序，scroll 全量 src 放入 map，边 scroll dst 边比较。

```
Step 1: scroll src → srcDocMaps{_id: source}
Step 2: scroll dst 分批:
  - _id 在 srcDocMaps → 比较内容，匹配后删除释放内存
  - _id 不在 srcDocMaps → 加入删除队列
Step 3: srcDocMaps 剩余 → 新增文档
```

| 特点 | 说明 |
|------|------|
| 内存 | `O(src)` — 全量 src 存内存，dst 边处理边释放 |
| 排序 | **不排序** — 避免 ES 5.x `_uid` fielddata 报错 |
| 适用 | ES 5.x 环境 |

> **ES 5.x `_uid` 问题**：ES 5.x 对 `_id` 排序底层依赖 `_uid` 字段的 fielddata，但 ES 5.x 默认禁止 `_uid` fielddata 访问。syncByMap 通过不传 sort 参数彻底绕开此问题。

### 数据流

```
                    scroll                    compare                   bulk
Source ES ──────────────────► srcDocMaps ──────────────► diffDocs ──────────► Target ES
                              (all src docs)              (add/update)

                              dst scroll ──► compare ───► batchDelete
                                          (per batch)     (dst-only docs)
```

## 快速开始

### 二进制运行

```bash
# 下载编译好的二进制
# Linux AMD64
wget https://github.com/zh634d/esm/releases/latest/download/esm-linux-amd64
chmod +x esm-linux-amd64

# 全量同步
./esm-linux-amd64 \
  -s http://source:9200 \
  -d http://target:9200 \
  -m elastic:password \
  -n elastic:password \
  -x source_index \
  -y target_index

# 增量同步（--sync）
./esm-linux-amd64 \
  -s http://source:9200 \
  -d http://target:9200 \
  -m elastic:password \
  -n elastic:password \
  -x source_index \
  -y target_index \
  --sync
```

### Docker 运行

```bash
# 1. 先编译 Linux 二进制
GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o esm-linux-amd64 .

# 2. 构建镜像
docker build -t zh634d/esm:latest .

# 或构建多架构镜像（需 docker buildx）
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -t zh634d/esm:latest \
  --push .

# 运行
docker run --rm zh634d/esm:latest \
  -s http://source:9200 \
  -d http://target:9200 \
  -x my_index --sync
```

### Docker Compose

```yaml
services:
  esm-sync:
    image: zh634d/esm:latest
    command: >
      -s http://source-es:9200
      -d http://target-es:9200
      -m elastic:password
      -n elastic:password
      -x callrecord_7,callrecord_8,callrecord_9
      --sync
      -c 5000
      -w 4
    restart: on-failure
```

## 参数说明

| 参数 | 缩写 | 默认值 | 说明 |
|------|------|--------|------|
| `--source` | `-s` | | 源 ES 地址，如 `http://localhost:9200` |
| `--dest` | `-d` | | 目标 ES 地址 |
| `--source_auth` | `-m` | | 源 ES 认证，格式 `user:pass` |
| `--dest_auth` | `-n` | | 目标 ES 认证 |
| `--src_indexes` | `-x` | `_all` | 源索引名，支持正则和逗号分隔 |
| `--dest_index` | `-y` | `""` | 目标索引名，不指定则使用源索引同名 |
| `--sync` | | `false` | **增量同步模式**，自动比较差异仅同步变更 |
| `--query` | `-q` | `""` | 查询过滤，如 `status:active` |
| `--count` | `-c` | `10000` | 每次 scroll/bulk 的文档数 |
| `--workers` | `-w` | `1` | 并发 bulk worker 数 |
| `--bulk_size` | `-b` | `5` | bulk 请求大小（MB） |
| `--time` | `-t` | `10m` | scroll 上下文保持时间 |
| `--sort` | | `_id` | scroll 排序字段（`--sync` 模式下 ES 5.x 自动忽略） |
| `--fields` | | `""` | 白名单字段，逗号分隔 |
| `--skip` | | `""` | 黑名单字段，逗号分隔 |
| `--rename` | | `""` | 字段重命名，如 `_type:type,name:myname` |
| `--force` | `-f` | `false` | 同步前删除目标索引 |
| `--copy_settings` | | `false` | 复制索引 settings |
| `--copy_mappings` | | `false` | 复制索引 mappings |
| `--compress` | | `false` | 启用 gzip 压缩传输 |
| `--log` | `-v` | `INFO` | 日志级别：trace/debug/info/warn/error |
| `--sleep` | `-p` | `-1` | 每次 bulk 后休眠秒数 |

## 增量同步示例

### 单索引同步

```bash
./esm -s http://source:9200 -d http://target:9200 \
  -x callrecord_9 -y callrecord_9 --sync
```

输出：
```
source ES version 5.5.2, using map-based sync (no sort required)
sync callrecord_9(150000) to callrecord_9(148500), add=1000, update=400, delete=100
```

### 多索引批量同步（Shell 脚本）

```bash
#!/bin/bash
INDEXES=("callrecord_7" "callrecord_8" "callrecord_9" "callrecord_10")

for idx in "${INDEXES[@]}"; do
    echo "同步 $idx ..."
    ./esm -s http://source:9200 -d http://target:9200 \
      -x "$idx" -y "$idx" --sync -c 5000 -w 4
done
```

### 配合 Crontab 定时增量

```bash
# 每 5 分钟执行一次增量同步
*/5 * * * * /usr/local/bin/esm-sync.sh >> /var/log/esm_sync.log 2>&1
```

## 从源码编译

```bash
# 本地编译
go build -o esm .

# 交叉编译 Linux AMD64
GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o esm-linux-amd64 .

# 交叉编译 Linux ARM64
GOOS=linux GOARCH=arm64 go build -ldflags="-s -w" -o esm-linux-arm64 .

# Docker 多架构构建
docker buildx create --use
docker buildx build --platform linux/amd64,linux/arm64 -t esm:latest --push .
```

## 支持的 ES 版本矩阵

| 源 \ 目标 | ES 5.x | ES 6.x | ES 7.x |
|-----------|--------|--------|--------|
| **ES 5.x** | ✅ Map 模式 | ✅ | ✅ |
| **ES 6.x** | ✅ | ✅ 双指针 | ✅ 双指针 |
| **ES 7.x** | ✅ | ✅ | ✅ 双指针 |

> 跨大版本迁移时，建议先同步一个索引验证数据完整性，再进行全量迁移。

## License

Apache License 2.0
