# ==============================================================================
# Stage 1: Build
# ==============================================================================
FROM --platform=$BUILDPLATFORM golang:1.21-alpine AS builder

RUN apk add --no-cache git ca-certificates

WORKDIR /build

# 先复制依赖文件，利用 Docker 缓存
COPY go.mod go.sum ./
RUN go mod download

# 复制源码
COPY . .

# 多架构构建：通过 TARGETOS/TARGETARCH 自动交叉编译
ARG TARGETOS
ARG TARGETARCH

RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -ldflags="-s -w" -o /build/esm .

# ==============================================================================
# Stage 2: Runtime
# ==============================================================================
FROM alpine:3.20

RUN apk add --no-cache ca-certificates tzdata \
    && cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime \
    && echo "Asia/Shanghai" > /etc/timezone

# 非 root 用户运行
RUN addgroup -S esm && adduser -S esm -G esm
USER esm

COPY --from=builder /build/esm /usr/local/bin/esm

EXPOSE 6060

ENTRYPOINT ["esm"]
