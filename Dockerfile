FROM alpine:3.20

RUN apk add --no-cache ca-certificates tzdata \
    && cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime \
    && echo "Asia/Shanghai" > /etc/timezone

RUN addgroup -S esm && adduser -S esm -G esm
USER esm

COPY esm-linux-amd64 /usr/local/bin/esm

EXPOSE 6060

ENTRYPOINT ["esm"]
