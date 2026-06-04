FROM docker.io/library/golang:1.26.4-alpine AS build

WORKDIR /usr/src/app/ckic

RUN sed --in-place 's!https://dl-cdn.alpinelinux.org/alpine!https://linux.sex/dl-cdn.alpinelinux.org!g' /etc/apk/repositories || true && \
    apk --verbose update && \
    apk --verbose upgrade --available && \
    apk add --no-cache git ca-certificates tzdata && \
    update-ca-certificates

COPY go.mod go.sum ./

RUN go mod download

COPY . .

RUN CGO_ENABLED=0 go build -ldflags="-s -w" -trimpath -a -o ckic-manager ./cmd/manager && \
    chmod +x ckic-manager

FROM docker.io/library/alpine:3

RUN sed --in-place 's!https://dl-cdn.alpinelinux.org/alpine!https://linux.sex/dl-cdn.alpinelinux.org!g' /etc/apk/repositories || true && \
    apk --verbose update && \
    apk --verbose upgrade --available && \
    apk add --no-cache ca-certificates tzdata && \
    update-ca-certificates && \
    apk cache clean

COPY --from=build /usr/src/app/ckic/ckic-manager /usr/bin/ckic-manager

ENTRYPOINT ["/usr/bin/ckic-manager"]
