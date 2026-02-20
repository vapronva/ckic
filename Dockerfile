FROM docker.io/library/golang:1.26-alpine AS build

WORKDIR /usr/src/app/ckic

RUN apk update && \
    apk upgrade && \
    apk add git ca-certificates tzdata && \
    rm -rf /var/cache/apk/*

COPY go.mod go.sum ./

RUN go mod download

COPY . .

RUN CGO_ENABLED=0 go build -ldflags="-s -w" -trimpath -a -o ckic-manager ./cmd/manager && \
    chmod +x ckic-manager

FROM docker.io/library/alpine:3

RUN apk update && \
    apk upgrade && \
    apk add ca-certificates tzdata && \
    rm -rf /var/cache/apk/*

COPY --from=build /usr/src/app/ckic/ckic-manager /usr/bin/ckic-manager

ENTRYPOINT ["/usr/bin/ckic-manager"]
