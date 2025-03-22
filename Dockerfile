FROM ot.cmld.ru/docker.io/library/golang:1.24-alpine AS build

COPY . /usr/src/app/ckic

WORKDIR /usr/src/app/ckic/cmd/manager

RUN set -ex && \
    go build -o /usr/bin/ckic-manager && \
    chmod +x /usr/bin/ckic-manager

FROM ot.cmld.ru/docker.io/library/alpine:3

COPY --from=build /usr/bin/ckic /usr/bin/ckic

USER nobody

ENTRYPOINT ["/usr/bin/ckic-manager"]
