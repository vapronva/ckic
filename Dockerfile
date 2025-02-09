FROM ot.cmld.ru/docker.io/library/golang:1.23-alpine AS build

COPY . /usr/src/app/ckic

WORKDIR /usr/src/app/ckic/cmd/caddy-kubernetes-ingress-controller

RUN set -ex && \
    go build -o /usr/bin/ckic && \
    chmod +x /usr/bin/ckic

FROM ot.cmld.ru/docker.io/library/alpine:3

COPY --from=build /usr/bin/ckic /usr/bin/ckic

USER nobody

ENTRYPOINT ["/usr/bin/ckic"]

CMD ["--namespace", "ckic-system", "--container-annotation", "ckic.cmld.ru/ingress-controller.caddy"]
