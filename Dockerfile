ARG TOOLS_IMAGE=docker.horse/ci/on-alpine/tools:1

FROM $TOOLS_IMAGE AS tools

FROM docker.io/library/golang:1.27.3-alpine AS build

WORKDIR /usr/src/app/ckic

ARG MIRROR_ALPINE_URL=""

ARG MIRROR_ALPINE_FALLBACK_URL=""

ARG MIRROR_GO_URL=""

ARG MIRROR_GO_FALLBACK_URL=""

COPY --from=tools /usr/local/bin/pkg-base-setup /usr/local/bin/

RUN pkg-base-setup

COPY go.mod go.sum ./

RUN go mod download

COPY . .

RUN CGO_ENABLED=0 go build -ldflags="-s -w" -trimpath -o ckic-manager ./cmd/manager

FROM docker.io/library/alpine:3

ARG MIRROR_ALPINE_URL=""

ARG MIRROR_ALPINE_FALLBACK_URL=""

COPY --from=tools /usr/local/bin/pkg-base-setup /usr/local/bin/

RUN pkg-base-setup

COPY --from=build /usr/src/app/ckic/ckic-manager /usr/bin/ckic-manager

ENTRYPOINT ["/usr/bin/ckic-manager"]
