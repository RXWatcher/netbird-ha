# Builds the HA management server from source. Build context: repository root.
FROM golang:1.26-bookworm AS build
ARG VERSION=dev
WORKDIR /src
COPY . .
RUN CGO_ENABLED=1 go build -trimpath \
      -ldflags "-s -w -X github.com/netbirdio/netbird/version.version=${VERSION}" \
      -o /out/netbird-mgmt ./cmd/management

FROM ubuntu:24.04
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/netbird-mgmt /go/bin/netbird-mgmt
ENTRYPOINT ["/go/bin/netbird-mgmt", "management"]
CMD ["--log-file", "console"]
