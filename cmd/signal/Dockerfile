# Builds the signal server from source with the HA dispatcher swapped in by go.mod.
# Build context: repository root.
FROM golang:1.26-bookworm AS build
ARG VERSION=dev
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -trimpath \
      -ldflags "-s -w -X github.com/netbirdio/netbird/version.version=${VERSION}" \
      -o /out/netbird-signal ./cmd/signal

FROM gcr.io/distroless/base:debug
COPY --from=build /out/netbird-signal /go/bin/netbird-signal
ENTRYPOINT ["/go/bin/netbird-signal", "run"]
CMD ["--log-file", "console"]
