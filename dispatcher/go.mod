// Drop-in replacement for github.com/netbirdio/signal-dispatcher/dispatcher at
// v0.0.0-20250805121659-6b4ac470ca45 (the version NetBird v0.82.1 and v0.83.0 pin). The root
// module swaps it in with:
//
//	replace github.com/netbirdio/signal-dispatcher/dispatcher => ./dispatcher
module github.com/netbirdio/signal-dispatcher/dispatcher

go 1.26.0

require (
	github.com/alicebob/miniredis/v2 v2.40.0
	github.com/netbirdio/netbird v0.83.0
	github.com/redis/go-redis/v9 v9.7.3
	github.com/sirupsen/logrus v1.9.4
	github.com/stretchr/testify v1.12.1
	go.opentelemetry.io/otel v1.43.0
	go.opentelemetry.io/otel/metric v1.43.0
	google.golang.org/grpc v1.80.0
	google.golang.org/protobuf v1.36.11
)

require (
	github.com/caarlos0/env/v11 v11.4.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/grafana/pyroscope-go v1.4.2 // indirect
	github.com/grafana/pyroscope-go/godeltaprof v0.1.11 // indirect
	github.com/klauspost/compress v1.18.7 // indirect
	github.com/yuin/gopher-lua v1.1.1 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel/trace v1.43.0 // indirect
	go.yaml.in/yaml/v3 v3.0.5 // indirect
	golang.org/x/net v0.58.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.41.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260401024825-9d38bb4040a9 // indirect
)

// Copied from NetBird v0.83.0 go.mod: a dependency's replace lines do not apply
// to the module that requires it.
replace github.com/cloudflare/circl => codeberg.org/cunicu/circl v0.0.0-20230801113412-fec58fc7b5f6

replace github.com/mailru/easyjson => github.com/netbirdio/easyjson v0.9.0

replace github.com/wailsapp/wails/v3 => github.com/netbirdio/wails/v3 v3.0.0-beta.3.0.20261028091752-cfa103160796
