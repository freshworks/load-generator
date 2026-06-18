# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

`load-generator` (binary name `lg`) is a generic, protocol-agnostic load testing tool written in Go. It drives load against HTTP, gRPC, Redis, MySQL, PostgreSQL, Cassandra (CQL), MongoDB, ClickHouse, SMTP, and Kafka endpoints. Each protocol is exposed both as a standalone subcommand and as a Lua module, so a single Lua script can mix protocols and have metrics collected per-call and in custom aggregations. Latency is tracked with HdrHistogram; SQL queries are fingerprinted so metrics aggregate by query signature rather than literal text.

## Common commands

```bash
make              # build the `lg` binary (CGO_ENABLED=0 go build -o lg)
make test         # go test -count=1 -race ./...   (this is what CI runs)
make lint         # golint ./...
make clean        # remove binary and ./dist

# run a single test
go test -race -run TestName ./internal/http/

# end-to-end smoke tests that spin up dependencies via docker compose
make test-redis   # also: test-kafka, test-kafka-scram, test-http, test-mongo, test-all
```

Several `internal/*` tests (`cql`, `psql`, `lua`, `mongo`, `kafka`) use `ory/dockertest` and will start real containers — they need a working Docker daemon. `docker-compose.yml` provides the same backing services for manual testing (`docker compose up -d <service>`).

CI (`.github/workflows/testing.yml`) runs `make test` on PRs targeting `main`. Releases are built with GoReleaser (`make snapshot` / `make release`).

## Architecture

The codebase has three layers: **`cmd/`** (CLI wiring), **`internal/runner` + `internal/loadgen`** (the concurrency/throughput engine), and **`internal/<protocol>`** (per-protocol generators). `internal/stats` is the shared metrics spine that everything writes into.

### The Generator contract

Everything that produces load implements `loadgen.Generator` (`internal/loadgen/loadgenerator.go`):

```go
type Generator interface {
    Init() error       // set up connections/clients (per worker goroutine)
    InitDone() error   // called after ALL workers finish Init
    Tick() error       // perform ONE unit of work; called once per work token
    Finish() error     // teardown
}
```

A `NewGenerator` factory closure adapts each protocol's constructor to this interface. To add a new protocol you: (1) implement a generator in `internal/<proto>/`, (2) add a cobra subcommand in `cmd/<proto>.go` that builds options and a `NewGenerator` closure, then calls `runner.New(...).Run()`, and (3) if it should be scriptable, register it as a Lua module in `internal/lua/lua_api.go` (`preloadModules`) and add a `TraceType` in `internal/stats/stats.go`.

### Runner: concurrency + throughput control

`internal/runner/runner.go` is the engine. `Run()`:
- Spawns `concurrency` worker goroutines, each owning its own `LoadGenerator`/`Generator` instance (so connections are per-worker, not shared).
- Uses a two-phase startup barrier: all workers run `Init()`, wait on a `WaitGroup`, then begin `Run()` together.
- A `ticker()` goroutine uses `golang.org/x/time/rate` to push work tokens onto `workChan` at `requestrate`/sec. Workers block on `workChan` and call `Tick()` per token. **`requestrate=0` means uncapped** — the ticker closes `workChan` and workers loop as fast as possible.
- If the channel backs up (target too slow to keep up), it logs "Target host is likely slow: missed request rate".
- `warmupTimer()` calls `stats.ResetMetrics()` after the warmup window so warmup traffic is excluded from the final report; `durationTimer()` cancels the context after `warmup + duration`.

`concurrency` defaults to `requestrate` (then to 1) — see `PersistentPreRunE` in `cmd/root.go`.

### Stats: the metrics spine (`internal/stats/stats.go`)

A single `Stats` object is shared across all workers, built on a **single-writer actor model**: exactly one goroutine (`statsCollector`) owns all mutable state (the metrics map, every HdrHistogram, `digestToQuery`). There are no mutexes on those structures. Two channels drive it:

- `statsChan` (buffered, size `requestrate*100`, min 10000) — the hot path. `RecordMetric()` just pushes a `*TraceInfo` and returns; all workers feed it.
- `statsCmd` (unbuffered) — control plane. `Report`/`Export`/`Reset`/`ResetMetrics`/`Stop` each send a command with a `done` reply channel and **block** until the collector responds (synchronous rendezvous).

Implications: the collector is a hard serialization point that does real per-metric work (SQL fingerprinting, map lookups, histogram recording). When `statsChan` fills, `RecordMetric` blocks and **stats backpressure throttles load generation itself**.

Data model: `MetricsMap = map[TraceType][Key][Subkey]*Metrics` — protocol → target(host) → operation(URL/query/command) → histogram bundle, lazily vivified by `getMetrics`. `TraceType` is the protocol (`http`, `grpc`, `sql`, `psql`, `cql`, `clickhouse`, `redis`, `mongo`, `smtp`, `custom`, `raw`). `custom`/`raw` back Lua's custom-metric aggregation.

Key behaviors and footguns:
- **Latency is recorded in microseconds**, displayed in ms via `scale = 1000`. `RawTrace` is the exception: recorded as-is, displayed with scale 1.
- **5-minute latency ceiling**: histograms are bounded `[1, 5min in µs]`. Slower responses are dropped with a warn — do not trust this tool for endpoints that can exceed 5 min.
- **RPS is sampled, not counted**: a 1s ticker records instantaneous `Δcount/Δtime` into a *separate* `rps` histogram; `AvgRPS = rps.Mean()`. Zero-throughput seconds are **not** recorded, so `AvgRPS` is the mean over *active* seconds and overstates sustained throughput for bursty workloads. In **server mode the ticker is disabled** — the server's RPS comes only from imported client reports.
- **SQL fingerprinting** (`sql`/`psql`/`cql`/`clickhouse`): the raw query Subkey is replaced by a percona `go-mysql` fingerprint Id, with `digestToQuery` mapping id→normalized query. All literal variants of a query aggregate under one signature.
- **Warmup exclusion**: the runner calls `ResetMetrics()` after warmup, which throws away the whole metrics map and resets start/end times.
- `Errors2` is overloaded — it is the gRPC deadline-exceeded counter, surfaced as the `deadline` column.

### Output rendering

`Report()` takes a format chosen by the `--output table|markdown` persistent flag (`md` is an alias for `markdown`; the flag is validated in `cmd/root.go` `PersistentPreRunE` and applied via `Stats.SetOutputFormat`). The format is stored on `Stats` and read inside `print()` — `Report()` itself stays parameterless. `print()` emits two independent renderers per metric leaf:
1. **Summary table** via `tablewriter` **v1.0.9**. Default (`table`) uses the library's Unicode box-drawing borders (`┌─┬┐│`); `markdown` mode swaps in `renderer.NewMarkdown()` (pipe `|` columns + `---` header separator, valid GFM). Columns are assembled dynamically: a fixed base (`<label>, Avg, StdDev, Min, Max, p50, p95, p99, p99.99, Total`), then `AvgRPS, Errors` for everything except `raw`, then protocol-specific trailing columns (HTTP: `2xx/3xx/4xx/5xx`; gRPC: `deadline`). The first column label is protocol-specific (`Url`/`Method`/`Query`/`Command`/`Operation`/`Key`). In markdown mode the protocol-name/target labels render as `###`/`####` headings and the digest map becomes a `| Digest | Query |` table. **Note:** v1.x `Header` takes variadic `any` (not `[]string`) and is NOT API-compatible with the widely-documented v0.x `NewWriter`/`SetHeader`. Rows sort by ascending request count; targets sort alphabetically; protocol **sections print in a fixed canonical order** (`sectionOrder` in `stats.go`) — this is intentional so regenerated reports diff cleanly (the map was previously ranged directly, giving non-deterministic order).
2. **Response-time histogram** — hand-rolled ASCII (`getResponseHistogram`), not tablewriter. Bars scale to a max of 40 `■` chars; HdrHistogram's native bars are re-projected onto 10 linear buckets (`hdrHistBarsToBuckets`, the fiddliest function here — covered by `TestBarsToBuckets`). `raw` metrics print without the `Response time histogram (ms):` heading. In markdown mode each histogram is wrapped in a ```` ``` ```` fence so the ASCII bars stay valid Markdown.

### Distributed (server/client) mode — two transport paths with different fidelity

`lg server :PORT` (`cmd/server.go`, `internal/server/`) runs a node that generates no load and serves a web UI (table, JSON, latency graphs at `/graphs`). There are two distinct merge/transport paths, and they are **not equivalent**:

- **Live aggregation** — clients run with `--server host:PORT` and publish their `stats.Report` over Go `net/rpc` (`LG.ImportReport`) in `PersistentPostRunE`. `net/rpc` uses **gob**, which ignores the `json:"-"` tag on `Result.LatencySnapshot`, so the full HdrHistogram snapshot crosses the wire and `importReport` **merges histograms** for statistically correct combined percentiles. (Merge rules differ per field: latency merged, counters summed, RPS *added* assuming parallel clients, start/end time min/max, duration summed — the duration sum is dubious and flagged `// ??` in the source.)
- **JSON `--export` / `--import`** — `Result.LatencySnapshot` is tagged `json:"-"`, so JSON keeps only the precomputed percentiles/buckets, **not** the raw histogram. JSON reports are therefore **display-only and terminal**: the server's `--import <file>` path renders them directly and never re-merges. You cannot aggregate JSON exports; use the RPC path for live multi-client aggregation.

### Lua scripting (`internal/lua/`)

`lg script foo.lua` is the most powerful entry point. The Lua VM is `yuin/gopher-lua`; Go objects are bridged with `layeh.com/gopher-luar`. The compiled bytecode is compiled once and shared across all worker LStates.

- A script defines global `init()`, `tick()`, and `finished()` functions, mapped to the `Generator` lifecycle. See `scripts/*.lua` for examples (`scripts/multiple.lua` mixes protocols; `scripts/http.lua` is the minimal template).
- `internal/lua/lua_api.go` `preloadModules` exposes each protocol as a `require`-able module (`http`, `grpc`, `redis`, `mysql`, `psql`, `cql`, `smtp`, `mongo`, `clickhouse`), each with `.Options()` and `.New()`. Globals `LG` (the `*LG` handle), `Log`, and helpers are injected.
- Custom metrics: `LG:BeginCustomMetrics(keys...)` / `LG:EndCustomMetrics(keys...)` / `LG:RecordRawMetrics(...)` let a script group several calls and report them as one combined unit.

## Conventions

- Logging uses `logrus`; workers tag entries with their `Id` field. `--verbose` enables debug level; `--debug` is specifically for diagnosing hung Lua scripts.
- SQL-type generators (`mysql`, `psql`, `clickhouse`) fingerprint queries (via `percona/go-mysql`) so metrics aggregate by normalized query signature, not literal SQL.
- Context cancellation is the universal stop signal: `main.go` cancels on SIGINT, and that context flows through the runner into every generator's `Tick`. Generators should treat "context canceled" errors as a clean exit, not a failure (see `scripts/http.lua`).
