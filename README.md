# N-Ops Agent

Agent, org proxy and orchestrator for [N-Ops](https://github.com/Neko-Nik-Org/N-Ops-Main-API),
an open-source infrastructure control plane.

- **Agent** - daemon on each managed Linux server; runs typed plugin operations and sends heartbeats.
- **Org proxy** - one per organization; agents connect out to it over mTLS.
- **Orchestrator** - validates server joins and records server status.

Written in Go, communicating over gRPC.

> Status: early development.

## Development

Requirements: Go, [buf](https://buf.build), `protoc-gen-go`, `protoc-gen-go-grpc`.

```bash
buf lint        # check .proto files
buf generate    # generate Go code into gen/
go build ./...
```
