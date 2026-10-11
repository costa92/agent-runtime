# agent-runtime

A Go runtime for Cordis plugin composition and durable agent Sessions. A Session records Turns and Steps as append-only events. The agent loop records model requests and capability decisions before performing external effects. Hosts provide storage, model, capability, and context sources through ports.

The module uses the Go standard library only. It has no database driver, HTTP transport, or model provider dependency.

| Package | Responsibility |
| --- | --- |
| `cordis` | Plugin lifecycle and scoped services |
| `profile` | Published plugin manifests and pinned digests |
| `session` | Event protocol, replay, budget facts, and Store port |
| `agentloop` | Turn and Step execution with durable model and capability decisions |
| `product` | Deterministic workflow planning from Session facts |
| `capability` | Governed, typed external effects |
| `model` | Model request and response port |

## Requirements

- Go 1.26.6

## Verify

```sh
go test ./...
go vet ./...
go build ./...
```

Database adapters and application products live in their host repositories.

## License

Licensed under the [MIT License](LICENSE).
