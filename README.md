# agent-runtime

A Go runtime for Cordis plugin composition and durable agent Sessions. A Session records Turns and Steps as append-only events. The agent loop records model requests and capability decisions before performing external effects. Protocol v3 requests reference a committed history prefix instead of duplicating it. Hosts provide storage, model, capability, and context sources through ports.

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

Execution requires an explicit Product via `agentloop.New`; the Runtime has no implicit conversation workflow. Products plan from committed Session facts, and hosts own Session creation and inbox submission.

`Context.Close(ctx)` and `Fiber.Close(ctx)` wait within the supplied deadline. On timeout teardown continues, retaining resources until handlers return; a later Close waits for the same teardown. Model settlement has a five-second deadline independent of caller cancellation.

Token allowances check settled usage before admission; they do not reserve in-flight tokens or provide a hard spending ceiling. Known actual usage is retained even when it exceeds a limit.

Database adapters and application products live in their host repositories.

## License

Licensed under the [MIT License](LICENSE).
