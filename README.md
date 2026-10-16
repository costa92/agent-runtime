# agent-runtime

A Go runtime for Cordis plugin composition and durable agent Sessions. A Session records Turns and Steps as append-only events. The agent loop records model requests and capability decisions before performing external effects. Protocol v3 requests reference a committed history prefix instead of duplicating it. Hosts provide storage, model, capability, and context sources through ports.

The module uses the Go standard library only. It has no database driver, HTTP transport, or model provider dependency.

| Package | Responsibility |
| --- | --- |
| `cordis` | Plugin lifecycle and scoped services |
| `profile` | Published plugin manifests and pinned digests |
| `session` | Event protocol, incremental replay/checkpoints, list summaries, budget facts, and Store port |
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

`session.NewReducer().Apply(events)` validates a contiguous event suffix. `Replay`
uses this same reducer from the beginning. A reducer is transaction-local; its
returned projection is borrowed and must not be mutated. Discard it after an
Apply error. `Checkpoint` and `RestoreCheckpoint` encode a disposable, versioned
binary cache with a corruption checksum; adapters bind its Session ID and
sequence to their committed log. Missing, corrupt, or unsupported checkpoints
are rebuilt from events. Checkpoint bytes are trusted storage data, never an
API input or a second fact source. Request histories are represented by prefix
lengths inside checkpoints, avoiding repeated history copies.

Checkpoints reduce event parsing; they still contain the full projection and
history and do not make long-session recovery constant-time or trim model input.

Database adapters and application products live in their host repositories.

## License

Licensed under the [MIT License](LICENSE).
