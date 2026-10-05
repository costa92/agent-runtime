# agent-runtime

A Go runtime for durable agent workflows. The runtime owns graph execution, run state transitions, tool governance, approvals, quotas, memory ports, and replay. Hosts supply storage, model, tool, and observability implementations through ports.

The module uses the Go standard library only. Production packages have no database driver, HTTP transport, or model provider dependency. Hosts can run the conformance suites in `conformance/` against their adapters.

## Requirements

- Go 1.26.6

## Verify

```sh
go test ./...
go vet ./...
go build ./...
```

The architecture tests enforce the dependency boundary. Repository-specific database adapters and application code live in their host repositories.

## License

No license has been granted for this repository.
