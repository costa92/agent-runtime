// Package agentruntime executes versioned Agent definitions through
// host-supplied ports.
//
// It owns execution semantics and nothing else. Persistence, model providers,
// transport, and every product concept belong to the host, which supplies them
// as ports. The boundary is not a convention here: architecture_test.go fails
// the build if this module's production dependency graph reaches beyond the
// standard library and a short reviewed allowlist.
//
// Current capability is documented in docs/architecture/system-architecture.md.
package agentruntime
