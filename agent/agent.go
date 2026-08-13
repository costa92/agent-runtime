// Package agent is the extension slot for the code that does an agent's work.
//
// A Definition names an implementation by stable key; this is where that key
// resolves. Implementations are registered in Go and frozen before the Runtime
// is built — the capability *type* is code, while which instances exist and how
// they behave is published data. That split is what keeps adding an agent a
// release and configuring one a publish.
package agent

import (
	"context"
	"encoding/json"

	"github.com/kart-io/wechat-account/agent-runtime/authorization"
	"github.com/kart-io/wechat-account/agent-runtime/run"
)

// Request is what an agent is asked to do. It carries references and bounded
// values, never host types: an agent that received the host's ORM entity would
// be an agent only this host could run.
type Request struct {
	RunID     ID
	Principal authorization.PrincipalRef
	// Prompt and Input come from the published Definition and the Run's input,
	// already validated against the declared schema.
	Prompt string
	Input  json.RawMessage
	// Remaining is what the budget has left. Passed so an implementation can
	// choose a cheaper path rather than discovering the ceiling by hitting it.
	Remaining run.Limits
}

// ID re-exports the Runtime's identifier type so implementations do not import
// the run package for one alias.
type ID = run.ID

// Response is an agent's result.
type Response struct {
	Output json.RawMessage
	// Used is what the agent actually consumed, for settlement against the
	// reservation the Runtime made before calling.
	Used run.Limits
}

// Agent is one unit of work. It is deliberately a single method: everything
// else an implementation might want — persistence, model access, tools — comes
// from the ports the Runtime governs, not from an interface the agent
// implements.
type Agent interface {
	Execute(ctx context.Context, request Request) (Response, error)
}

// Factory builds an Agent for a published configuration. It receives the
// declaration's raw config so an implementation can carry its own settings
// without the Runtime knowing their shape.
//
// A factory rather than an instance because configuration is per published
// version: one registered type serves every version of every definition that
// names it.
type Factory interface {
	New(config json.RawMessage) (Agent, error)
}

// FactoryFunc adapts a function to Factory.
type FactoryFunc func(config json.RawMessage) (Agent, error)

func (f FactoryFunc) New(config json.RawMessage) (Agent, error) { return f(config) }
