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
	"github.com/kart-io/wechat-account/agent-runtime/llm"
	"github.com/kart-io/wechat-account/agent-runtime/memory"
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
	// Upstreams is where each dependency's output was stored, by the plan key
	// the orchestrator gave it. Empty unless this Run is a delegated child with
	// dependencies. The implementation reads them through its ports; the
	// Runtime hands over refs, never the outputs themselves.
	Upstreams map[string]string
	// Remaining is what the budget has left. Passed so an implementation can
	// choose a cheaper path rather than discovering the ceiling by hitting it.
	Remaining run.Limits
	// Ports is the only way an implementation reaches a model, a tool or
	// memory. It is handed in per invocation rather than injected into the
	// implementation at registration, because every call through it is fenced,
	// budgeted and recorded against *this* Run — a port captured at
	// construction would outlive the Run it was governed for.
	Ports Ports
}

// Ports is the governed effect surface.
//
// Nothing here is a client the implementation could have built itself. Each
// method runs the Runtime's full path — reserve budget, commit the
// invocation-begin fact, perform the effect, settle — so an agent cannot make
// an ungoverned call by choosing a different code path, only by not calling at
// all.
type Ports interface {
	// Model runs one model call. Attempt limits and retries belong to the
	// Runtime, so an implementation asking twice is spending twice and can be
	// seen doing it.
	Model(ctx context.Context, request llm.Request) (llm.Response, error)
	// Tool invokes a declared tool by name.
	Tool(ctx context.Context, name string, arguments json.RawMessage) (json.RawMessage, error)
	// Recall reads memory under the Definition's declared scope and ceilings.
	Recall(ctx context.Context, key, text string) ([]memory.Record, error)
	// Remember writes memory. The idempotency key is required: a memory write
	// is a side effect the Runtime cannot roll back.
	Remember(ctx context.Context, key, ref, text, idempotencyKey string) error
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
