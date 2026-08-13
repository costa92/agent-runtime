package resource

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/kart-io/wechat-account/agent-runtime/run"
)

// AdmissionRequest is what the chain judges.
type AdmissionRequest struct {
	Kind Kind
	Name string
	// APIVersion is the version the caller published in; Payload is already
	// converted to the storage version by the time the chain runs, so a node
	// never has to handle two shapes.
	APIVersion string
	Payload    json.RawMessage
}

// AdmissionNode is one check in the fixed chain.
//
// Nodes are ordered and every one runs on every publish. That is what makes the
// chain reviewable: there is no per-Kind shortcut where one check was skipped
// because it "did not apply", which is how a resource family grows a hole.
type AdmissionNode interface {
	Name() string
	Admit(ctx context.Context, request AdmissionRequest) error
}

// AdmissionChain is the ordered, fixed sequence.
type AdmissionChain struct {
	nodes []AdmissionNode
}

// NewAdmissionChain builds a chain. Order is the caller's and is fixed at
// assembly: parse, convert, schema, Kind semantics, registry references, then
// built-in policy checks.
func NewAdmissionChain(nodes ...AdmissionNode) *AdmissionChain {
	return &AdmissionChain{nodes: nodes}
}

// Verdict is one node's conclusion, returned for every node rather than only
// for the one that refused. When a published resource later turns out to be
// wrong, "which checks passed and what did they see" is the question.
type Verdict struct {
	Node    string
	Allowed bool
	Detail  string
}

// Admit runs the whole chain and returns every verdict.
//
// It stops at the first refusal — running later nodes against a payload an
// earlier node already rejected would produce verdicts about something that
// will never exist — but the verdicts up to that point are still returned, so
// the audit fact records how far it got.
func (c *AdmissionChain) Admit(ctx context.Context, request AdmissionRequest) ([]Verdict, error) {
	verdicts := make([]Verdict, 0, len(c.nodes))
	for _, node := range c.nodes {
		err := node.Admit(ctx, request)
		verdicts = append(verdicts, Verdict{
			Node:    node.Name(),
			Allowed: err == nil,
			Detail:  detailOf(err),
		})
		if err != nil {
			return verdicts, err
		}
	}
	return verdicts, nil
}

// Nodes returns the chain's node names, for the startup log that makes the
// fixed chain visible.
func (c *AdmissionChain) Nodes() []string {
	names := make([]string, len(c.nodes))
	for i, node := range c.nodes {
		names[i] = node.Name()
	}
	return names
}

func detailOf(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// AdmissionFunc adapts a function to AdmissionNode.
type AdmissionFunc struct {
	NodeName string
	Check    func(ctx context.Context, request AdmissionRequest) error
}

func (f AdmissionFunc) Name() string { return f.NodeName }

func (f AdmissionFunc) Admit(ctx context.Context, request AdmissionRequest) error {
	return f.Check(ctx, request)
}

// ParseNode is the first node of every chain: the payload must be a JSON
// object. It is a node rather than an inline check so that the audit fact
// records that parsing happened, and so the chain has no untracked prologue.
var ParseNode = AdmissionFunc{
	NodeName: "parse",
	Check: func(_ context.Context, request AdmissionRequest) error {
		var object map[string]any
		if err := json.Unmarshal(request.Payload, &object); err != nil {
			return run.NewError("unparseable_payload", run.ErrorInvalid, run.RetryNever,
				fmt.Errorf("%s/%s: %w", request.Kind, request.Name, err))
		}
		return nil
	},
}
