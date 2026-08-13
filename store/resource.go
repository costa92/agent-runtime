package store

import (
	"encoding/json"

	"github.com/kart-io/wechat-account/agent-runtime/authorization"
	"github.com/kart-io/wechat-account/agent-runtime/resource"
	"github.com/kart-io/wechat-account/agent-runtime/run"
)

// AdmissionResult records what one node of the admission chain concluded.
//
// Every node's verdict is stored with the published version, not just the final
// yes. When a published resource later turns out to be wrong, "which checks
// passed and what did they see" is the question, and a single boolean cannot
// answer it.
type AdmissionResult struct {
	Node    string
	Allowed bool
	Detail  string
}

// PublishResourceCommand is an atomic compare-and-swap on one Kind's head.
//
// ExpectedHeadVersion is the CAS. Two operators publishing concurrently must
// produce one success and one conflict; last-write-wins would silently discard
// a policy change somebody believed had taken effect.
type PublishResourceCommand struct {
	ExpectedHeadVersion uint64
	Kind                resource.Kind
	Name                string
	APIVersion          string
	// Payload is the caller's bytes, in APIVersion. Conversion to the storage
	// version happens once, here on the publish path.
	Payload json.RawMessage

	PublishedBy      authorization.PrincipalRef
	AdmissionResults []AdmissionResult

	// Pending marks an approval-gated publish: the version is stored, stays out
	// of the head, and is invisible to every reader until approved.
	Pending bool

	// Graph carries the compiled execution graph for KindDefinition. It is not
	// typed as one yet — the sole compiler and the ExecutionGraph type arrive
	// in Task 7, and declaring a placeholder shape here would create a second
	// definition of it to reconcile later.
	GraphDigest string
}

func (c PublishResourceCommand) Validate() error {
	if !c.Kind.Valid() {
		return run.NewError("unknown_kind", run.ErrorInvalid, run.RetryNever)
	}
	if c.Name == "" {
		return run.NewError("missing_name", run.ErrorInvalid, run.RetryNever)
	}
	if err := resource.ValidateAPIVersion(c.Kind, c.APIVersion); err != nil {
		return err
	}
	if len(c.Payload) == 0 {
		return run.NewError("empty_payload", run.ErrorInvalid, run.RetryNever)
	}
	if c.PublishedBy.Zero() {
		// The publish audit fact has to name a publisher. Publishing is the
		// action that can rewrite prompts, widen allowlists and relax policy;
		// an unattributable one is not auditable at all.
		return run.NewError("missing_publisher", run.ErrorInvalid, run.RetryNever)
	}
	return nil
}
