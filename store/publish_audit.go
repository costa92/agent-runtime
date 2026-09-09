package store

import (
	"github.com/kart-io/wechat-account/agent-runtime/authorization"
	"github.com/kart-io/wechat-account/agent-runtime/resource"
)

// PublishAudit is the immutable record of one publish. Every publish writes
// one, in the same transaction as the version it describes: publishing is the
// action that can rewrite prompts, widen allowlists, raise budgets and relax
// policy, so an unattributable one is not auditable at all.
type PublishAudit struct {
	Ref         resource.Ref
	PreviousRef resource.Ref
	PublishedBy authorization.PrincipalRef
	Admission   []AdmissionResult
}
