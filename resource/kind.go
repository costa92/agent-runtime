// Package resource is the versioned declarative resource family.
//
// Definition, Policy, Quota, ModelProfile and ToolBinding are one family rather
// than five parallel mechanisms: they share a publisher, a CAS, a digest rule,
// an admission chain and an audit trail. Five publish paths would drift, and
// governance that drifts is governance nobody can reason about.
package resource

import "github.com/kart-io/wechat-account/agent-runtime/run"

// Kind names one member of the family. The set is closed here because each
// Kind needs a converter and a storage version registered alongside it; a Kind
// a host could invent would have neither.
type Kind string

const (
	KindDefinition   Kind = "Definition"
	KindPolicy       Kind = "Policy"
	KindQuota        Kind = "Quota"
	KindModelProfile Kind = "ModelProfile"
	// KindToolBinding is storable but inert: publishing one changes nothing.
	//
	// The Gateway seam that would have made a bound tool invocable was removed
	// once it turned out nothing had ever wired it — see Gateway.resolve. The
	// Kind stays because removing a member of a closed set is a data question,
	// not a code one: a deployment with stored ToolBinding rows would find them
	// unreadable. Retire it by migrating those rows away first, then dropping
	// the constant, its storage version and this comment together.
	KindToolBinding Kind = "ToolBinding"
)

// Kinds returns every declared Kind, in a stable order.
func Kinds() []Kind {
	return []Kind{KindDefinition, KindPolicy, KindQuota, KindModelProfile, KindToolBinding}
}

// Valid reports whether the Kind is one this Runtime knows.
func (k Kind) Valid() bool {
	for _, known := range Kinds() {
		if k == known {
			return true
		}
	}
	return false
}

// Ref identifies one published version of one resource.
//
// Version is the monotonic publish sequence and Digest is the canonical hash of
// the payload. Both are recorded because they answer different questions: the
// version orders publications, the digest proves two refs are the same bytes —
// which is what makes a cache safe and a recovery load faithful.
type Ref struct {
	Kind       Kind   `json:"kind"`
	Name       string `json:"name"`
	APIVersion string `json:"api_version"`
	Version    uint64 `json:"version"`
	Digest     string `json:"digest"`
}

// Validate checks the reference is well formed. Version zero is rejected:
// "unversioned" is not a state a published resource can be in, and accepting
// it would let a Run pin something that can still change underneath it.
func (r Ref) Validate() error {
	if !r.Kind.Valid() {
		return run.NewError("unknown_kind", run.ErrorInvalid, run.RetryNever)
	}
	if r.Name == "" {
		return run.NewError("missing_name", run.ErrorInvalid, run.RetryNever)
	}
	if err := ValidateAPIVersion(r.Kind, r.APIVersion); err != nil {
		return err
	}
	if r.Version == 0 {
		return run.NewError("mutable_version", run.ErrorInvalid, run.RetryNever)
	}
	return nil
}
