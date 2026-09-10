// Package resource is the versioned declarative resource family.
//
// Definition, Policy, Quota and ModelProfile are one family rather
// than four parallel mechanisms: they share a publisher, a CAS, a digest rule,
// an admission chain and an audit trail. Four publish paths would drift, and
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
)

// Kinds returns every declared Kind, in a stable order.
func Kinds() []Kind {
	return []Kind{KindDefinition, KindPolicy, KindQuota, KindModelProfile}
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

// ValidateForRead checks the three fields that actually locate a version.
//
// APIVersion is not among them: the stored row is found by (kind, name,
// version) alone, and its apiVersion is read out of that row rather than
// matched against. A reader that has a version number but not its apiVersion —
// which is every reader, since no endpoint reports the apiVersion of a
// non-head version — must still be able to ask for it.
//
// Version zero is rejected: "unversioned" is not a state a published resource
// can be in, and accepting it would let a Run pin something that can still
// change underneath it.
func (r Ref) ValidateForRead() error {
	if !r.Kind.Valid() {
		return run.NewError("unknown_kind", run.ErrorInvalid, run.RetryNever)
	}
	if r.Name == "" {
		return run.NewError("missing_name", run.ErrorInvalid, run.RetryNever)
	}
	if r.Version == 0 {
		return run.NewError("mutable_version", run.ErrorInvalid, run.RetryNever)
	}
	return nil
}

// Validate additionally requires the apiVersion, which is what makes a Ref a
// complete identity rather than a lookup key. Refs handed back by the store
// carry it; refs assembled from a URL do not, and those go through
// ValidateForRead.
func (r Ref) Validate() error {
	if err := r.ValidateForRead(); err != nil {
		return err
	}
	return ValidateAPIVersion(r.Kind, r.APIVersion)
}
