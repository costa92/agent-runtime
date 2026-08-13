package authorization

import "context"

// ResourceRef names the thing a decision is about. Kind is the resource family
// ("definition", "tool", "policy", "quota", ...); the Runtime does not
// enumerate them here, because the set grows with the resource model and a
// closed enum in this package would have to be edited by every host that adds
// one.
type ResourceRef struct {
	Kind string
	ID   string
}

// Authorizer is the host's answer to "may this principal do this". The Runtime
// asks; it never decides.
//
// Use and publish are separate methods rather than one method with an action
// argument, because they are separate powers and conflating them is the
// specific mistake this split exists to prevent: publishing rewrites prompts,
// widens tool allowlists, raises budgets and relaxes policy, so the right to
// run a definition must never imply the right to change it.
//
// Both take a PrincipalContext rather than a PrincipalRef: a decision is made
// against claims that were true at authentication time, and a Run resumed days
// later must be re-authorized against fresh ones rather than against whatever
// was recorded when it started.
type Authorizer interface {
	// AuthorizeUse reports whether the principal may use the resource — run
	// this definition, call this tool, read this memory scope.
	AuthorizeUse(ctx context.Context, principal PrincipalContext, resource ResourceRef) error

	// AuthorizePublish reports whether the principal may publish a new version
	// of the resource. Authorization is per Kind: publishing a Policy or Quota
	// is a strictly higher power than publishing a Definition.
	AuthorizePublish(ctx context.Context, principal PrincipalContext, resource ResourceRef) error
}
