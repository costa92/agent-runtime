package testkit

import (
	"context"
	"sort"
	"sync"

	"github.com/kart-io/wechat-account/agent-runtime/authorization"
	"github.com/kart-io/wechat-account/agent-runtime/quota"
	"github.com/kart-io/wechat-account/agent-runtime/resource"
	"github.com/kart-io/wechat-account/agent-runtime/run"
	"github.com/kart-io/wechat-account/agent-runtime/store"
)

type versionKey struct {
	kind resource.Kind
	name string
}

// MemoryResources is an in-process resource store: one publish path for every
// Kind, CAS on the head, immutable versions and an audit fact per publish.
type MemoryResources struct {
	mu    sync.Mutex
	clock *Clock

	versions map[versionKey][]store.PublishedResource
	heads    map[versionKey]uint64
	audit    []store.PublishAudit

	chain      *resource.AdmissionChain
	converters resource.Converters
	// publishers is the set of subjects allowed to publish. Deliberately
	// separate from any notion of read access: the two powers are different.
	publishers map[string]bool
}

func NewMemoryResources(clock *Clock, publishers ...string) *MemoryResources {
	allowed := make(map[string]bool, len(publishers))
	for _, subject := range publishers {
		allowed[subject] = true
	}
	return &MemoryResources{
		clock:      clock,
		versions:   make(map[versionKey][]store.PublishedResource),
		heads:      make(map[versionKey]uint64),
		chain:      resource.NewAdmissionChain(resource.ParseNode),
		converters: resource.Converters{},
		publishers: allowed,
	}
}

var (
	_ store.ResourceReader     = (*MemoryResources)(nil)
	_ store.ResourcePublisher  = (*MemoryResources)(nil)
	_ store.ResourceAuthorizer = (*MemoryResources)(nil)
)

// Head returns the current head version, or zero when nothing is published.
func (r *MemoryResources) Head(kind resource.Kind, name string) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.heads[versionKey{kind, name}]
}

// Audit returns the publish audit facts.
func (r *MemoryResources) Audit() []store.PublishAudit {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]store.PublishAudit(nil), r.audit...)
}

func (r *MemoryResources) AuthorizeUse(context.Context, authorization.PrincipalRef, resource.Ref) error {
	return nil
}

func (r *MemoryResources) AuthorizePublish(_ context.Context, principal authorization.PrincipalRef, _ store.PublishIntent) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.publishers[principal.Subject] {
		return run.NewError("publish_denied", run.ErrorDenied, run.RetryNever)
	}
	return nil
}

func (r *MemoryResources) GetPublished(_ context.Context, ref resource.Ref) (store.PublishedResource, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, version := range r.versions[versionKey{ref.Kind, ref.Name}] {
		if version.Ref.Version != ref.Version {
			continue
		}
		if version.Pending {
			// Invisible, not "visible but flagged": a reader that could see a
			// pending version could run one, which is what approval-gating is
			// for.
			return store.PublishedResource{}, run.NewError("pending_version", run.ErrorInvalid, run.RetryNever)
		}
		return version, nil
	}
	return store.PublishedResource{}, run.NewError("unknown_resource", run.ErrorInvalid, run.RetryNever)
}

func (r *MemoryResources) ListActive(_ context.Context, query store.ResourceQuery) ([]store.PublishedResource, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	var active []store.PublishedResource
	for key, versions := range r.versions {
		if key.kind != query.Kind {
			continue
		}
		head := r.heads[key]
		for _, version := range versions {
			if version.Ref.Version == head && !version.Pending {
				active = append(active, version)
			}
		}
	}
	sort.Slice(active, func(i, j int) bool { return active[i].Ref.Name < active[j].Ref.Name })
	return active, nil
}

func (r *MemoryResources) Publish(ctx context.Context, command store.PublishResourceCommand) (store.PublishedResource, error) {
	if err := command.Validate(); err != nil {
		return store.PublishedResource{}, err
	}
	// Authorization first: a refused publish must leave no version, no head
	// move and no audit fact, so nothing may be written before this point.
	if err := r.AuthorizePublish(ctx, command.PublishedBy, store.PublishIntent{
		Kind: command.Kind, Name: command.Name,
	}); err != nil {
		return store.PublishedResource{}, err
	}

	converted, digest, verdicts, err := resource.Prepare(
		ctx, r.chain, r.converters, command.Kind, command.Name, command.APIVersion, command.Payload,
	)
	if err != nil {
		return store.PublishedResource{}, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	key := versionKey{command.Kind, command.Name}
	head := r.heads[key]
	if command.ExpectedHeadVersion != head {
		return store.PublishedResource{}, run.NewError("head_conflict", run.ErrorConflict, run.RetryNever)
	}

	storageVersion, err := resource.StorageVersion(command.Kind)
	if err != nil {
		return store.PublishedResource{}, err
	}
	published := store.PublishedResource{
		Ref: resource.Ref{
			Kind:       command.Kind,
			Name:       command.Name,
			APIVersion: storageVersion,
			Version:    head + 1,
			Digest:     digest,
		},
		Payload:     converted,
		Pending:     command.Pending,
		PublishedBy: command.PublishedBy,
		PublishedAt: r.clock.Now(),
	}

	var previous resource.Ref
	for _, version := range r.versions[key] {
		if version.Ref.Version == head {
			previous = version.Ref
		}
	}

	r.versions[key] = append(r.versions[key], published)
	// A pending version is stored but does not move the head, so no reader
	// resolves it and no Run can pin it.
	if !command.Pending {
		r.heads[key] = published.Ref.Version
	}

	results := make([]store.AdmissionResult, len(verdicts))
	for i, verdict := range verdicts {
		results[i] = store.AdmissionResult{Node: verdict.Node, Allowed: verdict.Allowed, Detail: verdict.Detail}
	}
	r.audit = append(r.audit, store.PublishAudit{
		Ref:         published.Ref,
		PreviousRef: previous,
		PublishedBy: command.PublishedBy,
		Admission:   results,
	})
	return published, nil
}

// MemoryQuota enforces tenant limits against the same ledger the Root Budget
// uses. It holds one usage counter, not two: the suite's sharpest test charges
// through the ledger and asserts the enforcer sees it.
type MemoryQuota struct {
	mu sync.Mutex

	quotas map[string]quota.Quota
	// used is the shared meter. Reserve and LedgerCharge both land here,
	// because a quota-private counter lets a tenant spend its ceiling twice.
	used map[string]run.Limits
	// concurrent counts in-flight Runs per tenant.
	concurrent map[string]int
}

func NewMemoryQuota() *MemoryQuota {
	return &MemoryQuota{
		quotas:     make(map[string]quota.Quota),
		used:       make(map[string]run.Limits),
		concurrent: make(map[string]int),
	}
}

var _ store.QuotaEnforcer = (*MemoryQuota)(nil)

// Publish installs a quota, as an operator would. It replaces the tenant's
// quota without touching the meter: a tightened limit applies to the next
// checkpoint, and retroactively revoking granted capacity would kill Runs that
// were within their limit when they started.
func (q *MemoryQuota) Publish(published quota.Quota) error {
	if err := published.Validate(); err != nil {
		return err
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	q.quotas[published.Tenant] = published
	return nil
}

// Metered returns the shared meter's value.
func (q *MemoryQuota) Metered(tenant string) run.Limits {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.used[tenant]
}

// LedgerCharge records usage the way the Root Budget Ledger does, bypassing
// every quota code path.
func (q *MemoryQuota) LedgerCharge(tenant string, amount run.Limits) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.used[tenant] = q.used[tenant].Add(amount)
}

func (q *MemoryQuota) Reserve(_ context.Context, tenant string, reservation store.BudgetReservation) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.used[tenant] = q.used[tenant].Add(reservation.Amount)
	q.concurrent[tenant]++
	return nil
}

func (q *MemoryQuota) Check(_ context.Context, checkpoint store.QuotaCheckpoint, tenant string, spend run.Limits) (store.QuotaDecision, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	limit, ok := q.quotas[tenant]
	if !ok {
		return store.QuotaDecision{Allowed: true}, nil
	}

	if checkpoint == store.CheckpointRunCreate && limit.MaxConcurrentRuns > 0 &&
		q.concurrent[tenant] >= limit.MaxConcurrentRuns {
		return store.QuotaDecision{Outcome: limit.OnExceeded, Limit: "max_concurrent_runs"}, nil
	}

	projected := q.used[tenant].Add(spend)
	for _, usage := range limit.Usage {
		if usage.Tokens > 0 && projected.Tokens > usage.Tokens {
			return store.QuotaDecision{
				Outcome: limit.OnExceeded,
				Limit:   "tokens/" + string(usage.Window),
			}, nil
		}
		if usage.LLMCalls > 0 && projected.LLMCalls > usage.LLMCalls {
			return store.QuotaDecision{Outcome: limit.OnExceeded, Limit: "llm_calls/" + string(usage.Window)}, nil
		}
		if usage.ToolCalls > 0 && projected.ToolCalls > usage.ToolCalls {
			return store.QuotaDecision{Outcome: limit.OnExceeded, Limit: "tool_calls/" + string(usage.Window)}, nil
		}
	}
	return store.QuotaDecision{Allowed: true}, nil
}
