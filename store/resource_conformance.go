package store

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/kart-io/wechat-account/agent-runtime/authorization"
	"github.com/kart-io/wechat-account/agent-runtime/resource"
	"github.com/kart-io/wechat-account/agent-runtime/run"
)

// ResourceHarness is what a resource-store adapter supplies.
type ResourceHarness struct {
	Reader     ResourceReader
	Publisher  ResourcePublisher
	Authorizer ResourceAuthorizer
	// Head returns the current head version of a Kind/name, for CAS tests.
	Head func(kind resource.Kind, name string) uint64
	// Audit returns the publish audit facts recorded so far.
	Audit func() []PublishAudit
}

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

// ResourceConformance checks the whole Kind family with one suite, parameterized
// by Kind. One suite because they are one family: a per-Kind suite is how a
// Kind ends up with a publish path that skips a check.
func ResourceConformance(t *testing.T, newHarness func(t *testing.T) ResourceHarness) {
	t.Helper()

	for _, kind := range resource.Kinds() {
		t.Run(string(kind), func(t *testing.T) {
			t.Run("PublishIsCASOnExpectedHead", func(t *testing.T) {
				harness := newHarness(t)
				ctx := context.Background()

				first, err := harness.Publisher.Publish(ctx, samplePublish(kind, "a", 0))
				if err != nil {
					t.Fatalf("publish: %v", err)
				}
				if first.Ref.Version == 0 {
					t.Fatal("a published version is still zero")
				}

				// Two operators publishing concurrently must produce one
				// success and one conflict. Last-write-wins would silently
				// discard a change somebody believed had taken effect.
				if _, err := harness.Publisher.Publish(ctx, samplePublish(kind, "a", 0)); run.KindOf(err) != run.ErrorConflict {
					t.Fatalf("stale head error=%s want=conflict", run.KindOf(err))
				}

				second, err := harness.Publisher.Publish(ctx, samplePublish(kind, "a", first.Ref.Version))
				if err != nil {
					t.Fatalf("publish on current head: %v", err)
				}
				if second.Ref.Version <= first.Ref.Version {
					t.Fatal("the version did not advance")
				}
			})

			t.Run("PublishedVersionsAreImmutable", func(t *testing.T) {
				harness := newHarness(t)
				ctx := context.Background()

				first, err := harness.Publisher.Publish(ctx, samplePublish(kind, "a", 0))
				if err != nil {
					t.Fatalf("publish: %v", err)
				}
				before, err := harness.Reader.GetPublished(ctx, first.Ref)
				if err != nil {
					t.Fatalf("get: %v", err)
				}

				changed := samplePublish(kind, "a", first.Ref.Version)
				changed.Payload = json.RawMessage(`{"label":"changed"}`)
				if _, err := harness.Publisher.Publish(ctx, changed); err != nil {
					t.Fatalf("publish: %v", err)
				}

				after, err := harness.Reader.GetPublished(ctx, first.Ref)
				if err != nil {
					t.Fatalf("get: %v", err)
				}
				if string(before.Payload) != string(after.Payload) {
					t.Fatal("publishing rewrote an already-published version in place")
				}
			})

			t.Run("DigestIsStableAndContentAddressed", func(t *testing.T) {
				harness := newHarness(t)
				ctx := context.Background()

				first, err := harness.Publisher.Publish(ctx, samplePublish(kind, "a", 0))
				if err != nil {
					t.Fatalf("publish: %v", err)
				}
				same := samplePublish(kind, "a", first.Ref.Version)
				same.Payload = json.RawMessage(`{"label":"a","extra":null}`)
				reordered := samplePublish(kind, "b", 0)
				reordered.Payload = json.RawMessage("{\n \"label\" : \"a\"\n}")

				identical, err := harness.Publisher.Publish(ctx, reordered)
				if err != nil {
					t.Fatalf("publish: %v", err)
				}
				if identical.Ref.Digest != first.Ref.Digest {
					t.Fatalf("the same content under different formatting hashed differently:\n%s\n%s",
						first.Ref.Digest, identical.Ref.Digest)
				}

				different, err := harness.Publisher.Publish(ctx, same)
				if err != nil {
					t.Fatalf("publish: %v", err)
				}
				if different.Ref.Digest == first.Ref.Digest {
					t.Fatal("different content hashed the same")
				}
			})

			t.Run("PendingVersionsAreInvisibleToEveryReader", func(t *testing.T) {
				harness := newHarness(t)
				ctx := context.Background()

				approved, err := harness.Publisher.Publish(ctx, samplePublish(kind, "a", 0))
				if err != nil {
					t.Fatalf("publish: %v", err)
				}

				pending := samplePublish(kind, "a", approved.Ref.Version)
				pending.Pending = true
				pendingRef, err := harness.Publisher.Publish(ctx, pending)
				if err != nil {
					t.Fatalf("publish pending: %v", err)
				}

				if _, err := harness.Reader.GetPublished(ctx, pendingRef.Ref); err == nil {
					t.Error("a pending version was readable")
				}
				active, err := harness.Reader.ListActive(ctx, ResourceQuery{Kind: kind})
				if err != nil {
					t.Fatalf("list active: %v", err)
				}
				for _, item := range active {
					if item.Pending {
						t.Error("ListActive returned a pending version")
					}
					if item.Ref.Version == pendingRef.Ref.Version {
						t.Error("a pending version entered the active set")
					}
				}
				if harness.Head(kind, "a") != approved.Ref.Version {
					t.Error("a pending version moved the head")
				}
			})

			t.Run("UseAndPublishAuthorizationAreSeparate", func(t *testing.T) {
				harness := newHarness(t)
				ctx := context.Background()

				published, err := harness.Publisher.Publish(ctx, samplePublish(kind, "a", 0))
				if err != nil {
					t.Fatalf("publish: %v", err)
				}

				reader := authorization.PrincipalRef{Subject: "reader", Tenant: "t-1", Kind: authorization.PrincipalUser}
				if err := harness.Authorizer.AuthorizeUse(ctx, reader, published.Ref); err != nil {
					t.Fatalf("a reader could not use a published resource: %v", err)
				}
				// The right to run a definition must never imply the right to
				// rewrite it: publishing can widen allowlists and relax policy.
				if err := harness.Authorizer.AuthorizePublish(ctx, reader, PublishIntent{Kind: kind, Name: "a"}); err == nil {
					t.Fatal("use authorization also granted publish")
				}
			})

			t.Run("EveryPublishWritesOneAuditFact", func(t *testing.T) {
				harness := newHarness(t)
				ctx := context.Background()

				first, err := harness.Publisher.Publish(ctx, samplePublish(kind, "a", 0))
				if err != nil {
					t.Fatalf("publish: %v", err)
				}
				if _, err := harness.Publisher.Publish(ctx, samplePublish(kind, "a", first.Ref.Version)); err != nil {
					t.Fatalf("publish: %v", err)
				}

				audit := harness.Audit()
				if len(audit) != 2 {
					t.Fatalf("audit facts=%d want=2", len(audit))
				}
				last := audit[len(audit)-1]
				if last.PublishedBy.Zero() {
					t.Error("a publish was recorded with no publisher")
				}
				if last.PreviousRef.Version != first.Ref.Version {
					t.Error("the audit fact does not record what was replaced")
				}
				if len(last.Admission) == 0 {
					t.Error("the audit fact records no admission verdicts")
				}
			})

			t.Run("UnauthorizedPublishIsRefusedAndLeavesNothing", func(t *testing.T) {
				harness := newHarness(t)
				ctx := context.Background()

				unauthorized := samplePublish(kind, "a", 0)
				unauthorized.PublishedBy = authorization.PrincipalRef{
					Subject: "reader", Tenant: "t-1", Kind: authorization.PrincipalUser,
				}
				if _, err := harness.Publisher.Publish(ctx, unauthorized); run.KindOf(err) != run.ErrorDenied {
					t.Fatalf("error=%s want=denied", run.KindOf(err))
				}
				if harness.Head(kind, "a") != 0 {
					t.Error("a refused publish still moved the head")
				}
				if len(harness.Audit()) != 0 {
					t.Error("a refused publish wrote an audit fact")
				}
			})
		})
	}
}

func samplePublish(kind resource.Kind, name string, expectedHead uint64) PublishResourceCommand {
	return PublishResourceCommand{
		ExpectedHeadVersion: expectedHead,
		Kind:                kind,
		Name:                name,
		APIVersion:          "v1",
		Payload:             json.RawMessage(`{"label":"a"}`),
		PublishedBy: authorization.PrincipalRef{
			Subject: "publisher", Tenant: "t-1", Kind: authorization.PrincipalUser,
		},
	}
}
