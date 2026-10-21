package session

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

var (
	ErrNotFound     = errors.New("session not found")
	ErrConflict     = errors.New("session command conflict")
	ErrLease        = errors.New("session lease invalid or expired")
	ErrInvalidInput = errors.New("invalid session input")
)

// Input is an inbox command. IdempotencyKey is unique within a Session.
type Input struct {
	IdempotencyKey string          `json:"idempotency_key"`
	Payload        json.RawMessage `json:"payload"`
}

type InboxItem struct {
	ID             uint64          `json:"id"`
	IdempotencyKey string          `json:"idempotency_key"`
	Payload        json.RawMessage `json:"payload"`
}

type Lease struct {
	SessionID string    `json:"session_id"`
	WorkerID  string    `json:"worker_id"`
	Fence     uint64    `json:"fence"`
	ExpiresAt time.Time `json:"expires_at"`
}

// AppendCommand is one transaction. AckInboxID is zero when no inbox item is
// consumed. Acknowledgment must commit with the Turn's durable opening facts.
type AppendCommand struct {
	Lease            Lease
	ExpectedSequence uint64
	Events           []Event
	Outbox           []OutboxRecord
	AckInboxID       uint64
	Budget           BudgetPolicy
}

// Store is the only writer for Session facts and inbox state. Implementations
// must validate the lease fence and expected sequence in the same transaction
// that writes events, outbox records, budget facts, and optional inbox ack.
// Claim increments the fence monotonically; Wake records a durable signal for
// parked sessions when an external approval or effect result needs processing.
type Store interface {
	Create(ctx context.Context, start Event) (Projection, error)
	// Enqueue replays an accepted matching key even while its Turn is active.
	// New keys are rejected while a Turn is active, or cancellation is pending
	// or complete. A changed payload for an existing key is always a conflict.
	Enqueue(ctx context.Context, sessionID string, input Input) (InboxItem, bool, error)
	Claim(ctx context.Context, workerID string, limit int, ttl time.Duration) ([]Lease, error)
	// ClaimSession targets one Session for an explicit control command even if
	// ordinary work is parked. It must never replace a live lease.
	ClaimSession(ctx context.Context, sessionID, workerID string, ttl time.Duration) (Lease, error)
	Renew(ctx context.Context, lease Lease, ttl time.Duration) (Lease, error)
	Append(ctx context.Context, command AppendCommand) (Projection, error)
	Release(ctx context.Context, lease Lease) error
	RequestCancel(ctx context.Context, sessionID string) error
	CancelRequested(ctx context.Context, sessionID string) (bool, error)
	// RequestTurnCancel asks the driving worker to stop turnID only. It is a
	// conflict unless turnID is the Session's active Turn. The Store forgets
	// the request once that Turn is no longer active.
	RequestTurnCancel(ctx context.Context, sessionID, turnID string) error
	// TurnCancelRequested returns the Turn awaiting a stop, or "" for none.
	TurnCancelRequested(ctx context.Context, sessionID string) (string, error)
	Wake(ctx context.Context, sessionID string) error
	Inbox(ctx context.Context, sessionID string) ([]InboxItem, error)
	Inspect(ctx context.Context, sessionID string) (Projection, error)
	// List pages the principal's Sessions newest first as summaries; full
	// state is read one Session at a time through Inspect.
	List(ctx context.Context, tenantID, principalID string, projectID int64, after string, limit int) ([]Summary, string, error)
	Events(ctx context.Context, sessionID string, after uint64, limit int) ([]Event, error)
	Outbox(ctx context.Context, sessionID string, after uint64, limit int) ([]OutboxRecord, error)
}
