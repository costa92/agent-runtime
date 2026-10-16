package session

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"io"
)

// CheckpointVersion versions a disposable storage encoding, independently of
// the event protocol. Unsupported or damaged checkpoints must be rebuilt from
// events, never migrated as execution facts or accepted from API clients.
const CheckpointVersion = 1

var ErrInvalidCheckpoint = errors.New("invalid session checkpoint")

// Reducer owns replay state, including identities needed to reject duplicates
// across checkpoint boundaries. It is local to one read/write transaction and
// is not safe for concurrent use. Its zero value is not usable; use NewReducer.
type Reducer struct {
	state  checkpointState
	failed bool
}

type checkpointState struct {
	Projection      Projection
	TurnIDs         map[string]bool
	StepIDs         map[string]bool
	CallIDs         map[string]bool
	IdempotencyKeys map[string]bool
	ApprovalIDs     map[string]bool
	AttemptIDs      map[string]bool
	Seeded          bool
	HistoryLengths  map[string]int
}

// Header: magic, encoding version, event protocol, SHA-256 of the body.
const checkpointHeaderSize = 4 + 4 + 4 + sha256.Size

func NewReducer() *Reducer {
	return &Reducer{state: checkpointState{
		TurnIDs: map[string]bool{}, StepIDs: map[string]bool{},
		CallIDs: map[string]bool{}, IdempotencyKeys: map[string]bool{},
		ApprovalIDs: map[string]bool{}, AttemptIDs: map[string]bool{},
	}}
}

// Checkpoint encodes the validated prefix. Request history is stored once in
// Projection.History; each request retains only its prefix length. Re-encoding
// full request histories would reintroduce quadratic checkpoint growth.
func (r *Reducer) Checkpoint() ([]byte, error) {
	if r == nil || r.failed || r.state.Projection.Sequence == 0 {
		return nil, ErrInvalidCheckpoint
	}
	state := r.state
	state.Projection.Turns = append([]Turn(nil), state.Projection.Turns...)
	state.Projection.CancelRequested = false // control state belongs to Store
	state.HistoryLengths = map[string]int{}
	for ti := range state.Projection.Turns {
		turn := &state.Projection.Turns[ti]
		turn.Steps = append([]Step(nil), turn.Steps...)
		for si := range turn.Steps {
			step := &turn.Steps[si]
			if step.Request != nil {
				state.HistoryLengths[step.ID] = len(step.Request.History)
				request := *step.Request
				request.History = nil
				step.Request = &request
			}
		}
	}
	var body bytes.Buffer
	if err := gob.NewEncoder(&body).Encode(state); err != nil {
		return nil, err
	}
	result := make([]byte, checkpointHeaderSize+body.Len())
	copy(result, "ARCP")
	binary.BigEndian.PutUint32(result[4:8], CheckpointVersion)
	binary.BigEndian.PutUint32(result[8:12], ProtocolVersion)
	sum := sha256.Sum256(body.Bytes())
	copy(result[12:checkpointHeaderSize], sum[:])
	copy(result[checkpointHeaderSize:], body.Bytes())
	return result, nil
}

// RestoreCheckpoint accepts only trusted store-owned bytes. The checksum
// detects corruption, not malicious modification. Binding to the Session ID
// and committed sequence is the storage adapter's responsibility. No external
// effects occur during restore; missing/invalid checkpoints are cache misses.
func RestoreCheckpoint(data []byte) (*Reducer, error) {
	if len(data) < checkpointHeaderSize || string(data[:4]) != "ARCP" || binary.BigEndian.Uint32(data[4:8]) != CheckpointVersion || binary.BigEndian.Uint32(data[8:12]) != ProtocolVersion {
		return nil, ErrInvalidCheckpoint
	}
	body := data[checkpointHeaderSize:]
	sum := sha256.Sum256(body)
	if !bytes.Equal(data[12:checkpointHeaderSize], sum[:]) {
		return nil, ErrInvalidCheckpoint
	}
	var state checkpointState
	decoder := gob.NewDecoder(bytes.NewReader(body))
	if err := decoder.Decode(&state); err != nil {
		return nil, ErrInvalidCheckpoint
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, ErrInvalidCheckpoint
	}
	p := &state.Projection
	if p.ID == "" || p.Sequence == 0 || p.TenantID == "" || p.PrincipalID == "" || p.ConfigDigest == "" || p.PluginDigest == "" || state.TurnIDs == nil || state.StepIDs == nil || state.CallIDs == nil || state.IdempotencyKeys == nil || state.ApprovalIDs == nil || state.AttemptIDs == nil || state.HistoryLengths == nil {
		return nil, ErrInvalidCheckpoint
	}
	requests := 0
	for ti := range p.Turns {
		for si := range p.Turns[ti].Steps {
			step := &p.Turns[ti].Steps[si]
			if step.Tools == nil || step.Approvals == nil || step.Budget == nil {
				return nil, ErrInvalidCheckpoint
			}
			// Reconstitute writable maps when an empty collection was omitted.
			if step.SkippedTools == nil {
				step.SkippedTools = map[string]ToolSkip{}
			}
			if step.Request != nil {
				length, ok := state.HistoryLengths[step.ID]
				if !ok || length < 0 || length > len(p.History) || step.Request.HistoryThrough >= p.Sequence {
					return nil, ErrInvalidCheckpoint
				}
				step.Request.History = p.History[:length:length]
				requests++
			}
		}
	}
	if requests != len(state.HistoryLengths) {
		return nil, ErrInvalidCheckpoint
	}
	return &Reducer{state: state}, nil
}
