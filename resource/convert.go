package resource

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/kart-io/wechat-account/agent-runtime/run"
)

// Converter migrates a payload from an older apiVersion to the storage version.
//
// Conversion belongs to the publish path and only to the publish path. If the
// read path could convert, then what a Run executes would depend on the
// converter present in whichever deployment happened to load it — and a Run that
// pinned a digest would still get different bytes back. Converting once, at
// publish, means the stored payload is the payload.
type Converter interface {
	StorageVersion() string
	Convert(from string, payload json.RawMessage) (json.RawMessage, error)
}

// Reader is the read path. It deliberately has no conversion entry point; a
// test asserts that it cannot be type-asserted into one.
type Reader interface {
	Get(ref Ref) (json.RawMessage, error)
	ListActive(kind Kind) ([]Ref, error)
}

// Publisher is the write path: the only place conversion, admission and digest
// computation happen.
type Publisher interface {
	// Publish stores the payload at the next version, converting it to the
	// storage version first. ExpectedHead is the CAS: publishing against a head
	// that has moved is a conflict, not a silent overwrite.
	Publish(kind Kind, name, apiVersion string, payload json.RawMessage, expectedHead uint64) (Ref, error)
}

// Digest is the canonical hash of a payload, and the one rule every Kind shares.
//
// It hashes the re-encoded JSON rather than the caller's bytes so that
// whitespace, key order and the caller's marshaller cannot change the identity
// of an otherwise identical resource. Go's encoder emits object keys sorted,
// which is what makes the re-encoding canonical rather than merely tidy.
func Digest(payload json.RawMessage) (string, error) {
	canonical, err := canonicalize(payload)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:]), nil
}

// DigestOf canonicalizes and hashes any value. Used by Kinds that hold a typed
// payload rather than raw bytes.
func DigestOf(value any) (string, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", run.NewError("unencodable_payload", run.ErrorInvalid, run.RetryNever, err)
	}
	return Digest(encoded)
}

// canonicalize round-trips the payload through the decoder and encoder, which
// normalizes key order and whitespace. It uses `any` rather than a typed value
// so that the rule is the same for every Kind.
func canonicalize(payload json.RawMessage) ([]byte, error) {
	var value any
	decoder := json.NewDecoder(bytes.NewReader(payload))
	// Numbers stay as their literal text: re-encoding through float64 would
	// round a large integer id and change the digest of a payload nobody
	// edited.
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return nil, run.NewError("unparseable_payload", run.ErrorInvalid, run.RetryNever, err)
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return nil, run.NewError("unencodable_payload", run.ErrorInvalid, run.RetryNever, err)
	}
	return canonical, nil
}
