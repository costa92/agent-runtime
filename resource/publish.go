package resource

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/kart-io/wechat-account/agent-runtime/run"
)

// Converters maps each Kind to its converter. A Kind with no converter is not
// publishable: conversion is where an older apiVersion becomes storable, and a
// Kind that cannot answer that question has no defined storage form.
type Converters map[Kind]Converter

// Prepare runs the publish-path preamble: convert the payload to the storage
// version, run the admission chain against the converted bytes, and compute the
// canonical digest.
//
// It returns the converted payload rather than mutating anything, because the
// actual insert, head move and audit write belong to one Store transaction and
// this function is not inside it.
//
// Conversion happens exactly once, here. A second conversion on read would make
// what a Run executes depend on the deployment that loaded it.
func Prepare(
	ctx context.Context,
	chain *AdmissionChain,
	converters Converters,
	kind Kind,
	name, apiVersion string,
	payload json.RawMessage,
) (converted json.RawMessage, digest string, verdicts []Verdict, err error) {
	if !kind.Valid() {
		return nil, "", nil, run.NewError("unknown_kind", run.ErrorInvalid, run.RetryNever)
	}
	if err := ValidateAPIVersion(kind, apiVersion); err != nil {
		return nil, "", nil, err
	}

	storage, err := StorageVersion(kind)
	if err != nil {
		return nil, "", nil, err
	}

	converted = payload
	if apiVersion != storage {
		converter, ok := converters[kind]
		if !ok {
			return nil, "", nil, run.NewError("no_converter", run.ErrorInvalid, run.RetryNever,
				fmt.Errorf("%s has no converter to %s", kind, storage))
		}
		if converter.StorageVersion() != storage {
			// A converter that disagrees with the table about the storage
			// version would write payloads the reader cannot interpret.
			return nil, "", nil, run.NewError("converter_version_mismatch", run.ErrorInternal, run.RetryNever,
				fmt.Errorf("%s converter targets %s, table says %s", kind, converter.StorageVersion(), storage))
		}
		converted, err = converter.Convert(apiVersion, payload)
		if err != nil {
			return nil, "", nil, run.NewError("conversion_failed", run.ErrorInvalid, run.RetryNever, err)
		}
	}

	if chain != nil {
		verdicts, err = chain.Admit(ctx, AdmissionRequest{
			Kind:       kind,
			Name:       name,
			APIVersion: apiVersion,
			Payload:    converted,
		})
		if err != nil {
			return nil, "", verdicts, err
		}
	}

	digest, err = Digest(converted)
	if err != nil {
		return nil, "", verdicts, err
	}
	return converted, digest, verdicts, nil
}
