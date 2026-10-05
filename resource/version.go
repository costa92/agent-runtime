package resource

import (
	"fmt"
	"regexp"

	"github.com/costa92/agent-runtime/run"
)

// apiVersionPattern is the accepted shape: v1, v2, v1alpha1, v2beta3.
var apiVersionPattern = regexp.MustCompile(`^v[1-9][0-9]*((alpha|beta)[1-9][0-9]*)?$`)

// apiVersions declares, per Kind, every apiVersion that may be published and
// which one is stored.
//
// The distinction is what makes additive change cheap and breaking change
// visible: a field that older payloads simply lack keeps the same apiVersion,
// because every existing payload still means what it said. A field that changes
// what an existing payload means needs a new one, because the old bytes would
// otherwise be silently reinterpreted.
type kindVersions struct {
	storage   string
	published []string
}

var apiVersions = map[Kind]kindVersions{
	KindDefinition:   {storage: "v1", published: []string{"v1"}},
	KindPolicy:       {storage: "v1", published: []string{"v1"}},
	KindQuota:        {storage: "v1", published: []string{"v1"}},
	KindModelProfile: {storage: "v1", published: []string{"v1"}},
}

// StorageVersion returns the apiVersion a Kind is stored as.
func StorageVersion(kind Kind) (string, error) {
	versions, ok := apiVersions[kind]
	if !ok {
		return "", run.NewError("unknown_kind", run.ErrorInvalid, run.RetryNever)
	}
	return versions.storage, nil
}

// PublishedVersions returns every apiVersion a Kind accepts on publish.
func PublishedVersions(kind Kind) ([]string, error) {
	versions, ok := apiVersions[kind]
	if !ok {
		return nil, run.NewError("unknown_kind", run.ErrorInvalid, run.RetryNever)
	}
	return append([]string(nil), versions.published...), nil
}

// ValidateAPIVersion rejects an apiVersion this Kind cannot accept.
//
// Unparseable and unknown are the same answer — invalid — on purpose. Guessing
// what "v1.0" or "1" was supposed to mean would make the accepted set depend on
// the parser rather than on this table.
func ValidateAPIVersion(kind Kind, apiVersion string) error {
	versions, ok := apiVersions[kind]
	if !ok {
		return run.NewError("unknown_kind", run.ErrorInvalid, run.RetryNever)
	}
	if !apiVersionPattern.MatchString(apiVersion) {
		return run.NewError("unparseable_api_version", run.ErrorInvalid, run.RetryNever,
			fmt.Errorf("%q is not a valid apiVersion", apiVersion))
	}
	for _, known := range versions.published {
		if apiVersion == known {
			return nil
		}
	}
	return run.NewError("unsupported_api_version", run.ErrorInvalid, run.RetryNever,
		fmt.Errorf("%s does not accept %q", kind, apiVersion))
}
