package tool

import (
	"encoding/json"
	"testing"
)

// A tool's declared media cost is what the quota is asked for, so the
// declaration has to be read from the arguments the caller actually sent —
// including the ones that say nothing about size.
func TestDeclaredMediaOpsAreReadFromTheArguments(t *testing.T) {
	for _, tc := range []struct {
		name      string
		spec      Spec
		arguments string
		want      int
	}{
		{"no declaration costs nothing", Spec{}, `{"pages":9}`, 0},
		{"a fixed producer costs its base", Spec{MediaOpsBase: 1}, `{"prompt":"a cat"}`, 1},
		{"a sized producer adds the named argument",
			Spec{MediaOpsBase: 1, MediaOpsPerArg: "pages"}, `{"pages":8}`, 9},
		{"a resumed call without the argument falls back to the base",
			Spec{MediaOpsBase: 1, MediaOpsPerArg: "pages"}, `{"book_id":"b1"}`, 1},
		{"a non-numeric argument is not a negative reservation",
			Spec{MediaOpsBase: 1, MediaOpsPerArg: "pages"}, `{"pages":"lots"}`, 1},
		{"a negative argument does not refund",
			Spec{MediaOpsBase: 1, MediaOpsPerArg: "pages"}, `{"pages":-5}`, 1},
		{"unparseable arguments still reserve the base",
			Spec{MediaOpsBase: 1, MediaOpsPerArg: "pages"}, `not json`, 1},
		{"absent arguments still reserve the base",
			Spec{MediaOpsBase: 1, MediaOpsPerArg: "pages"}, ``, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.spec.mediaOps(json.RawMessage(tc.arguments)); got != tc.want {
				t.Fatalf("mediaOps = %d, want %d", got, tc.want)
			}
		})
	}
}
