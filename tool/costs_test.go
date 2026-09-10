package tool

import (
	"encoding/json"
	"testing"
)

// A tool's declared cost is what the quota is asked for, so the declaration
// has to be read from the arguments the caller actually sent — including the
// ones that say nothing about size.
func TestDeclaredCostsAreReadFromTheArguments(t *testing.T) {
	sized := Spec{Costs: []UnitCost{{Unit: "images", Base: 1, PerArg: "pages"}}}
	for _, tc := range []struct {
		name      string
		spec      Spec
		arguments string
		want      int
	}{
		{"no declaration costs nothing", Spec{}, `{"pages":9}`, 0},
		{"a fixed producer costs its base", Spec{Costs: []UnitCost{{Unit: "images", Base: 1}}}, `{"prompt":"a cat"}`, 1},
		{"a sized producer adds the named argument", sized, `{"pages":8}`, 9},
		{"a resumed call without the argument falls back to the base", sized, `{"book_id":"b1"}`, 1},
		{"a non-numeric argument is not a negative reservation", sized, `{"pages":"lots"}`, 1},
		{"a negative argument does not refund", sized, `{"pages":-5}`, 1},
		{"unparseable arguments still reserve the base", sized, `not json`, 1},
		{"absent arguments still reserve the base", sized, ``, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.spec.declaredCosts(json.RawMessage(tc.arguments)).Unit("images"); got != tc.want {
				t.Fatalf("declared images = %d, want %d", got, tc.want)
			}
		})
	}
}

// Two units on one tool are two reservations, and a nameless cost is dropped
// rather than charged to "".
func TestEachDeclaredUnitIsReservedSeparately(t *testing.T) {
	spec := Spec{Costs: []UnitCost{
		{Unit: "images", Base: 1}, {Unit: "pages", PerArg: "pages"}, {Base: 5},
	}}
	got := spec.declaredCosts(json.RawMessage(`{"pages":3}`))
	if got.Unit("images") != 1 || got.Unit("pages") != 3 || len(got.Units) != 2 {
		t.Fatalf("declaredCosts = %+v, want images=1 pages=3 and nothing else", got.Units)
	}
}
