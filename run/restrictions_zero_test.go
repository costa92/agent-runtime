package run_test

import (
	"reflect"
	"testing"

	"github.com/costa92/agent-runtime/run"
)

// Every field must count towards emptiness.
//
// A store writes NULL for an empty Restrictions so that "never restricted" and
// "restriction lost in transit" stay distinguishable. That decision is only as
// good as the emptiness test: a field added to the struct and forgotten here is
// written as NULL, and the Run reads back with the restriction silently gone —
// which is the direction that loses safety rather than the one that fails.
//
// Reflection rather than a hand-written list, because a hand-written list has
// the same failure mode as the predicate it is guarding.
func TestEveryRestrictionFieldCountsTowardsEmptiness(t *testing.T) {
	if !(run.Restrictions{}).Zero() {
		t.Fatal("the empty Restrictions is not Zero")
	}

	value := reflect.ValueOf(&run.Restrictions{}).Elem()
	for i := range value.NumField() {
		field := value.Type().Field(i)
		if !field.IsExported() {
			continue
		}

		one := reflect.New(field.Type).Elem()
		switch field.Type.Kind() {
		case reflect.Slice:
			one.Set(reflect.Append(one, reflect.Zero(field.Type.Elem())))
		case reflect.Bool:
			one.SetBool(true)
		case reflect.String:
			one.SetString("x")
		default:
			t.Fatalf("field %s has kind %s; teach this test how to fill it",
				field.Name, field.Type.Kind())
		}

		populated := run.Restrictions{}
		reflect.ValueOf(&populated).Elem().Field(i).Set(one)
		if populated.Zero() {
			t.Errorf("Restrictions with only %s set reports Zero; a store would write NULL and drop it",
				field.Name)
		}
	}
}
