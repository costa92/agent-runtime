package cordis

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func TestDependencyActivationAndRollback(t *testing.T) {
	ctx := New()
	var order []string
	plugins := []Plugin{
		{Name: "consumer", Requires: []string{"model"}, Setup: func(f *Fiber) error {
			if _, err := f.Require("model"); err != nil {
				return err
			}
			order = append(order, "consumer")
			return nil
		}},
		{Name: "provider", Provides: []string{"model"}, Setup: func(f *Fiber) error {
			order = append(order, "provider")
			return f.Provide("model", "v1")
		}},
	}
	fibers, err := ctx.Use(plugins...)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(order, []string{"provider", "consumer"}) {
		t.Fatalf("activation order: %v", order)
	}
	if err := fibers[0].Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := ctx.Require("model"); err == nil {
		t.Fatal("closed provider remained available")
	}
	if err := ctx.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestMissingAndCyclicDependencies(t *testing.T) {
	setup := func(*Fiber) error { return nil }
	for _, tc := range []struct {
		name    string
		plugins []Plugin
		want    string
	}{
		{"missing", []Plugin{{Name: "a", Requires: []string{"absent"}, Setup: setup}}, "missing"},
		{"cycle", []Plugin{{Name: "a", Provides: []string{"a"}, Requires: []string{"b"}, Setup: setup}, {Name: "b", Provides: []string{"b"}, Requires: []string{"a"}, Setup: setup}}, "cyclic"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := New().Use(tc.plugins...)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want %q", err, tc.want)
			}
		})
	}
}

func TestPluginCannotUseUndeclaredService(t *testing.T) {
	ctx := New()
	if _, err := ctx.Provide("secret", "hidden"); err != nil {
		t.Fatal(err)
	}
	fibers, err := ctx.Use(Plugin{Name: "limited", Setup: func(f *Fiber) error {
		if _, err := f.Require("secret"); err == nil {
			return errors.New("undeclared service was accessible")
		}
		if err := f.Provide("secret", "replacement"); err == nil {
			return errors.New("undeclared service was provided")
		}
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := fibers[0].Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := fibers[0].Require("secret"); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed fiber could require service: %v", err)
	}
	if _, err := fibers[0].Serial(context.Background(), "event", nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed fiber could dispatch event: %v", err)
	}
}

func TestScopesAndReverseDisposal(t *testing.T) {
	root := New()
	_, err := root.Provide("principal", "root")
	if err != nil {
		t.Fatal(err)
	}
	left, err := root.Scope()
	if err != nil {
		t.Fatal(err)
	}
	right, err := root.Scope()
	if err != nil {
		t.Fatal(err)
	}
	_, err = left.Provide("principal", "left")
	if err != nil {
		t.Fatal(err)
	}
	for scope, want := range map[*Context]string{root: "root", left: "left", right: "root"} {
		got, err := scope.Require("principal")
		if err != nil || got != want {
			t.Fatalf("scope service: %v, %v, want %q", got, err, want)
		}
	}
	var disposed []string
	_, err = root.Use(
		Plugin{Name: "first", Setup: func(f *Fiber) error {
			return f.Effect(func() error { disposed = append(disposed, "first"); return nil })
		}},
		Plugin{Name: "second", Setup: func(f *Fiber) error {
			if err := f.Effect(func() error { disposed = append(disposed, "second-1"); return nil }); err != nil {
				return err
			}
			return f.Effect(func() error { disposed = append(disposed, "second-2"); return nil })
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := root.Close(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(disposed, []string{"second-2", "second-1", "first"}) {
		t.Fatalf("disposal order: %v", disposed)
	}
	if _, err := right.Scope(); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed child usable: %v", err)
	}
}

func TestLocalProviderPrecedesConsumerEvenWithInheritedService(t *testing.T) {
	root := New()
	if _, err := root.Provide("model", "root"); err != nil {
		t.Fatal(err)
	}
	child, err := root.Scope()
	if err != nil {
		t.Fatal(err)
	}
	var got any
	_, err = child.Use(
		Plugin{Name: "consumer", Requires: []string{"model"}, Setup: func(f *Fiber) error {
			var err error
			got, err = f.Require("model")
			return err
		}},
		Plugin{Name: "provider", Provides: []string{"model"}, Setup: func(f *Fiber) error {
			return f.Provide("model", "child")
		}},
	)
	if err != nil || got != "child" {
		t.Fatalf("local dependency: %v, %v", got, err)
	}
}

func TestProviderRemovalCascadesToDependents(t *testing.T) {
	root := New()
	var disposed []string
	fibers, err := root.Use(
		Plugin{Name: "provider", Provides: []string{"model"}, Setup: func(f *Fiber) error {
			return f.Provide("model", "v1")
		}},
		Plugin{Name: "consumer", Requires: []string{"model"}, Provides: []string{"tool"}, Setup: func(f *Fiber) error {
			if err := f.Provide("tool", "v1"); err != nil {
				return err
			}
			return f.Effect(func() error { disposed = append(disposed, "consumer"); return nil })
		}},
		Plugin{Name: "grandchild", Requires: []string{"tool"}, Setup: func(f *Fiber) error {
			return f.Effect(func() error { disposed = append(disposed, "grandchild"); return nil })
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := fibers[0].Close(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(disposed, []string{"grandchild", "consumer"}) {
		t.Fatalf("dependent disposal order: %v", disposed)
	}
	for _, name := range []string{"model", "tool"} {
		if _, err := root.Require(name); err == nil {
			t.Fatalf("removed service %q remained", name)
		}
	}
	if err := fibers[1].Effect(func() error { return nil }); !errors.Is(err, ErrClosed) {
		t.Fatalf("dependent fiber remained usable: %v", err)
	}
}

func TestHostServiceRemovalClosesOnlyConsumersOfThatScope(t *testing.T) {
	root := New()
	remove, err := root.Provide("principal", "root")
	if err != nil {
		t.Fatal(err)
	}
	left, err := root.Scope()
	if err != nil {
		t.Fatal(err)
	}
	right, err := root.Scope()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := right.Provide("principal", "right"); err != nil {
		t.Fatal(err)
	}
	consumer := func(name string) Plugin {
		return Plugin{Name: name, Requires: []string{"principal"}, Setup: func(f *Fiber) error {
			_, err := f.Require("principal")
			return err
		}}
	}
	leftFibers, err := left.Use(consumer("left"))
	if err != nil {
		t.Fatal(err)
	}
	rightFibers, err := right.Use(consumer("right"))
	if err != nil {
		t.Fatal(err)
	}
	if err := remove(); err != nil {
		t.Fatal(err)
	}
	if err := leftFibers[0].Effect(func() error { return nil }); !errors.Is(err, ErrClosed) {
		t.Fatalf("inherited consumer remained active: %v", err)
	}
	if _, err := rightFibers[0].Require("principal"); err != nil {
		t.Fatalf("shadowed consumer was closed: %v", err)
	}
}

func TestActivationFailureRollsBackEffects(t *testing.T) {
	ctx := New()
	var disposed []string
	_, err := ctx.Use(
		Plugin{Name: "one", Provides: []string{"a"}, Setup: func(f *Fiber) error {
			if err := f.Provide("a", 1); err != nil {
				return err
			}
			return f.Effect(func() error { disposed = append(disposed, "one"); return nil })
		}},
		Plugin{Name: "two", Requires: []string{"a"}, Setup: func(f *Fiber) error {
			if err := f.Effect(func() error { disposed = append(disposed, "two"); return nil }); err != nil {
				return err
			}
			return fmt.Errorf("failed")
		}},
	)
	if err == nil || !reflect.DeepEqual(disposed, []string{"two", "one"}) {
		t.Fatalf("rollback: %v, %v", err, disposed)
	}
	if _, err := ctx.Require("a"); err == nil {
		t.Fatal("rolled-back service remained")
	}
}

func TestEventOrderPropagationAndCleanup(t *testing.T) {
	root := New()
	var seen []string
	fibers, err := root.Use(
		Plugin{Name: "one", Setup: func(f *Fiber) error {
			return f.On("change", func(_ context.Context, value any) (EventResult, error) {
				seen = append(seen, "one:"+value.(string))
				return EventResult{Value: value.(string) + "1", Replace: true}, nil
			})
		}},
		Plugin{Name: "two", Setup: func(f *Fiber) error {
			return f.On("change", func(_ context.Context, value any) (EventResult, error) {
				seen = append(seen, "two:"+value.(string))
				return EventResult{Value: value.(string) + "2", Replace: true, Stop: true}, nil
			})
		}},
		Plugin{Name: "three", Setup: func(f *Fiber) error {
			return f.On("change", func(_ context.Context, value any) (EventResult, error) {
				seen = append(seen, "three")
				return EventResult{}, nil
			})
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	got, err := root.Waterfall(context.Background(), "change", "x")
	if err != nil || got != "x12" || !reflect.DeepEqual(seen, []string{"one:x", "two:x1"}) {
		t.Fatalf("waterfall: %v, %v, %v", got, err, seen)
	}
	seen = nil
	result, err := root.Serial(context.Background(), "change", "x")
	if err != nil || !result.Stop || !reflect.DeepEqual(seen, []string{"one:x", "two:x"}) {
		t.Fatalf("serial: %v, %v, %v", result, err, seen)
	}
	if err := fibers[1].Close(); err != nil {
		t.Fatal(err)
	}
	seen = nil
	_, err = root.Waterfall(context.Background(), "change", "x")
	if err != nil || !reflect.DeepEqual(seen, []string{"one:x", "three"}) {
		t.Fatalf("cleanup: %v, %v", err, seen)
	}
}

func TestParallelDispatchResultsAndErrors(t *testing.T) {
	ctx := New()
	var mu sync.Mutex
	var called int
	_, err := ctx.Use(
		Plugin{Name: "one", Setup: func(f *Fiber) error {
			return f.On("p", func(context.Context, any) (EventResult, error) {
				mu.Lock()
				called++
				mu.Unlock()
				return EventResult{Value: 1, Stop: true}, errors.New("first")
			})
		}},
		Plugin{Name: "two", Setup: func(f *Fiber) error {
			return f.On("p", func(context.Context, any) (EventResult, error) {
				mu.Lock()
				called++
				mu.Unlock()
				return EventResult{Value: 2}, nil
			})
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	results, err := ctx.Parallel(context.Background(), "p", nil)
	if err == nil || !strings.Contains(err.Error(), "first") || len(results) != 2 || results[1].Value != 2 || called != 2 {
		t.Fatalf("parallel: %v, %v, called %d", results, err, called)
	}
}

func TestSerialAndWaterfallPropagateErrors(t *testing.T) {
	ctx := New()
	boom := errors.New("boom")
	called := false
	_, err := ctx.Use(
		Plugin{Name: "error", Setup: func(f *Fiber) error {
			return f.On("fail", func(context.Context, any) (EventResult, error) {
				return EventResult{}, boom
			})
		}},
		Plugin{Name: "later", Setup: func(f *Fiber) error {
			return f.On("fail", func(context.Context, any) (EventResult, error) {
				called = true
				return EventResult{}, nil
			})
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ctx.Serial(context.Background(), "fail", nil); !errors.Is(err, boom) || called {
		t.Fatalf("serial error: %v, later called %v", err, called)
	}
	if _, err := ctx.Waterfall(context.Background(), "fail", nil); !errors.Is(err, boom) || called {
		t.Fatalf("waterfall error: %v, later called %v", err, called)
	}
}
