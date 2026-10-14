package cordis

import (
	"context"
	"runtime"
	"testing"
	"time"
)

func TestFiberCloseDrainsHandlerBeforeDisposingResources(t *testing.T) {
	root := New()
	started, release, disposed := make(chan struct{}), make(chan struct{}), make(chan struct{})
	fibers, err := root.Use(Plugin{Name: "listener", Setup: func(f *Fiber) error {
		if err := f.On("work", func(context.Context, any) (EventResult, error) {
			close(started)
			<-release
			select {
			case <-disposed:
				t.Error("resource disposed while handler active")
			default:
			}
			return EventResult{}, nil
		}); err != nil {
			return err
		}
		// Resource registered after listener: reverse effect order must not
		// dispose this resource before the handler has drained.
		return f.Effect(func() error { close(disposed); return nil })
	}})
	if err != nil {
		t.Fatal(err)
	}
	eventDone := make(chan error, 1)
	go func() { _, err := root.Serial(context.Background(), "work", nil); eventDone <- err }()
	<-started
	closeDone := make(chan error, 1)
	go func() { closeDone <- fibers[0].Close() }()
	deadline := time.Now().Add(time.Second)
	for {
		fibers[0].mu.Lock()
		closing := fibers[0].closing
		fibers[0].mu.Unlock()
		if closing {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("close did not start")
		}
		runtime.Gosched()
	}
	select {
	case <-disposed:
		t.Error("Close disposed resources before handler returned")
	default:
	}
	close(release)
	if err := <-eventDone; err != nil {
		t.Fatal(err)
	}
	if err := <-closeDone; err != nil {
		t.Fatal(err)
	}
	select {
	case <-disposed:
	default:
		t.Fatal("resource not disposed after handler finished")
	}
}

func TestConcurrentFiberCloseWaitsForDisposal(t *testing.T) {
	root := New()
	started, release := make(chan struct{}), make(chan struct{})
	fibers, err := root.Use(Plugin{Name: "resource", Setup: func(f *Fiber) error {
		return f.Effect(func() error { close(started); <-release; return nil })
	}})
	if err != nil {
		t.Fatal(err)
	}
	first := make(chan error, 1)
	go func() { first <- fibers[0].Close() }()
	<-started
	second := make(chan error, 1)
	go func() { second <- fibers[0].Close() }()
	select {
	case <-second:
		t.Error("concurrent Close returned before disposal finished")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-second:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("second Close stuck")
	}
}

func TestContextCloseWaitsForPendingActivation(t *testing.T) {
	root := New()
	started, release, disposed := make(chan struct{}), make(chan struct{}), make(chan struct{})
	activation := make(chan error, 1)
	go func() {
		_, err := root.Use(Plugin{Name: "pending", Setup: func(f *Fiber) error {
			if err := f.Effect(func() error { close(disposed); return nil }); err != nil {
				return err
			}
			close(started)
			<-release
			return nil
		}})
		activation <- err
	}()
	<-started
	closed := make(chan error, 1)
	go func() { closed <- root.Close() }()
	select {
	case <-closed:
		t.Fatal("Close returned while activation owns a resource")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-activation; err != nil {
		t.Fatal(err)
	}
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	select {
	case <-disposed:
	default:
		t.Fatal("Close returned without disposing activated resource")
	}
}

func TestProviderCloseWaitsForDescendantActivation(t *testing.T) {
	root := New()
	providerDisposed, consumerDisposed := make(chan struct{}), make(chan struct{})
	providers, err := root.Use(Plugin{Name: "provider", Provides: []string{"resource"}, Setup: func(f *Fiber) error {
		if err := f.Provide("resource", new(int)); err != nil {
			return err
		}
		return f.Effect(func() error {
			select {
			case <-consumerDisposed:
			default:
				t.Error("provider disposed before consumer")
			}
			close(providerDisposed)
			return nil
		})
	}})
	if err != nil {
		t.Fatal(err)
	}
	child, err := root.Scope()
	if err != nil {
		t.Fatal(err)
	}
	started, release := make(chan struct{}), make(chan struct{})
	activation := make(chan error, 1)
	go func() {
		_, err := child.Use(Plugin{Name: "consumer", Requires: []string{"resource"}, Setup: func(f *Fiber) error {
			if _, err := f.Require("resource"); err != nil {
				return err
			}
			if err := f.Effect(func() error { close(consumerDisposed); return nil }); err != nil {
				return err
			}
			close(started)
			<-release
			return nil
		}})
		activation <- err
	}()
	<-started
	closed := make(chan error, 1)
	go func() { closed <- providers[0].Close() }()
	select {
	case <-closed:
		t.Fatal("provider closed during descendant activation")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-activation; err != nil {
		t.Fatal(err)
	}
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	select {
	case <-consumerDisposed:
	default:
		t.Fatal("consumer survived provider disposal")
	}
	select {
	case <-providerDisposed:
	default:
		t.Fatal("provider resource not disposed")
	}
	if _, err := child.Require("resource"); err == nil {
		t.Fatal("disposed provider still available")
	}
}

func TestServiceRemovalWaitsForDescendantActivation(t *testing.T) {
	root := New()
	remove, err := root.Provide("resource", new(int))
	if err != nil {
		t.Fatal(err)
	}
	child, err := root.Scope()
	if err != nil {
		t.Fatal(err)
	}
	started, release, disposed := make(chan struct{}), make(chan struct{}), make(chan struct{})
	activation := make(chan error, 1)
	go func() {
		_, err := child.Use(Plugin{Name: "consumer", Requires: []string{"resource"}, Setup: func(f *Fiber) error {
			if _, err := f.Require("resource"); err != nil {
				return err
			}
			if err := f.Effect(func() error { close(disposed); return nil }); err != nil {
				return err
			}
			close(started)
			<-release
			return nil
		}})
		activation <- err
	}()
	<-started
	removed := make(chan error, 1)
	go func() { removed <- remove() }()
	select {
	case <-removed:
		t.Fatal("service removed during activation")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-activation; err != nil {
		t.Fatal(err)
	}
	if err := <-removed; err != nil {
		t.Fatal(err)
	}
	select {
	case <-disposed:
	default:
		t.Fatal("service removal did not dispose consumer")
	}
}
