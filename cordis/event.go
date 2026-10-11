package cordis

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// EventResult explicitly says whether a listener replaces the event payload
// and whether subsequent listeners should be skipped. Stop has no effect in
// parallel dispatch, where every listener is invoked.
type EventResult struct {
	Value   any
	Replace bool
	Stop    bool
}

type EventHandler func(context.Context, any) (EventResult, error)

type listener struct {
	handler EventHandler
	active  bool
	mu      sync.RWMutex
}

func (l *listener) call(ctx context.Context, value any) (EventResult, error) {
	l.mu.RLock()
	if !l.active {
		l.mu.RUnlock()
		return EventResult{}, nil
	}
	handler := l.handler
	l.mu.RUnlock()
	return handler(ctx, value)
}

// On registers a listener at the fiber's scope. It is removed when the fiber
// closes. Listeners are called in ancestor-to-descendant registration order.
func (f *Fiber) On(name string, handler EventHandler) error {
	if name == "" || handler == nil {
		return errors.New("cordis: event name and handler are required")
	}
	l := &listener{handler: handler, active: true}
	f.ctx.mu.Lock()
	if f.ctx.closed {
		f.ctx.mu.Unlock()
		return ErrClosed
	}
	f.ctx.listeners[name] = append(f.ctx.listeners[name], l)
	f.ctx.mu.Unlock()
	remove := func() error {
		l.mu.Lock()
		l.active = false
		l.mu.Unlock()
		f.ctx.mu.Lock()
		defer f.ctx.mu.Unlock()
		current := f.ctx.listeners[name]
		for i, candidate := range current {
			if candidate == l {
				f.ctx.listeners[name] = append(current[:i], current[i+1:]...)
				break
			}
		}
		return nil
	}
	if err := f.Effect(remove); err != nil {
		_ = remove()
		return err
	}
	return nil
}

func (c *Context) eventListeners(name string) ([]*listener, error) {
	var chain []*Context
	for scope := c; scope != nil; scope = scope.parent {
		chain = append(chain, scope)
	}
	var listeners []*listener
	for i := len(chain) - 1; i >= 0; i-- {
		chain[i].mu.RLock()
		if chain[i].closed {
			chain[i].mu.RUnlock()
			return nil, ErrClosed
		}
		listeners = append(listeners, chain[i].listeners[name]...)
		chain[i].mu.RUnlock()
	}
	return listeners, nil
}

// Serial passes the original payload to listeners in registration order.
// It stops on an error or an explicit Stop result.
func (c *Context) Serial(ctx context.Context, name string, payload any) (EventResult, error) {
	listeners, err := c.eventListeners(name)
	if err != nil {
		return EventResult{}, err
	}
	var last EventResult
	for _, l := range listeners {
		if err := ctx.Err(); err != nil {
			return last, err
		}
		last, err = l.call(ctx, payload)
		if err != nil || last.Stop {
			return last, err
		}
	}
	return last, nil
}

// Waterfall passes each explicit replacement to the next listener. A listener
// that does not set Replace leaves the value unchanged. Stop ends propagation.
func (c *Context) Waterfall(ctx context.Context, name string, value any) (any, error) {
	listeners, err := c.eventListeners(name)
	if err != nil {
		return nil, err
	}
	for _, l := range listeners {
		if err := ctx.Err(); err != nil {
			return value, err
		}
		result, err := l.call(ctx, value)
		if err != nil {
			return value, err
		}
		if result.Replace {
			value = result.Value
		}
		if result.Stop {
			break
		}
	}
	return value, nil
}

// Parallel invokes every listener and returns results in registration order.
// All errors are joined after every listener has finished.
func (c *Context) Parallel(ctx context.Context, name string, payload any) ([]EventResult, error) {
	listeners, err := c.eventListeners(name)
	if err != nil {
		return nil, err
	}
	results := make([]EventResult, len(listeners))
	errs := make([]error, len(listeners))
	var wg sync.WaitGroup
	for i, l := range listeners {
		wg.Add(1)
		go func(i int, l *listener) {
			defer wg.Done()
			if err := ctx.Err(); err != nil {
				errs[i] = err
				return
			}
			results[i], errs[i] = l.call(ctx, payload)
			if errs[i] != nil {
				errs[i] = fmt.Errorf("cordis: listener %d: %w", i, errs[i])
			}
		}(i, l)
	}
	wg.Wait()
	return results, errors.Join(errs...)
}
