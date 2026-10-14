// Package cordis implements scoped plugin composition and process-local events.
// Durable session events belong to the session store, not to this package.
package cordis

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

var ErrClosed = errors.New("cordis: context or fiber is closed")

// Context is a scope for services, plugins, and transient event listeners.
// A child sees its ancestors' services and listeners; its own registrations
// are invisible to its parent and siblings.
type Context struct {
	mu          sync.RWMutex
	lifecycleMu *sync.Mutex
	parent      *Context
	children    []*Context
	fibers      []*Fiber
	services    map[string]any
	listeners   map[string][]*listener
	closed      bool
	closeDone   chan struct{}
	closeErr    error
	closeOnce   sync.Once
}

func New() *Context {
	return &Context{lifecycleMu: &sync.Mutex{}, services: make(map[string]any), listeners: make(map[string][]*listener), closeDone: make(chan struct{})}
}

func (c *Context) Scope() (*Context, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, ErrClosed
	}
	child := New()
	child.lifecycleMu = c.lifecycleMu
	child.parent = c
	c.children = append(c.children, child)
	return child, nil
}

// Provide installs a service in this scope. Its disposer only removes this
// specific registration, so a later scope-local provider is never removed.
// Provide and its disposer are lifecycle-owner operations. Do not invoke them
// from Setup, handlers, or disposers in this scope tree; Setup uses Fiber.Provide.
func (c *Context) Provide(name string, value any) (func() error, error) {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	remove, err := c.provide(name, value)
	if err != nil {
		return nil, err
	}
	return func() error {
		c.lifecycleMu.Lock()
		defer c.lifecycleMu.Unlock()
		return remove()
	}, nil
}

// provide runs inside activation or teardown, which already owns lifecycleMu.
func (c *Context) provide(name string, value any) (func() error, error) {
	if name == "" || value == nil {
		return nil, errors.New("cordis: service name and value are required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, ErrClosed
	}
	if _, exists := c.services[name]; exists {
		return nil, fmt.Errorf("cordis: service %q is already provided in this scope", name)
	}
	c.services[name] = value
	var once sync.Once
	var disposeErr error
	return func() error {
		once.Do(func() {
			disposeErr = c.closeDependents(name)
			c.mu.Lock()
			defer c.mu.Unlock()
			delete(c.services, name)
		})
		return disposeErr
	}, nil
}

// closeDependents removes active consumers before their provider disappears.
// The lookup uses the nearest provider scope, so a child that shadows this
// service is independent of the ancestor provider.
func (c *Context) closeDependents(name string) error {
	var fibers []*Fiber
	var walk func(*Context)
	walk = func(scope *Context) {
		scope.mu.RLock()
		localFibers := append([]*Fiber(nil), scope.fibers...)
		children := append([]*Context(nil), scope.children...)
		scope.mu.RUnlock()
		for _, fiber := range localFibers {
			if fiber.needs(name) && serviceScope(scope, name) == c {
				fibers = append(fibers, fiber)
			}
		}
		for _, child := range children {
			walk(child)
		}
	}
	walk(c)
	var errs []error
	for i := len(fibers) - 1; i >= 0; i-- {
		errs = append(errs, fibers[i].close())
	}
	return errors.Join(errs...)
}

func serviceScope(scope *Context, name string) *Context {
	for scope != nil {
		scope.mu.RLock()
		_, exists := scope.services[name]
		scope.mu.RUnlock()
		if exists {
			return scope
		}
		scope = scope.parent
	}
	return nil
}

func (c *Context) Require(name string) (any, error) {
	for scope := c; scope != nil; scope = scope.parent {
		scope.mu.RLock()
		if scope.closed {
			scope.mu.RUnlock()
			return nil, ErrClosed
		}
		value, ok := scope.services[name]
		scope.mu.RUnlock()
		if ok {
			return value, nil
		}
	}
	return nil, fmt.Errorf("cordis: required service %q is unavailable", name)
}

// Close requests teardown once and waits for it within the caller's deadline.
// If the deadline expires, teardown continues and resources remain owned until
// their handlers finish. A later Close waits for the same teardown operation.
// Call it outside Setup, handlers, and disposers in this scope tree; those
// callbacks must return before resources can be safely disposed.
func (c *Context) Close(ctx context.Context) error {
	c.closeOnce.Do(func() {
		go func() {
			c.lifecycleMu.Lock()
			defer c.lifecycleMu.Unlock()
			_ = c.close()
		}()
	})
	select {
	case <-c.closeDone:
		c.mu.RLock()
		defer c.mu.RUnlock()
		return c.closeErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Context) close() error {
	c.mu.Lock()
	if c.closed {
		done := c.closeDone
		c.mu.Unlock()
		<-done
		c.mu.RLock()
		err := c.closeErr
		c.mu.RUnlock()
		return err
	}
	c.closed = true
	children := append([]*Context(nil), c.children...)
	fibers := append([]*Fiber(nil), c.fibers...)
	c.mu.Unlock()
	var errs []error
	for i := len(children) - 1; i >= 0; i-- {
		errs = append(errs, children[i].close())
	}
	for i := len(fibers) - 1; i >= 0; i-- {
		errs = append(errs, fibers[i].close())
	}
	c.mu.Lock()
	c.closeErr = errors.Join(errs...)
	close(c.closeDone)
	err := c.closeErr
	c.mu.Unlock()
	return err
}
