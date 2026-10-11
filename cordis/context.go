// Package cordis implements scoped plugin composition and process-local events.
// Durable session events belong to the session store, not to this package.
package cordis

import (
	"errors"
	"fmt"
	"sync"
)

var ErrClosed = errors.New("cordis: context or fiber is closed")

// Context is a scope for services, plugins, and transient event listeners.
// A child sees its ancestors' services and listeners; its own registrations
// are invisible to its parent and siblings.
type Context struct {
	mu           sync.RWMutex
	activationMu sync.Mutex
	parent       *Context
	children     []*Context
	fibers       []*Fiber
	services     map[string]any
	listeners    map[string][]*listener
	closed       bool
}

func New() *Context {
	return &Context{services: make(map[string]any), listeners: make(map[string][]*listener)}
}

func (c *Context) Scope() (*Context, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, ErrClosed
	}
	child := New()
	child.parent = c
	c.children = append(c.children, child)
	return child, nil
}

// Provide installs a service in this scope. Its disposer only removes this
// specific registration, so a later scope-local provider is never removed.
func (c *Context) Provide(name string, value any) (func() error, error) {
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
		errs = append(errs, fibers[i].Close())
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

// Close disposes child scopes and plugin fibers in reverse creation order.
func (c *Context) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	children := append([]*Context(nil), c.children...)
	fibers := append([]*Fiber(nil), c.fibers...)
	c.mu.Unlock()
	var errs []error
	for i := len(children) - 1; i >= 0; i-- {
		errs = append(errs, children[i].Close())
	}
	for i := len(fibers) - 1; i >= 0; i-- {
		errs = append(errs, fibers[i].Close())
	}
	return errors.Join(errs...)
}
