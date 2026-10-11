package cordis

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
)

type Plugin struct {
	Name     string
	Requires []string
	Provides []string
	Setup    func(*Fiber) error
}

// Fiber owns all effects installed by one activation of a plugin.
type Fiber struct {
	mu       sync.Mutex
	ctx      *Context
	plugin   Plugin
	effects  []func() error
	provided []string
	closing  bool
	closed   bool
}

// Require only grants services listed in the plugin manifest.
func (f *Fiber) Require(name string) (any, error) {
	f.mu.Lock()
	closed := f.closed || f.closing
	f.mu.Unlock()
	if closed {
		return nil, ErrClosed
	}
	if !slices.Contains(f.plugin.Requires, name) {
		return nil, fmt.Errorf("cordis: plugin %q did not declare dependency %q", f.plugin.Name, name)
	}
	return f.ctx.Require(name)
}

func (f *Fiber) Serial(ctx context.Context, name string, payload any) (EventResult, error) {
	if err := f.ensureOpen(); err != nil {
		return EventResult{}, err
	}
	return f.ctx.Serial(ctx, name, payload)
}

func (f *Fiber) Waterfall(ctx context.Context, name string, value any) (any, error) {
	if err := f.ensureOpen(); err != nil {
		return nil, err
	}
	return f.ctx.Waterfall(ctx, name, value)
}

func (f *Fiber) Parallel(ctx context.Context, name string, payload any) ([]EventResult, error) {
	if err := f.ensureOpen(); err != nil {
		return nil, err
	}
	return f.ctx.Parallel(ctx, name, payload)
}

func (f *Fiber) ensureOpen() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed || f.closing {
		return ErrClosed
	}
	return nil
}

func (f *Fiber) needs(service string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return !f.closed && !f.closing && slices.Contains(f.plugin.Requires, service)
}

func (f *Fiber) Effect(dispose func() error) error {
	if dispose == nil {
		return errors.New("cordis: effect disposer is required")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed || f.closing {
		return ErrClosed
	}
	f.effects = append(f.effects, dispose)
	return nil
}

func (f *Fiber) Provide(name string, value any) error {
	if err := f.ensureOpen(); err != nil {
		return err
	}
	if !slices.Contains(f.plugin.Provides, name) {
		return fmt.Errorf("cordis: plugin %q did not declare service %q", f.plugin.Name, name)
	}
	remove, err := f.ctx.Provide(name, value)
	if err != nil {
		return err
	}
	f.mu.Lock()
	if f.closed || f.closing {
		f.mu.Unlock()
		_ = remove()
		return ErrClosed
	}
	f.effects = append(f.effects, remove)
	f.provided = append(f.provided, name)
	f.mu.Unlock()
	return nil
}

func (f *Fiber) Close() error {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return nil
	}
	if f.closing {
		f.mu.Unlock()
		return nil
	}
	f.closing = true
	provided := append([]string(nil), f.provided...)
	f.mu.Unlock()
	var errs []error
	for i := len(provided) - 1; i >= 0; i-- {
		errs = append(errs, f.ctx.closeDependents(provided[i]))
	}
	f.mu.Lock()
	f.closed = true
	effects := append([]func() error(nil), f.effects...)
	f.effects = nil
	f.mu.Unlock()
	for i := len(effects) - 1; i >= 0; i-- {
		errs = append(errs, effects[i]())
	}
	return errors.Join(errs...)
}

// Use activates a batch in dependency order. The batch is atomic: if any
// setup fails, all fibers activated by this call are disposed in reverse order.
func (c *Context) Use(plugins ...Plugin) ([]*Fiber, error) {
	c.activationMu.Lock()
	defer c.activationMu.Unlock()
	ordered, err := c.order(plugins)
	if err != nil {
		return nil, err
	}
	activated := make([]*Fiber, 0, len(ordered))
	rollback := func(cause error) ([]*Fiber, error) {
		for i := len(activated) - 1; i >= 0; i-- {
			cause = errors.Join(cause, activated[i].Close())
		}
		return nil, cause
	}
	for _, plugin := range ordered {
		fiber := &Fiber{ctx: c, plugin: plugin}
		activated = append(activated, fiber)
		if err := plugin.Setup(fiber); err != nil {
			return rollback(fmt.Errorf("cordis: activate %q: %w", plugin.Name, err))
		}
		for _, name := range plugin.Provides {
			c.mu.RLock()
			_, exists := c.services[name]
			c.mu.RUnlock()
			if !exists {
				return rollback(fmt.Errorf("cordis: plugin %q did not provide %q", plugin.Name, name))
			}
		}
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return rollback(ErrClosed)
	}
	c.fibers = append(c.fibers, activated...)
	c.mu.Unlock()
	return activated, nil
}

func (c *Context) order(plugins []Plugin) ([]Plugin, error) {
	c.mu.RLock()
	closed := c.closed
	existing := append([]*Fiber(nil), c.fibers...)
	c.mu.RUnlock()
	if closed {
		return nil, ErrClosed
	}
	providers := make(map[string]int)
	names := make(map[string]bool)
	for _, fiber := range existing {
		fiber.mu.Lock()
		if !fiber.closed {
			names[fiber.plugin.Name] = true
		}
		fiber.mu.Unlock()
	}
	for i, plugin := range plugins {
		if plugin.Name == "" || plugin.Setup == nil || names[plugin.Name] {
			return nil, fmt.Errorf("cordis: plugin %q needs a unique name and setup", plugin.Name)
		}
		names[plugin.Name] = true
		for _, service := range plugin.Provides {
			if service == "" {
				return nil, fmt.Errorf("cordis: plugin %q declares an empty service", plugin.Name)
			}
			if _, ok := providers[service]; ok {
				return nil, fmt.Errorf("cordis: duplicate provider for %q", service)
			}
			c.mu.RLock()
			_, present := c.services[service]
			c.mu.RUnlock()
			if present {
				return nil, fmt.Errorf("cordis: service %q is already available", service)
			}
			providers[service] = i
		}
	}
	ordered := make([]Plugin, 0, len(plugins))
	used := make([]bool, len(plugins))
	available := make(map[string]bool)
	for len(ordered) < len(plugins) {
		progress := false
		for i, plugin := range plugins {
			if used[i] {
				continue
			}
			ready := true
			for _, service := range plugin.Requires {
				if _, inBatch := providers[service]; inBatch {
					if !available[service] {
						ready = false
					}
					continue
				}
				if available[service] {
					continue
				}
				if _, err := c.Require(service); err == nil {
					continue
				}
				return nil, fmt.Errorf("cordis: plugin %q requires missing service %q", plugin.Name, service)
			}
			if !ready {
				continue
			}
			used[i] = true
			ordered = append(ordered, plugin)
			for _, service := range plugin.Provides {
				available[service] = true
			}
			progress = true
		}
		if !progress {
			return nil, errors.New("cordis: cyclic plugin service dependencies")
		}
	}
	return ordered, nil
}
