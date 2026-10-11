// Package profile resolves published agent profiles against an installed
// plugin catalog. The resulting digest pins both configuration and code
// identities without depending on any in-memory plugin instance.
package profile

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
)

type Profile struct {
	Key              string         `json:"key"`
	Version          uint64         `json:"version"`
	Scope            Scope          `json:"scope"`
	AgentVersionID   int64          `json:"agent_version_id,omitempty"`
	Plugins          []PluginConfig `json:"plugins"`
	RequiredServices []string       `json:"required_services,omitempty"`
	Capabilities     []string       `json:"capabilities,omitempty"`
}

// Scope states which owner must be supplied when a Session is created.
type Scope string

const (
	ProjectScope   Scope = "project"
	PrincipalScope Scope = "principal"
)

type PluginConfig struct {
	Name          string          `json:"name"`
	ConfigVersion uint32          `json:"config_version"`
	Config        json.RawMessage `json:"config"`
}

// InstalledPlugin is the published identity and dependency declaration of
// one installed implementation. ImplementationVersion must change whenever
// the plugin's execution behavior changes.
type InstalledPlugin struct {
	Name                  string   `json:"name"`
	ImplementationVersion string   `json:"implementation_version"`
	ConfigVersion         uint32   `json:"config_version"`
	Requires              []string `json:"requires,omitempty"`
	Provides              []string `json:"provides,omitempty"`
	Capabilities          []string `json:"capabilities,omitempty"`
}

type Catalog struct {
	Plugins          []InstalledPlugin
	BaseServices     []string
	BaseCapabilities []string
}

type Resolved struct {
	Profile  Profile           `json:"profile"`
	Manifest []InstalledPlugin `json:"manifest"`
	Digest   string            `json:"digest"`
}

// Resolve validates a profile and returns a dependency-ordered, canonical
// manifest. Peers must resolve against the same installed catalog before
// accepting a pinned digest for execution.
func Resolve(input Profile, catalog Catalog) (Resolved, error) {
	if err := validName(input.Key); err != nil {
		return Resolved{}, fmt.Errorf("profile key: %w", err)
	}
	if input.Version == 0 {
		return Resolved{}, errors.New("profile: version must be positive")
	}
	if input.Scope != ProjectScope && input.Scope != PrincipalScope {
		return Resolved{}, errors.New("profile: scope must be project or principal")
	}
	if input.AgentVersionID < 0 {
		return Resolved{}, errors.New("profile: agent version ID cannot be negative")
	}
	if len(input.Plugins) == 0 {
		return Resolved{}, errors.New("profile: at least one plugin is required")
	}
	services, err := nameSet(catalog.BaseServices)
	if err != nil {
		return Resolved{}, fmt.Errorf("base services: %w", err)
	}
	capabilities, err := nameSet(catalog.BaseCapabilities)
	if err != nil {
		return Resolved{}, fmt.Errorf("base capabilities: %w", err)
	}
	installed := make(map[string]InstalledPlugin, len(catalog.Plugins))
	for _, descriptor := range catalog.Plugins {
		if err := validName(descriptor.Name); err != nil {
			return Resolved{}, fmt.Errorf("catalog plugin: %w", err)
		}
		if _, duplicate := installed[descriptor.Name]; duplicate {
			return Resolved{}, fmt.Errorf("profile: duplicate installed plugin %q", descriptor.Name)
		}
		if descriptor.ImplementationVersion == "" || descriptor.ConfigVersion == 0 {
			return Resolved{}, fmt.Errorf("profile: plugin %q needs implementation and config versions", descriptor.Name)
		}
		for _, group := range []struct {
			label string
			items []string
		}{{"requires", descriptor.Requires}, {"provides", descriptor.Provides}, {"capabilities", descriptor.Capabilities}} {
			if _, err := nameSet(group.items); err != nil {
				return Resolved{}, fmt.Errorf("plugin %q %s: %w", descriptor.Name, group.label, err)
			}
		}
		installed[descriptor.Name] = descriptor
	}
	selected := make(map[string]PluginConfig, len(input.Plugins))
	for _, plugin := range input.Plugins {
		if err := validName(plugin.Name); err != nil {
			return Resolved{}, fmt.Errorf("profile plugin: %w", err)
		}
		if _, duplicate := selected[plugin.Name]; duplicate {
			return Resolved{}, fmt.Errorf("profile: duplicate plugin %q", plugin.Name)
		}
		descriptor, ok := installed[plugin.Name]
		if !ok {
			return Resolved{}, fmt.Errorf("profile: unknown plugin %q", plugin.Name)
		}
		if plugin.ConfigVersion != descriptor.ConfigVersion {
			return Resolved{}, fmt.Errorf("profile: plugin %q config version %d differs from installed version %d", plugin.Name, plugin.ConfigVersion, descriptor.ConfigVersion)
		}
		canonical, err := canonicalJSON(plugin.Config)
		if err != nil {
			return Resolved{}, fmt.Errorf("profile: plugin %q config: %w", plugin.Name, err)
		}
		plugin.Config = canonical
		selected[plugin.Name] = plugin
	}
	provider := make(map[string]string)
	for name := range selected {
		descriptor := installed[name]
		for _, service := range descriptor.Provides {
			if services[service] {
				return Resolved{}, fmt.Errorf("profile: plugin %q overrides base service %q", name, service)
			}
			if previous, exists := provider[service]; exists {
				return Resolved{}, fmt.Errorf("profile: service %q has providers %q and %q", service, previous, name)
			}
			provider[service] = name
		}
		for _, capability := range descriptor.Capabilities {
			capabilities[capability] = true
		}
	}
	input.RequiredServices, err = sortedNames(input.RequiredServices)
	if err != nil {
		return Resolved{}, fmt.Errorf("required services: %w", err)
	}
	input.Capabilities, err = sortedNames(input.Capabilities)
	if err != nil {
		return Resolved{}, fmt.Errorf("capabilities: %w", err)
	}
	for _, service := range input.RequiredServices {
		if !services[service] && provider[service] == "" {
			return Resolved{}, fmt.Errorf("profile: undeclared required service %q", service)
		}
	}
	for _, capability := range input.Capabilities {
		if !capabilities[capability] {
			return Resolved{}, fmt.Errorf("profile: undeclared capability %q", capability)
		}
	}
	ordered, err := dependencyOrder(selected, installed, services, provider)
	if err != nil {
		return Resolved{}, err
	}
	resolved := Resolved{Profile: Profile{Key: input.Key, Version: input.Version, Scope: input.Scope, AgentVersionID: input.AgentVersionID, RequiredServices: input.RequiredServices, Capabilities: input.Capabilities}}
	for _, name := range ordered {
		resolved.Profile.Plugins = append(resolved.Profile.Plugins, selected[name])
		descriptor := installed[name]
		descriptor.Requires, _ = sortedNames(descriptor.Requires)
		descriptor.Provides, _ = sortedNames(descriptor.Provides)
		descriptor.Capabilities, _ = sortedNames(descriptor.Capabilities)
		resolved.Manifest = append(resolved.Manifest, descriptor)
	}
	payload, err := json.Marshal(struct {
		Profile  Profile           `json:"profile"`
		Manifest []InstalledPlugin `json:"manifest"`
	}{resolved.Profile, resolved.Manifest})
	if err != nil {
		return Resolved{}, err
	}
	sum := sha256.Sum256(payload)
	resolved.Digest = "sha256:" + hex.EncodeToString(sum[:])
	return resolved, nil
}

func dependencyOrder(selected map[string]PluginConfig, installed map[string]InstalledPlugin, base map[string]bool, providers map[string]string) ([]string, error) {
	names := make([]string, 0, len(selected))
	for name := range selected {
		names = append(names, name)
	}
	slices.Sort(names)
	used := make(map[string]bool, len(names))
	ordered := make([]string, 0, len(names))
	for len(ordered) < len(names) {
		progress := false
		for _, name := range names {
			if used[name] {
				continue
			}
			ready := true
			for _, service := range installed[name].Requires {
				if base[service] {
					continue
				}
				provider := providers[service]
				if provider == "" {
					return nil, fmt.Errorf("profile: plugin %q has undeclared dependency %q", name, service)
				}
				if !used[provider] {
					ready = false
				}
			}
			if ready {
				used[name] = true
				ordered = append(ordered, name)
				progress = true
			}
		}
		if !progress {
			return nil, errors.New("profile: cyclic plugin service dependencies")
		}
	}
	return ordered, nil
}

func canonicalJSON(raw json.RawMessage) (json.RawMessage, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		raw = json.RawMessage(`{}`)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, errors.New("configuration must contain exactly one JSON value")
	}
	return json.Marshal(value)
}

func sortedNames(items []string) ([]string, error) {
	set, err := nameSet(items)
	if err != nil {
		return nil, err
	}
	if len(set) == 0 {
		return nil, nil
	}
	names := make([]string, 0, len(set))
	for name := range set {
		names = append(names, name)
	}
	slices.Sort(names)
	return names, nil
}

func nameSet(items []string) (map[string]bool, error) {
	set := make(map[string]bool, len(items))
	for _, name := range items {
		if err := validName(name); err != nil {
			return nil, err
		}
		if set[name] {
			return nil, fmt.Errorf("duplicate name %q", name)
		}
		set[name] = true
	}
	return set, nil
}

func validName(name string) error {
	if name == "" || strings.TrimSpace(name) != name || strings.ContainsAny(name, "\r\n\t ") {
		return fmt.Errorf("invalid name %q", name)
	}
	return nil
}
