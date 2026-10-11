package profile

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func sample() (Profile, Catalog) {
	return Profile{
			Key: "article_writer", Version: 2, Scope: ProjectScope,
			Plugins: []PluginConfig{
				{Name: "writer", ConfigVersion: 3, Config: json.RawMessage(`{"temperature":0.2,"model":{"name":"m","limit":9007199254740993}}`)},
				{Name: "model", ConfigVersion: 1, Config: json.RawMessage(`{"region":"a"}`)},
			},
			RequiredServices: []string{"model", "policy"}, Capabilities: []string{"article", "llm"},
		}, Catalog{
			BaseServices: []string{"policy"},
			Plugins: []InstalledPlugin{
				{Name: "writer", ImplementationVersion: "2.1.0", ConfigVersion: 3, Requires: []string{"policy", "model"}, Provides: []string{"article"}, Capabilities: []string{"article"}},
				{Name: "model", ImplementationVersion: "1.4.0", ConfigVersion: 1, Provides: []string{"model"}, Capabilities: []string{"llm"}},
			},
		}
}

func TestCanonicalDigestIgnoresDeclarationAndJSONObjectOrder(t *testing.T) {
	input, catalog := sample()
	first, err := Resolve(input, catalog)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Digest) != len("sha256:")+64 {
		t.Fatalf("digest: %q", first.Digest)
	}
	if !reflect.DeepEqual([]string{first.Profile.Plugins[0].Name, first.Profile.Plugins[1].Name}, []string{"model", "writer"}) {
		t.Fatalf("dependency order: %+v", first.Profile.Plugins)
	}
	input.Plugins[0], input.Plugins[1] = input.Plugins[1], input.Plugins[0]
	input.Plugins[1].Config = json.RawMessage(`{ "model": { "limit":9007199254740993, "name":"m" }, "temperature":0.2 }`)
	input.RequiredServices[0], input.RequiredServices[1] = input.RequiredServices[1], input.RequiredServices[0]
	input.Capabilities[0], input.Capabilities[1] = input.Capabilities[1], input.Capabilities[0]
	catalog.Plugins[0], catalog.Plugins[1] = catalog.Plugins[1], catalog.Plugins[0]
	second, err := Resolve(input, catalog)
	if err != nil {
		t.Fatal(err)
	}
	if first.Digest != second.Digest {
		t.Fatalf("canonical digest changed: %s vs %s", first.Digest, second.Digest)
	}
}

func TestDigestPinsProfileConfigAndInstalledImplementation(t *testing.T) {
	input, catalog := sample()
	base, err := Resolve(input, catalog)
	if err != nil {
		t.Fatal(err)
	}
	assertChanged := func(label string, profile Profile, installed Catalog) {
		t.Helper()
		got, err := Resolve(profile, installed)
		if err != nil {
			t.Fatalf("%s: %v", label, err)
		}
		if got.Digest == base.Digest {
			t.Fatalf("%s did not change digest", label)
		}
	}
	changed := input
	changed.Key = "another_agent"
	assertChanged("profile key", changed, catalog)
	changed = input
	changed.Version++
	assertChanged("profile version", changed, catalog)
	changed = input
	changed.Scope = PrincipalScope
	assertChanged("profile scope", changed, catalog)
	changed = input
	changed.Plugins = append([]PluginConfig(nil), input.Plugins...)
	changed.Plugins[0].Config = json.RawMessage(`{"temperature":0.3,"model":{"name":"m","limit":9007199254740993}}`)
	assertChanged("config", changed, catalog)
	updated := catalog
	updated.Plugins = append([]InstalledPlugin(nil), catalog.Plugins...)
	updated.Plugins[0].ImplementationVersion = "2.1.1"
	assertChanged("implementation version", input, updated)
}

func TestMissingAndDuplicatePlugins(t *testing.T) {
	input, catalog := sample()
	for _, tc := range []struct {
		name string
		edit func(*Profile, *Catalog)
		want string
	}{
		{"unknown", func(p *Profile, _ *Catalog) { p.Plugins[0].Name = "missing" }, "unknown plugin"},
		{"duplicate selected", func(p *Profile, _ *Catalog) { p.Plugins[1].Name = "writer" }, "duplicate plugin"},
		{"duplicate installed", func(_ *Profile, c *Catalog) { c.Plugins = append(c.Plugins, c.Plugins[0]) }, "duplicate installed plugin"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := input
			p.Plugins = append([]PluginConfig(nil), input.Plugins...)
			c := catalog
			c.Plugins = append([]InstalledPlugin(nil), catalog.Plugins...)
			tc.edit(&p, &c)
			_, err := Resolve(p, c)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want %q", err, tc.want)
			}
		})
	}
}

func TestDependenciesCapabilitiesAndCycles(t *testing.T) {
	input, catalog := sample()
	for _, tc := range []struct {
		name string
		edit func(*Profile, *Catalog)
		want string
	}{
		{"missing dependency", func(_ *Profile, c *Catalog) { c.Plugins[0].Requires = []string{"unknown"} }, "undeclared dependency"},
		{"profile service", func(p *Profile, _ *Catalog) { p.RequiredServices = []string{"absent"} }, "undeclared required service"},
		{"profile capability", func(p *Profile, _ *Catalog) { p.Capabilities = []string{"absent"} }, "undeclared capability"},
		{"cycle", func(_ *Profile, c *Catalog) { c.Plugins[1].Requires = []string{"article"} }, "cyclic"},
		{"duplicate provider", func(_ *Profile, c *Catalog) { c.Plugins[1].Provides = []string{"article"} }, "providers"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := input
			p.Plugins = append([]PluginConfig(nil), input.Plugins...)
			c := catalog
			c.Plugins = append([]InstalledPlugin(nil), catalog.Plugins...)
			tc.edit(&p, &c)
			_, err := Resolve(p, c)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want %q", err, tc.want)
			}
		})
	}
}

func TestConfigVersionAndJSONValidation(t *testing.T) {
	input, catalog := sample()
	input.Plugins[0].ConfigVersion = 2
	if _, err := Resolve(input, catalog); err == nil || !strings.Contains(err.Error(), "config version") {
		t.Fatalf("version mismatch: %v", err)
	}
	input.Plugins[0].ConfigVersion = 3
	input.Plugins[0].Config = json.RawMessage(`{"bad":`)
	if _, err := Resolve(input, catalog); err == nil || !strings.Contains(err.Error(), "config") {
		t.Fatalf("invalid config: %v", err)
	}
}

func TestIndependentPluginsUseStableNameOrder(t *testing.T) {
	input := Profile{Key: "simple", Version: 1, Scope: PrincipalScope, Plugins: []PluginConfig{{Name: "z", ConfigVersion: 1}, {Name: "a", ConfigVersion: 1}}}
	catalog := Catalog{Plugins: []InstalledPlugin{{Name: "z", ImplementationVersion: "1", ConfigVersion: 1}, {Name: "a", ImplementationVersion: "1", ConfigVersion: 1}}}
	got, err := Resolve(input, catalog)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual([]string{got.Profile.Plugins[0].Name, got.Profile.Plugins[1].Name}, []string{"a", "z"}) {
		t.Fatalf("independent order: %+v", got.Profile.Plugins)
	}
}
