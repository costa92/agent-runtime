package agentruntime_test

import (
	"encoding/json"
	"io"
	"os/exec"
	"slices"
	"strings"
	"testing"
)

// The Runtime's dependency graph is deny-by-default: production code may reach
// the standard library and nothing else until something is added here with a
// reason. An allowlist is the only form of this check that holds — a deny-list
// of "bad" dependencies passes for everything nobody thought to name, and the
// dependency that breaks embedding is by definition the one nobody expected.
//
// Empty today, which is the point: the module compiles against the standard
// library alone.
var runtimeModuleAllowlist = map[string]string{}

// Prefixes that are wrong for this module whatever module they arrive in. The
// allowlist above answers "may we depend on this module"; these answer "may we
// import this kind of thing at all", and they catch the case an allowlist
// cannot: a permitted module growing a package that drags in a driver, a
// transport, or a product concept.
var deniedImportPrefixes = []struct {
	prefix string
	reason string
}{
	{"github.com/kart-io/wechat-account/api/", "product code: the host owns business definitions, the Runtime executes them"},
	{"github.com/kart-io/wechat-account/agent-platform", "the platform depends on the Runtime, never the reverse"},
	{"gorm.io/", "persistence is a host-supplied Store port"},
	{"github.com/jackc/", "database drivers are a host-supplied Store port"},
	{"github.com/lib/pq", "database drivers are a host-supplied Store port"},
	{"github.com/mattn/go-sqlite3", "database drivers are a host-supplied Store port"},
	{"github.com/gin-gonic/", "transport is the host's"},
	{"github.com/gorilla/", "transport is the host's"},
	{"net/http", "transport is the host's; a Runtime that dials has stopped being embeddable"},
	{"github.com/sashabaranov/", "model providers are a host-supplied Model port"},
	{"github.com/openai/", "model providers are a host-supplied Model port"},
	{"google.golang.org/genai", "model providers are a host-supplied Model port"},
	{"testing", "a test framework in the production graph links itself into every host binary that imports the package holding it"},
}

// conformancePackage holds the adapter suites a host runs against its own Store,
// Model, Memory and Policy implementations. They cannot live in _test.go files —
// a test file is not importable — so they are production-tagged code that
// legitimately imports `testing`, and they are excluded from the scan below.
// Nothing else may be: the whole point of the exclusion is that it is one
// package, and that no host reaches it from a production import path.
const conformancePackage = "github.com/costa92/agent-runtime/conformance"

// packagesExcept lists the module's packages minus the named ones.
func packagesExcept(t *testing.T, dir string, excluded ...string) []string {
	t.Helper()
	cmd := exec.Command("go", "list", "./...")
	cmd.Dir = dir
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list ./... in %s: %v", dir, err)
	}
	var kept []string
	for _, line := range strings.Fields(string(out)) {
		if !slices.Contains(excluded, line) {
			kept = append(kept, line)
		}
	}
	if len(kept) == 0 {
		t.Fatal("no packages left to scan; the check would pass vacuously")
	}
	return kept
}

// listedPackage is the subset of `go list -json` this test reads.
type listedPackage struct {
	ImportPath string `json:"ImportPath"`
	Standard   bool   `json:"Standard"`
	Module     *struct {
		Path string `json:"Path"`
	} `json:"Module"`
}

// productionDeps returns every package the module's production build reaches.
// `go list -deps` without -test excludes test-only imports on purpose: a test
// may use anything, and holding tests to the embedding constraint would only
// push fixtures into the production tree.
func productionDeps(t *testing.T, dir string) []listedPackage {
	t.Helper()

	// The conformance package is listed away rather than filtered out of the
	// results: `go list -deps` returns a flat set, so a dependency pulled in
	// only by that package is indistinguishable from one pulled in by the
	// Runtime itself once it is in the list.
	roots := packagesExcept(t, dir, conformancePackage)
	cmd := exec.Command("go", append([]string{"list", "-deps", "-json"}, roots...)...)
	cmd.Dir = dir
	out, err := cmd.Output()
	if err != nil {
		var stderr string
		if ee, ok := err.(*exec.ExitError); ok {
			stderr = string(ee.Stderr)
		}
		t.Fatalf("go list -deps in %s: %v\n%s", dir, err, stderr)
	}

	var pkgs []listedPackage
	dec := json.NewDecoder(strings.NewReader(string(out)))
	for {
		var pkg listedPackage
		if err := dec.Decode(&pkg); err == io.EOF {
			break
		} else if err != nil {
			t.Fatalf("decode go list output: %v", err)
		}
		pkgs = append(pkgs, pkg)
	}
	if len(pkgs) == 0 {
		t.Fatal("go list returned no packages; the scanner would pass vacuously")
	}
	return pkgs
}

func TestRuntimeProductionDependencyAllowlist(t *testing.T) {
	for _, pkg := range productionDeps(t, ".") {
		if pkg.Standard {
			continue
		}
		if pkg.Module == nil {
			t.Errorf("package %s has no module; cannot be checked against the allowlist", pkg.ImportPath)
			continue
		}
		if pkg.Module.Path == "github.com/costa92/agent-runtime" {
			continue
		}
		if _, ok := runtimeModuleAllowlist[pkg.Module.Path]; !ok {
			t.Errorf("module %s (via %s) is not in the Runtime allowlist; add it with a reason or keep it in the host",
				pkg.Module.Path, pkg.ImportPath)
		}
	}
}

func TestRuntimeRejectsHostTransportProviderAndProductImports(t *testing.T) {
	for _, pkg := range productionDeps(t, ".") {
		for _, denied := range deniedImportPrefixes {
			// A standard-library package matching a denied prefix is still
			// denied: net/http is in the list precisely because it is standard.
			if strings.HasPrefix(pkg.ImportPath, denied.prefix) {
				t.Errorf("%s is reachable from Runtime production code (denied prefix %q): %s",
					pkg.ImportPath, denied.prefix, denied.reason)
			}
		}
	}
}
