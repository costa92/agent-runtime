package agentruntime_test

import (
	"encoding/json"
	"io"
	"os/exec"
	"strings"
	"testing"
)

// Scan the complete production dependency closure, including packages added
// later. Provider transports, databases and host business code stay outside.
func TestProductionDependenciesAreStandardLibraryOnly(t *testing.T) {
	output, err := exec.Command("go", "list", "-deps", "-json", "./...").Output()
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(strings.NewReader(string(output)))
	count := 0
	for {
		var pkg struct {
			ImportPath string
			Standard   bool
			Module     *struct{ Path string }
		}
		if err := decoder.Decode(&pkg); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		if pkg.Standard {
			for _, denied := range []string{"net", "net/http", "database/sql", "os/exec", "testing"} {
				if pkg.ImportPath == denied || strings.HasPrefix(pkg.ImportPath, denied+"/") {
					t.Errorf("production dependency %s must stay in the host or tests", pkg.ImportPath)
				}
			}
			continue
		}
		if pkg.Module == nil || pkg.Module.Path != "github.com/costa92/agent-runtime" {
			t.Errorf("production dependency %s must stay in the host", pkg.ImportPath)
		}
		count++
	}
	if count < 7 {
		t.Fatalf("only %d Runtime packages scanned; dependency guard did not cover the module", count)
	}
}
