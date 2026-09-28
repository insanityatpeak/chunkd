package main

import (
	"go/parser"
	"go/token"
	"strings"
	"testing"
)

func TestCheckFile(t *testing.T) {
	tests := []struct {
		name string
		src  string
		want []string // substrings, one per expected violation
	}{
		{"duration only", `package p
import "time"
var d time.Duration = 3 * time.Second`, nil},
		{"now", `package p
import "time"
var n = time.Now()`, []string{"time.Now"}},
		{"renamed time", `package p
import tm "time"
var _ = tm.Sleep`, []string{"time.Sleep"}},
		{"dot import", `package p
import . "time"`, []string{"dot-import"}},
		{"net", `package p
import "net"`, []string{`"net"`}},
		{"net subpackage", `package p
import "net/http"`, []string{`"net/http"`}},
		{"os", `package p
import "os"`, []string{`"os"`}},
		{"math rand v2", `package p
import "math/rand/v2"`, []string{`"math/rand/v2"`}},
		{"crypto rand", `package p
import "crypto/rand"`, []string{`"crypto/rand"`}},
		{"allowed neighbours", `package p
import (
	"crypto/sha256"
	"math/bits"
	"context"
)`, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fset := token.NewFileSet()
			f, err := parser.ParseFile(fset, "x.go", tt.src, 0)
			if err != nil {
				t.Fatal(err)
			}
			got := checkFile(fset, f)
			if len(got) != len(tt.want) {
				t.Fatalf("got %d violations %v, want %d", len(got), got, len(tt.want))
			}
			for i, w := range tt.want {
				if !strings.Contains(got[i], w) {
					t.Errorf("violation %q does not mention %q", got[i], w)
				}
			}
		})
	}
}

func TestCheckImportsMissingRoot(t *testing.T) {
	v, n, err := checkImports(t.TempDir() + "/does-not-exist")
	if err != nil || n != 0 || len(v) != 0 {
		t.Fatalf("got %v, %d, %v; want no violations, 0 files, nil", v, n, err)
	}
}
