package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// bannedImports are environment packages core code must reach only through
// internal/iface. A prefix match also bans subpackages (net/http, os/exec).
var bannedImports = []string{"net", "os", "syscall", "math/rand", "crypto/rand"}

// allowedTime lists the only time identifiers core may use: the Duration type
// and its unit constants. Anything else (Now, Sleep, Timer) bypasses the Clock.
var allowedTime = map[string]bool{
	"Duration": true, "Nanosecond": true, "Microsecond": true,
	"Millisecond": true, "Second": true, "Minute": true, "Hour": true,
}

func lintImports(root string) error {
	violations, n, err := checkImports(root)
	if err != nil {
		return err
	}
	for _, v := range violations {
		fmt.Println(v)
	}
	if len(violations) > 0 {
		return fmt.Errorf("lint-imports: %d violation(s) in %s", len(violations), root)
	}
	fmt.Printf("lint-imports: %d files under %s clean\n", n, root)
	return nil
}

// checkImports returns one "file:line: message" per violation and the number
// of Go files checked. A missing root is not an error.
func checkImports(root string) ([]string, int, error) {
	var out []string
	files := 0
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) && path == root {
				return filepath.SkipDir
			}
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") {
			return nil
		}
		files++
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if err != nil {
			return err
		}
		out = append(out, checkFile(fset, f)...)
		return nil
	})
	return out, files, err
}

func checkFile(fset *token.FileSet, f *ast.File) []string {
	var out []string
	report := func(p token.Pos, format string, args ...any) {
		out = append(out, fmt.Sprintf("%s: %s", fset.Position(p), fmt.Sprintf(format, args...)))
	}
	timeName := ""
	for _, imp := range f.Imports {
		path, _ := strconv.Unquote(imp.Path.Value)
		for _, b := range bannedImports {
			if path == b || strings.HasPrefix(path, b+"/") {
				report(imp.Pos(), "core must not import %q; use internal/iface", path)
			}
		}
		if path != "time" {
			continue
		}
		timeName = "time"
		if imp.Name != nil {
			timeName = imp.Name.Name
		}
		if timeName == "." {
			report(imp.Pos(), `dot-import of "time" hides clock access`)
		}
	}
	if timeName == "" || timeName == "." || timeName == "_" {
		return out
	}
	ast.Inspect(f, func(n ast.Node) bool {
		sel, ok := n.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if x, ok := sel.X.(*ast.Ident); ok && x.Name == timeName && !allowedTime[sel.Sel.Name] {
			report(sel.Pos(), "time.%s bypasses iface.Clock; only time.Duration and its units are allowed", sel.Sel.Name)
		}
		return true
	})
	return out
}
