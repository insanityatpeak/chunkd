// Package bench holds what the benchmarks share: CSV files, the hardware
// description and SVG charts. The measurements themselves live next to the
// code they measure (internal/sim/cluster, internal/core/meta) and in
// cmd/chunkd-bench.
package bench

import (
	"encoding/csv"
	"os"
	"path/filepath"
	"strconv"
)

// OutEnv names the directory sim benchmarks write CSVs to. Unset, they skip.
const OutEnv = "CHUNKD_BENCH_OUT"

// Table is a CSV file in memory.
type Table struct {
	Header []string
	Rows   [][]string
}

// Add appends a row; numbers keep a fixed precision so reruns diff cleanly.
func (t *Table) Add(vals ...any) {
	row := make([]string, len(vals))
	for i, v := range vals {
		switch x := v.(type) {
		case float64:
			row[i] = strconv.FormatFloat(x, 'f', 3, 64)
		case int:
			row[i] = strconv.Itoa(x)
		case int64:
			row[i] = strconv.FormatInt(x, 10)
		case string:
			row[i] = x
		default:
			panic("bench: unsupported cell type")
		}
	}
	t.Rows = append(t.Rows, row)
}

// WriteCSV writes t to path, creating the directory.
func (t *Table) WriteCSV(path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	w := csv.NewWriter(f)
	_ = w.Write(t.Header)
	_ = w.WriteAll(t.Rows)
	if err := w.Error(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// ReadCSV reads a file WriteCSV wrote.
func ReadCSV(path string) (*Table, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	recs, err := csv.NewReader(f).ReadAll()
	if err != nil {
		return nil, err
	}
	if len(recs) == 0 {
		return &Table{}, nil
	}
	return &Table{Header: recs[0], Rows: recs[1:]}, nil
}

// Col returns column name as floats; cells that do not parse are 0.
func (t *Table) Col(name string) []float64 {
	idx := -1
	for i, h := range t.Header {
		if h == name {
			idx = i
		}
	}
	out := make([]float64, len(t.Rows))
	if idx < 0 {
		return out
	}
	for i, r := range t.Rows {
		out[i], _ = strconv.ParseFloat(r[idx], 64)
	}
	return out
}

// Str returns column name as strings.
func (t *Table) Str(name string) []string {
	out := make([]string, len(t.Rows))
	for i, h := range t.Header {
		if h != name {
			continue
		}
		for j, r := range t.Rows {
			out[j] = r[i]
		}
	}
	return out
}

// OutDir returns the directory sim benchmarks write to, or "".
func OutDir() string { return os.Getenv(OutEnv) }
