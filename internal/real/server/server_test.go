package server

import (
	"testing"

	"github.com/insanityatpeak/chunkd/internal/iface"
)

func TestParsePeers(t *testing.T) {
	tests := []struct {
		in      string
		want    map[iface.NodeID]string
		wantErr bool
	}{
		{"", map[iface.NodeID]string{}, false},
		{"meta-1=meta-1:7000", map[iface.NodeID]string{"meta-1": "meta-1:7000"}, false},
		{" a=h:1 , b=h:2 ,", map[iface.NodeID]string{"a": "h:1", "b": "h:2"}, false},
		{"a", nil, true},
		{"=h:1", nil, true},
		{"a=", nil, true},
	}
	for _, tt := range tests {
		got, err := ParsePeers(tt.in)
		if (err != nil) != tt.wantErr {
			t.Errorf("ParsePeers(%q) err = %v, wantErr %v", tt.in, err, tt.wantErr)
			continue
		}
		if len(got) != len(tt.want) {
			t.Errorf("ParsePeers(%q) = %v, want %v", tt.in, got, tt.want)
			continue
		}
		for k, v := range tt.want {
			if got[k] != v {
				t.Errorf("ParsePeers(%q)[%s] = %q, want %q", tt.in, k, got[k], v)
			}
		}
	}
}
