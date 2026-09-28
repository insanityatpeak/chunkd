package sim

import (
	"math/rand/v2"

	"github.com/insanityatpeak/chunkd/internal/iface"
)

// NewRand returns a PCG generator fully determined by seed.
func NewRand(seed uint64) iface.Rand {
	return rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
}

// chance reports true with probability p, consuming one value from r.
func chance(r iface.Rand, p float64) bool {
	return float64(r.Uint64()>>11)/(1<<53) < p
}
