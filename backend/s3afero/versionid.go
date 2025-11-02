package s3afero

import (
	"crypto/rand"
	"fmt"
	"sync"

	"github.com/johannesboyne/gofakes3"
)

type versionGenerator struct {
	counter uint64
	mu      sync.Mutex
}

func newVersionGenerator(seed uint64, _ int) *versionGenerator {
	return &versionGenerator{counter: 0}
}

// Next generates a version ID in the format: {incrementing_number}-{6_random_hex_chars}
// Example: "1-a3f2c9", "2-7b8d4e", "3-1c5e9f"
func (v *versionGenerator) Next(_ []byte) (gofakes3.VersionID, []byte) {
	v.mu.Lock()
	defer v.mu.Unlock()

	v.counter++

	// Generate 3 random bytes (6 hex characters)
	randomBytes := make([]byte, 3)
	if _, err := rand.Read(randomBytes); err != nil {
		// Fallback to a predictable value if random fails
		randomBytes = []byte{0xab, 0xcd, 0xef}
	}

	versionID := fmt.Sprintf("%d-%02x%02x%02x", v.counter, randomBytes[0], randomBytes[1], randomBytes[2])
	return gofakes3.VersionID(versionID), nil
}
