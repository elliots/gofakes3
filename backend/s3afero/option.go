package s3afero

import (
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/internal/versionid"
	"github.com/spf13/afero"
)

type MultiOption func(b *MultiBucketBackend) error

func MultiWithMetaFs(fs afero.Fs) MultiOption {
	return func(b *MultiBucketBackend) error {
		if err := ensureNoOsFs("MultiWithMetaFs", fs); err != nil {
			return err
		}
		b.configOnly.metaFs = fs
		return nil
	}
}

func MultiWithVersionSeed(seed int) MultiOption {
	return func(b *MultiBucketBackend) error {
		// Recreate version generator with fixed seed for testing
		b.versionGenerator = versionid.NewVersionGenerator(uint64(seed), 0)
		return nil
	}
}

func MultiFsFlags(flags FsFlags) MultiOption {
	return func(b *MultiBucketBackend) error {
		b.flags = flags
		return nil
	}
}

type SingleOption func(b *SingleBucketBackend) error

// Global variables for convenience
var emptyVersionsPage = &gofakes3.ListBucketVersionsPage{}
