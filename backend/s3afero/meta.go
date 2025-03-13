package s3afero

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/afero"
)

type Metadata struct {
	File         string
	ModTime      time.Time
	Size         int64
	Hash         []byte
	Meta         map[string]string
	VersionID    string
	DeleteMarker bool
}

type metaPath struct {
	bucket string
	object string
}

func (mp metaPath) FilePath() string {
	return filepath.Join(mp.bucket, mp.object)
}

type metaStore struct {
	fs          afero.Fs
	modTimeCalc modTimeCalc
	modTimeRes  time.Duration
}

func newMetaStore(fs afero.Fs, modTimeCalc modTimeCalc) *metaStore {
	b := &metaStore{
		fs:          fs,
		modTimeCalc: modTimeCalc,
		modTimeRes:  -1,
	}
	return b
}

func (ms *metaStore) getModTimeRes() (dur time.Duration, err error) {
	if ms.modTimeRes < 0 {
		ms.modTimeRes, err = ms.modTimeCalc()
		if err != nil {
			return -1, err
		}
	}
	return ms.modTimeRes, nil
}

func (ms *metaStore) metaPath(bucket string, object string) metaPath {
	// FIXME: may need to add path segments but that may be a thing of the past:
	// https://stackoverflow.com/questions/466521/how-many-files-can-i-put-in-a-directory
	h := fnv.New128a()
	h.Write([]byte(object))
	object = strings.Replace(object, "/", "_", -1)
	object = strings.Replace(object, "\\", "_", -1)

	return metaPath{bucket, object}
}

func (ms *metaStore) loadMeta(bucket string, object string, size int64, mtime time.Time) (*Metadata, error) {
	metaPath := ms.metaPath(bucket, object)
	fullPath := metaPath.FilePath()

	bts, err := afero.ReadFile(ms.fs, fullPath)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	var meta Metadata
	if len(bts) > 0 {
		if err := json.Unmarshal(bts, &meta); err != nil {
			return nil, err
		}
	}

	modRes, err := ms.getModTimeRes()
	if err != nil {
		return nil, err
	}

	modDiff := mtime.Sub(meta.ModTime)
	if len(meta.Hash) == 0 || meta.Size != size || modDiff < -modRes || modDiff > modRes {
		meta.Size = size
		meta.ModTime = mtime
		meta.Hash, err = hashFile(ms.fs, fullPath)
		if err != nil {
			return nil, err
		}
		if err := ms.saveMeta(metaPath, &meta); err != nil {
			return nil, err
		}
	}

	return &meta, nil
}

func (ms *metaStore) saveMeta(path metaPath, meta *Metadata) error {
	bts, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	if err := ms.fs.MkdirAll(filepath.Dir(path.FilePath()), 0777); err != nil {
		return err
	}

	return afero.WriteFile(ms.fs, path.FilePath(), bts, 0666)
}

func (ms *metaStore) deleteMeta(path metaPath) error {
	if err := ms.fs.Remove(path.FilePath()); os.IsNotExist(err) {
		return nil
	} else {
		return err
	}
}

func (ms *metaStore) deleteBucket(bucket string) error {
	if err := ms.fs.RemoveAll(bucket); os.IsNotExist(err) {
		return nil
	} else {
		return err
	}
}

// versionMetaPath returns a path for a specific version of an object
func (ms *metaStore) versionMetaPath(bucket string, object string, versionID string) metaPath {
	// Replace forward slashes in the versionID to avoid directory issues
	safeVersionID := safeVersionID(versionID)

	h := fnv.New128a()
	h.Write([]byte(object))
	object = strings.Replace(object, "/", "_", -1)
	object = strings.Replace(object, "\\", "_", -1)

	return metaPath{bucket, fmt.Sprintf("%s-version-%s", object, safeVersionID)}
}

func safeVersionID(versionID string) string {
	return strings.Replace(versionID, "/", "-", -1)
}

// saveVersionMeta saves metadata for a specific version of an object
func (ms *metaStore) saveVersionMeta(bucket string, object string, meta *Metadata) error {
	if meta.VersionID == "" {
		return fmt.Errorf("versionID is required for version metadata")
	}

	metaPath := ms.versionMetaPath(bucket, object, meta.VersionID)
	return ms.saveMeta(metaPath, meta)
}

// loadVersionMeta loads metadata for a specific version of an object
func (ms *metaStore) loadVersionMeta(bucket string, object string, versionID string) (*Metadata, error) {
	metaPath := ms.versionMetaPath(bucket, object, versionID)
	fullPath := metaPath.FilePath()

	bts, err := afero.ReadFile(ms.fs, fullPath)
	if err != nil {
		return nil, err
	}

	var meta Metadata
	if err := json.Unmarshal(bts, &meta); err != nil {
		return nil, err
	}

	return &meta, nil
}

// listVersions returns all versions of an object
func (ms *metaStore) listVersions(bucket string, object string) ([]*Metadata, error) {
	prefix := object
	prefix = strings.Replace(prefix, "/", "_", -1)
	prefix = strings.Replace(prefix, "\\", "_", -1)
	prefix += "-version-"

	var versions []*Metadata

	err := afero.Walk(ms.fs, bucket, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}

		fileName := strings.TrimPrefix(path, bucket+"/")
		if strings.HasPrefix(fileName, prefix) {
			bts, err := afero.ReadFile(ms.fs, path)
			if err != nil {
				return err
			}

			var meta Metadata
			if err := json.Unmarshal(bts, &meta); err != nil {
				return err
			}

			versions = append(versions, &meta)
		}

		return nil
	})

	return versions, err
}
