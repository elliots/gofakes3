package s3afero

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/internal/s3io"
	"github.com/johannesboyne/gofakes3/internal/versionid"
	"github.com/spf13/afero"
)

// MultiBucketBackend is a gofakes3.Backend that allows you to create multiple
// buckets within the same afero.Fs. Buckets are stored under the `/buckets`
// subdirectory. Metadata is stored in the `/metadata` subdirectory by default,
// but any afero.Fs can be used.
//
// It is STRONGLY recommended that the metadata Fs is not contained within the
// `/buckets` subdirectory as that could make a significant mess, but this is
// infeasible to validate, so you're encouraged to be extremely careful!
type MultiBucketBackend struct {
	lock             sync.Mutex
	baseFs           afero.Fs
	bucketFs         afero.Fs
	metaStore        *metaStore
	dirMode          os.FileMode
	flags            FsFlags
	bucketVersioning map[string]gofakes3.VersioningStatus
	versionGenerator *versionid.VersionGenerator
	versionScratch   []byte
	timeSource       gofakes3.TimeSource

	// FIXME(bw): values in here should not be used beyond the configuration
	// step; maybe this can be cleaned up later using a builder struct or
	// something.
	configOnly struct {
		metaFs afero.Fs
	}
}

var _ gofakes3.Backend = &MultiBucketBackend{}
var _ gofakes3.VersionedBackend = &MultiBucketBackend{}

func MultiBucket(fs afero.Fs, opts ...MultiOption) (*MultiBucketBackend, error) {
	if err := ensureNoOsFs("fs", fs); err != nil {
		return nil, err
	}

	timeSource := gofakes3.DefaultTimeSource()

	b := &MultiBucketBackend{
		bucketVersioning: make(map[string]gofakes3.VersioningStatus),
		timeSource:       timeSource,
		versionGenerator: versionid.NewVersionGenerator(uint64(timeSource.Now().UnixNano()), 0),
	}

	for _, opt := range opts {
		if err := opt(b); err != nil {
			return nil, err
		}
	}

	bucketsFs, err := NewBasePathFs(fs, "buckets", FsPathCreateAll)
	if err != nil {
		return nil, err
	}

	b.baseFs = fs
	b.bucketFs = bucketsFs
	b.dirMode = 0700

	if b.configOnly.metaFs == nil {
		metaFs, err := NewBasePathFs(fs, "metadata", FsPathCreateAll)
		if err != nil {
			return nil, err
		}
		b.configOnly.metaFs = metaFs
	}
	b.metaStore = newMetaStore(b.configOnly.metaFs, modTimeFsCalc(fs))

	return b, nil
}

func (db *MultiBucketBackend) ListBuckets() ([]gofakes3.BucketInfo, error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	dirEntries, err := afero.ReadDir(db.bucketFs, "")
	if err != nil {
		return nil, err
	}

	var buckets = make([]gofakes3.BucketInfo, 0, len(dirEntries))
	for _, dirEntry := range dirEntries {
		if err := gofakes3.ValidateBucketName(dirEntry.Name()); err != nil {
			continue
		}

		buckets = append(buckets, gofakes3.BucketInfo{
			Name: dirEntry.Name(),

			// FIXME: "birth time" is not available cross-platform.
			// https://github.com/djherbis/times provides access to it on supported
			// platforms, but that wouldn't really be compatible with afero.
			// ModTime and some documented caveats might be the least-worst
			// option for this particular backend:
			CreationDate: gofakes3.NewContentTime(dirEntry.ModTime()),
		})
	}

	return buckets, nil
}

func (db *MultiBucketBackend) ListBucket(bucket string, prefix *gofakes3.Prefix, page gofakes3.ListBucketPage) (*gofakes3.ObjectList, error) {
	if prefix == nil {
		prefix = emptyPrefix
	}
	if err := gofakes3.ValidateBucketName(bucket); err != nil {
		return nil, gofakes3.BucketNotFound(bucket)
	}
	if !page.IsEmpty() {
		return nil, gofakes3.ErrInternalPageNotImplemented
	}

	db.lock.Lock()
	defer db.lock.Unlock()

	path, part, ok := prefix.FilePrefix()
	if ok {
		return db.getBucketWithFilePrefixLocked(bucket, path, part)
	} else {
		return db.getBucketWithArbitraryPrefixLocked(bucket, prefix)
	}
}

func (db *MultiBucketBackend) getBucketWithFilePrefixLocked(bucket string, prefixPath, prefixPart string) (*gofakes3.ObjectList, error) {
	bucketPath := path.Join(bucket, prefixPath)

	dirEntries, err := afero.ReadDir(db.bucketFs, filepath.FromSlash(bucketPath))
	if os.IsNotExist(err) {
		return nil, gofakes3.BucketNotFound(bucket)
	} else if err != nil {
		return nil, err
	}

	response := gofakes3.NewObjectList()

	for _, entry := range dirEntries {
		object := entry.Name()

		// Expected use of 'path'; see the "Path Handling" subheading in doc.go:
		objectPath := path.Join(prefixPath, object)

		if prefixPart != "" && !strings.HasPrefix(object, prefixPart) {
			continue
		}

		if entry.IsDir() {
			response.AddPrefix(path.Join(prefixPath, prefixPart, entry.Name()) + "/")

		} else {
			size := entry.Size()
			mtime := entry.ModTime()

			meta, err := db.metaStore.loadMeta(bucket, objectPath, size, mtime)
			if err != nil {
				return nil, err
			}

			response.Add(&gofakes3.Content{
				Key:          objectPath,
				LastModified: gofakes3.NewContentTime(mtime),
				ETag:         `"` + hex.EncodeToString(meta.Hash) + `"`,
				Size:         size,
			})
		}
	}

	return response, nil
}

func (db *MultiBucketBackend) getBucketWithArbitraryPrefixLocked(bucket string, prefix *gofakes3.Prefix) (*gofakes3.ObjectList, error) {
	stat, err := db.bucketFs.Stat(filepath.FromSlash(bucket))
	if os.IsNotExist(err) {
		return nil, gofakes3.BucketNotFound(bucket)
	} else if err != nil {
		return nil, err
	} else if !stat.IsDir() {
		return nil, fmt.Errorf("gofakes3: expected %q to be a bucket path", bucket)
	}

	response := gofakes3.NewObjectList()

	if err := afero.Walk(db.bucketFs, filepath.FromSlash(bucket), func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}

		objectPath := filepath.ToSlash(path)
		parts := strings.SplitN(objectPath, "/", 2)
		if len(parts) != 2 {
			panic(fmt.Errorf("unexpected path %q", path)) // should never happen
		}
		objectName := parts[1]

		if !prefix.Match(objectName, nil) {
			return nil
		}

		size := info.Size()
		mtime := info.ModTime()
		meta, err := db.metaStore.loadMeta(bucket, objectName, size, mtime)
		if err != nil {
			return err
		}

		response.Add(&gofakes3.Content{
			Key:          objectName,
			LastModified: gofakes3.NewContentTime(mtime),
			ETag:         `"` + hex.EncodeToString(meta.Hash) + `"`,
			Size:         size,
		})

		return nil

	}); err != nil {
		return nil, err
	}

	return response, nil
}

func (db *MultiBucketBackend) CreateBucket(name string) error {
	db.lock.Lock()
	defer db.lock.Unlock()

	if _, err := db.bucketFs.Stat(name); os.IsNotExist(err) {
		if err := db.bucketFs.MkdirAll(name, db.dirMode); err != nil {
			return err
		}
		return nil
	} else if err != nil {
		return err
	} else {
		return gofakes3.ResourceError(gofakes3.ErrBucketAlreadyExists, name)
	}
}

func (db *MultiBucketBackend) DeleteBucket(name string) (rerr error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	entries, err := afero.ReadDir(db.bucketFs, name)
	if err != nil {
		return err
	}

	if len(entries) > 0 {
		// This check is slightly racy. If another service outside gofakes3
		// changes the filesystem between this check and the call to Remove,
		// the bucket may be deleted even though there are items in it. You
		// would expect that afero.Fs would raise an error if you tried to
		// delete a directory that had stuff in it, but implementers of
		// afero.Fs may not implement that particular constraint. We have no
		// choice but to fall back on the db's lock and assume that a race
		// won't happen.
		return gofakes3.ResourceError(gofakes3.ErrBucketNotEmpty, name)
	}

	// FIXME(bw): the error handling logic here is a little janky:
	if err := db.bucketFs.RemoveAll(name); os.IsNotExist(err) {
		rerr = gofakes3.BucketNotFound(name)
	} else if err != nil {
		return err
	}

	if err := db.metaStore.deleteBucket(name); err != nil {
		return err
	}

	return rerr
}

func (db *MultiBucketBackend) ForceDeleteBucket(name string) error {
	db.lock.Lock()
	defer db.lock.Unlock()

	// Delete all objects in the bucket
	entries, err := afero.ReadDir(db.bucketFs, name)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		fullPath := path.Join(name, entry.Name())
		if err := db.bucketFs.RemoveAll(fullPath); err != nil {
			return err
		}
	}

	// Delete the bucket itself
	if err := db.bucketFs.RemoveAll(name); err != nil {
		return err
	}

	// Delete bucket metadata
	if err := db.metaStore.deleteBucket(name); err != nil {
		return err
	}

	return nil
}

func (db *MultiBucketBackend) BucketExists(name string) (exists bool, err error) {
	db.lock.Lock()
	defer db.lock.Unlock()
	exists, err = afero.Exists(db.bucketFs, name)
	return
}

// nextVersion assumes the backend's lock is acquired
func (db *MultiBucketBackend) nextVersion() gofakes3.VersionID {
	v, scr := db.versionGenerator.Next(db.versionScratch)
	db.versionScratch = scr
	return v
}

// VersioningConfiguration returns the versioning configuration for the bucket
func (db *MultiBucketBackend) VersioningConfiguration(bucket string) (versioning gofakes3.VersioningConfiguration, rerr error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	exists, err := afero.Exists(db.bucketFs, bucket)
	if err != nil {
		return versioning, err
	} else if !exists {
		return versioning, gofakes3.BucketNotFound(bucket)
	}

	versioning.Status = db.bucketVersioning[bucket]
	return versioning, nil
}

// SetVersioningConfiguration sets the versioning configuration for the bucket
func (db *MultiBucketBackend) SetVersioningConfiguration(bucket string, v gofakes3.VersioningConfiguration) error {
	if v.MFADelete.Enabled() {
		return gofakes3.ErrNotImplemented
	}

	db.lock.Lock()
	defer db.lock.Unlock()

	exists, err := afero.Exists(db.bucketFs, bucket)
	if err != nil {
		return err
	} else if !exists {
		return gofakes3.BucketNotFound(bucket)
	}

	currentStatus := db.bucketVersioning[bucket]

	if v.Enabled() {
		db.bucketVersioning[bucket] = gofakes3.VersioningEnabled
	} else if currentStatus == gofakes3.VersioningEnabled {
		db.bucketVersioning[bucket] = gofakes3.VersioningSuspended
	}

	return nil
}

// GetObjectVersion gets a specific version of an object
func (db *MultiBucketBackend) GetObjectVersion(bucketName, objectName string, versionID gofakes3.VersionID, rangeRequest *gofakes3.ObjectRangeRequest) (*gofakes3.Object, error) {

	if versionID == "" {
		return db.GetObject(bucketName, objectName, rangeRequest)
	}

	db.lock.Lock()
	defer db.lock.Unlock()

	exists, err := afero.Exists(db.bucketFs, bucketName)
	if err != nil {
		return nil, err
	} else if !exists {
		return nil, gofakes3.BucketNotFound(bucketName)
	}

	meta, err := db.metaStore.loadVersionMeta(bucketName, objectName, string(versionID))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, gofakes3.ErrNoSuchVersion
		}
		return nil, err
	}

	if meta.DeleteMarker {
		return nil, gofakes3.KeyNotFound(objectName)
	}

	// Build version-specific object path
	// (we'll use a directory structure with versions in a .versions/ subdirectory)
	versionPath := path.Join(bucketName, ".versions", objectName, safeVersionID(string(versionID)))
	versionPath = filepath.FromSlash(versionPath)

	f, err := db.bucketFs.Open(versionPath)
	if os.IsNotExist(err) {
		return nil, gofakes3.KeyNotFound(objectName)
	} else if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			f.Close()
		}
	}()

	stat, err := f.Stat()
	if err != nil {
		return nil, err
	} else if stat.IsDir() {
		return nil, gofakes3.KeyNotFound(objectName)
	}

	var rdr io.ReadCloser = f
	rnge, err := rangeRequest.Range(meta.Size)
	if err != nil {
		return nil, err
	}

	if rnge != nil {
		if _, err := f.Seek(rnge.Start, io.SeekStart); err != nil {
			return nil, err
		}
		rdr = limitReadCloser(rdr, f.Close, rnge.Length)
	}

	return &gofakes3.Object{
		Name:      objectName,
		Hash:      meta.Hash,
		Metadata:  meta.Meta,
		Size:      meta.Size,
		Range:     rnge,
		Contents:  rdr,
		VersionID: gofakes3.VersionID(meta.VersionID),
	}, nil
}

// HeadObjectVersion fetches object metadata for a specific version
func (db *MultiBucketBackend) HeadObjectVersion(bucketName, objectName string, versionID gofakes3.VersionID) (*gofakes3.Object, error) {
	if versionID == "" {
		return db.HeadObject(bucketName, objectName)
	}

	db.lock.Lock()
	defer db.lock.Unlock()

	exists, err := afero.Exists(db.bucketFs, bucketName)
	if err != nil {
		return nil, err
	} else if !exists {
		return nil, gofakes3.BucketNotFound(bucketName)
	}

	meta, err := db.metaStore.loadVersionMeta(bucketName, objectName, string(versionID))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, gofakes3.ErrNoSuchVersion
		}
		return nil, err
	}

	if meta.DeleteMarker {
		return nil, gofakes3.KeyNotFound(objectName)
	}

	return &gofakes3.Object{
		Name:      objectName,
		Hash:      meta.Hash,
		Metadata:  meta.Meta,
		Size:      meta.Size,
		Contents:  s3io.NoOpReadCloser{},
		VersionID: gofakes3.VersionID(meta.VersionID),
	}, nil
}

// DeleteObjectVersion permanently deletes a specific version of an object
func (db *MultiBucketBackend) DeleteObjectVersion(bucketName, objectName string, versionID gofakes3.VersionID) (result gofakes3.ObjectDeleteResult, rerr error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	exists, err := afero.Exists(db.bucketFs, bucketName)
	if err != nil {
		return result, err
	} else if !exists {
		return result, gofakes3.BucketNotFound(bucketName)
	}

	meta, err := db.metaStore.loadVersionMeta(bucketName, objectName, string(versionID))
	if err != nil {
		if os.IsNotExist(err) {
			// S300002 and S300003: If object or version doesn't exist, return empty result
			return result, nil
		}
		return result, err
	}

	// Delete version file if it exists
	versionPath := path.Join(bucketName, ".versions", objectName, safeVersionID(string(versionID)))
	versionPath = filepath.FromSlash(versionPath)

	if err := db.bucketFs.Remove(versionPath); err != nil && !os.IsNotExist(err) {
		return result, err
	}

	// Delete version metadata
	metaPath := db.metaStore.versionMetaPath(bucketName, objectName, string(versionID))
	if err := db.metaStore.deleteMeta(metaPath); err != nil {
		return result, err
	}

	result.VersionID = versionID
	result.IsDeleteMarker = meta.DeleteMarker

	// if this was the current version, delete that too
	currentMeta, err := db.metaStore.loadMeta(bucketName, objectName, meta.Size, meta.ModTime)
	if err != nil && !os.IsNotExist(err) {
		return result, err
	}
	if meta.VersionID == currentMeta.VersionID {
		fullPath := path.Join(bucketName, objectName)

		// S3 does not report an error when attemping to delete a key that does not exist, so
		// we need to skip IsNotExist errors.
		if err := db.bucketFs.Remove(filepath.FromSlash(fullPath)); err != nil && !os.IsNotExist(err) {
			return result, err
		}

		// delete the meta
		if err := db.metaStore.deleteMeta(db.metaStore.metaPath(bucketName, objectName)); err != nil {
			return result, err
		}

		// save the latest remaining version (if any exist) as the current one
		allVersions, err := db.metaStore.listVersions(bucketName, objectName)
		if err != nil {
			return result, err
		}
		if len(allVersions) > 0 {
			// convert to slice of Metadata

			sort.Slice(allVersions, func(i, j int) bool {
				return allVersions[i].ModTime.After(allVersions[j].ModTime)
			})

			latest := allVersions[len(allVersions)-1]
			if err := db.metaStore.saveMeta(db.metaStore.metaPath(bucketName, objectName), latest); err != nil {
				return result, err
			}
		}

	}

	return result, nil
}

// DeleteMultiVersions permanently deletes all specified object versions
func (db *MultiBucketBackend) DeleteMultiVersions(bucketName string, objects ...gofakes3.ObjectID) (result gofakes3.MultiDeleteResult, err error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	exists, err := afero.Exists(db.bucketFs, bucketName)
	if err != nil {
		return result, err
	} else if !exists {
		return result, gofakes3.BucketNotFound(bucketName)
	}

	for _, object := range objects {
		//var dresult gofakes3.ObjectDeleteResult
		var err error

		if object.VersionID != "" {
			_, err = db.DeleteObjectVersion(bucketName, object.Key, gofakes3.VersionID(object.VersionID))
		} else {
			_, err = db.DeleteObject(bucketName, object.Key)
		}

		if err != nil {
			errres := gofakes3.ErrorResultFromError(err)
			if errres.Code == gofakes3.ErrInternal {
				// FIXME: log
			}

			result.Error = append(result.Error, errres)
		} else {
			result.Deleted = append(result.Deleted, object)
		}
	}

	return result, nil
}

// ListBucketVersions lists all versions of objects in a bucket
func (db *MultiBucketBackend) ListBucketVersions(bucketName string, prefix *gofakes3.Prefix, page *gofakes3.ListBucketVersionsPage) (*gofakes3.ListBucketVersionsResult, error) {
	if prefix == nil {
		prefix = emptyPrefix
	}
	if page == nil {
		page = emptyVersionsPage
	}

	db.lock.Lock()
	defer db.lock.Unlock()

	exists, err := afero.Exists(db.bucketFs, bucketName)
	if err != nil {
		return nil, err
	} else if !exists {
		return nil, gofakes3.BucketNotFound(bucketName)
	}

	result := gofakes3.NewListBucketVersionsResult(bucketName, prefix, page)
	bucketPath := filepath.FromSlash(bucketName)

	// Get all files with matching prefix
	err = afero.Walk(db.bucketFs, bucketPath, func(filePath string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}

		// Convert Windows paths to forward slashes to match S3 key style
		relPath, err := filepath.Rel(bucketPath, filePath)
		if err != nil {
			return err
		}

		objectName := filepath.ToSlash(relPath)

		// Skip version files that are stored in .versions directory
		if strings.HasPrefix(objectName, ".versions/") {
			return nil
		}

		if !prefix.Match(objectName, nil) {
			return nil
		}

		// Get the current object metadata
		currentMeta, err := db.metaStore.loadMeta(bucketName, objectName, info.Size(), info.ModTime())
		if err != nil {
			return err
		}

		// Add current version
		isLatest := true
		bucketVersioning := db.bucketVersioning[bucketName]

		if bucketVersioning != gofakes3.VersioningNone {
			if currentMeta.DeleteMarker {
				marker := &gofakes3.DeleteMarker{
					Key:          objectName,
					IsLatest:     isLatest,
					LastModified: gofakes3.NewContentTime(currentMeta.ModTime),
					VersionID:    gofakes3.VersionID(currentMeta.VersionID),
				}
				result.Versions = append(result.Versions, marker)
			} else {
				resultVer := &gofakes3.Version{
					Key:          objectName,
					IsLatest:     isLatest,
					LastModified: gofakes3.NewContentTime(currentMeta.ModTime),
					Size:         currentMeta.Size,
					ETag:         `"` + hex.EncodeToString(currentMeta.Hash) + `"`,
					VersionID:    gofakes3.VersionID(currentMeta.VersionID),
				}
				result.Versions = append(result.Versions, resultVer)
			}
		} else {
			// S300005: Never versioned bucket returns current version with "null" version ID
			resultVer := &gofakes3.Version{
				Key:          objectName,
				IsLatest:     true,
				LastModified: gofakes3.NewContentTime(currentMeta.ModTime),
				Size:         currentMeta.Size,
				ETag:         `"` + hex.EncodeToString(currentMeta.Hash) + `"`,
				VersionID:    "null",
			}
			result.Versions = append(result.Versions, resultVer)
		}

		// Get previous versions
		versions, err := db.metaStore.listVersions(bucketName, objectName)
		if err != nil {
			return err
		}

		for _, ver := range versions {
			if ver.VersionID == currentMeta.VersionID {
				continue
			}
			if ver.DeleteMarker {
				marker := &gofakes3.DeleteMarker{
					Key:          objectName,
					IsLatest:     false,
					LastModified: gofakes3.NewContentTime(ver.ModTime),
					VersionID:    gofakes3.VersionID(ver.VersionID),
				}
				result.Versions = append(result.Versions, marker)
			} else {
				resultVer := &gofakes3.Version{
					Key:          objectName,
					IsLatest:     false,
					LastModified: gofakes3.NewContentTime(ver.ModTime),
					Size:         ver.Size,
					ETag:         `"` + hex.EncodeToString(ver.Hash) + `"`,
					VersionID:    gofakes3.VersionID(ver.VersionID),
				}
				result.Versions = append(result.Versions, resultVer)
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	// TODO: Implement paging

	return result, nil
}

func (db *MultiBucketBackend) HeadObject(bucketName, objectName string) (*gofakes3.Object, error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	// Another slighly racy check:
	exists, err := afero.Exists(db.bucketFs, bucketName)
	if err != nil {
		return nil, err
	} else if !exists {
		return nil, gofakes3.BucketNotFound(bucketName)
	}

	fullPath := path.Join(bucketName, objectName)

	stat, err := db.bucketFs.Stat(filepath.FromSlash(fullPath))
	if os.IsNotExist(err) {
		return nil, gofakes3.KeyNotFound(objectName)
	} else if err != nil {
		return nil, err
	} else if stat.IsDir() {
		return nil, gofakes3.KeyNotFound(objectName)
	}

	size, mtime := stat.Size(), stat.ModTime()

	meta, err := db.metaStore.loadMeta(bucketName, objectName, size, mtime)
	if err != nil {
		return nil, err
	}

	// If this is a delete marker and not a real object, return a "not found" error
	if meta.DeleteMarker {
		return nil, gofakes3.KeyNotFound(objectName)
	}

	result := &gofakes3.Object{
		Name:     objectName,
		Hash:     meta.Hash,
		Metadata: meta.Meta,
		Size:     size,
		Contents: s3io.NoOpReadCloser{},
	}

	// Only include version ID if versioning is enabled
	bucketVersioning := db.bucketVersioning[bucketName]
	if bucketVersioning == gofakes3.VersioningEnabled && meta.VersionID != "" {
		result.VersionID = gofakes3.VersionID(meta.VersionID)
	}

	return result, nil
}

func (db *MultiBucketBackend) GetObject(bucketName, objectName string, rangeRequest *gofakes3.ObjectRangeRequest) (obj *gofakes3.Object, rerr error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	// Another slighly racy check:
	exists, err := afero.Exists(db.bucketFs, bucketName)
	if err != nil {
		return nil, err
	} else if !exists {
		return nil, gofakes3.BucketNotFound(bucketName)
	}

	fullPath := path.Join(bucketName, objectName)

	f, err := db.bucketFs.Open(filepath.FromSlash(fullPath))
	if os.IsNotExist(err) {
		return nil, gofakes3.KeyNotFound(objectName)
	} else if err != nil {
		return nil, err
	}
	defer func() {
		// If an error occurs, the caller may not have access to Object.Body in order to close it:
		if obj == nil && rerr != nil {
			f.Close()
		}
	}()

	stat, err := f.Stat()
	if err != nil {
		return nil, err
	} else if stat.IsDir() {
		return nil, gofakes3.KeyNotFound(objectName)
	}

	size, mtime := stat.Size(), stat.ModTime()

	var rdr io.ReadCloser = f
	rnge, err := rangeRequest.Range(size)
	if err != nil {
		return nil, err
	}

	if rnge != nil {
		if _, err := f.Seek(rnge.Start, io.SeekStart); err != nil {
			return nil, err
		}
		rdr = limitReadCloser(rdr, f.Close, rnge.Length)
	}

	meta, err := db.metaStore.loadMeta(bucketName, objectName, size, mtime)
	if err != nil {
		return nil, err
	}

	// If this is a delete marker and not a real object, return a "not found" error
	if meta.DeleteMarker {
		return nil, gofakes3.KeyNotFound(objectName)
	}

	result := &gofakes3.Object{
		Name:     objectName,
		Hash:     meta.Hash,
		Metadata: meta.Meta,
		Range:    rnge,
		Size:     size,
		Contents: rdr,
	}

	// Only include version ID if versioning is enabled
	bucketVersioning := db.bucketVersioning[bucketName]
	if bucketVersioning == gofakes3.VersioningEnabled && meta.VersionID != "" {
		result.VersionID = gofakes3.VersionID(meta.VersionID)
	}

	return result, nil
}

func (db *MultiBucketBackend) PutObject(
	bucketName, objectName string,
	meta map[string]string,
	input io.Reader, size int64,
) (result gofakes3.PutObjectResult, err error) {

	err = gofakes3.MergeMetadata(db, bucketName, objectName, meta)
	if err != nil {
		return result, err
	}

	db.lock.Lock()
	defer db.lock.Unlock()

	// Another slighly racy check:
	exists, err := afero.Exists(db.bucketFs, bucketName)
	if err != nil {
		return result, err
	} else if !exists {
		return result, gofakes3.BucketNotFound(bucketName)
	}

	// Generate a version ID if versioning is enabled
	var versionID gofakes3.VersionID
	bucketVersioning := db.bucketVersioning[bucketName]
	if bucketVersioning == gofakes3.VersioningEnabled {
		versionID = db.nextVersion()
		result.VersionID = versionID
	}

	objectPath := path.Join(bucketName, objectName)
	objectFilePath := filepath.FromSlash(objectPath)
	objectDir := filepath.Dir(objectFilePath)

	if objectDir != "." {
		if err := db.bucketFs.MkdirAll(objectDir, db.dirMode); err != nil {
			return result, err
		}
	}

	f, err := db.bucketFs.Create(objectFilePath)
	if err != nil {
		return result, err
	}

	var closed bool
	defer func() {
		// Unfortunately, afero's MemMapFs updates the mtime if you double-close, which
		// highlights that other afero.Fs implementations may have side effects here::
		if !closed {
			f.Close()
		}
	}()
	hasher := md5.New()

	var writers []io.Writer = []io.Writer{f, hasher}

	if bucketVersioning == gofakes3.VersioningEnabled {

		// Save the file to a version file
		versionDir := path.Join(bucketName, ".versions", objectName)
		versionPath := path.Join(versionDir, safeVersionID(string(versionID)))
		versionFilePath := filepath.FromSlash(versionPath)

		// Create directory for versions if it doesn't exist
		if err := db.bucketFs.MkdirAll(path.Dir(versionFilePath), db.dirMode); err != nil {
			return result, err
		}

		versionF, err := db.bucketFs.Create(versionFilePath)
		if err != nil {
			return result, err
		}
		var closed bool
		defer func() {
			// Unfortunately, afero's MemMapFs updates the mtime if you double-close, which
			// highlights that other afero.Fs implementations may have side effects here::
			if !closed {
				versionF.Close()
			}
		}()

		writers = append(writers, versionF)
	}

	w := io.MultiWriter(writers...)
	if _, err := io.Copy(w, input); err != nil {
		return result, err
	}

	// We have to close here before we stat the file as some filesystems don't update the
	// mtime until after close:
	if err := f.Close(); err != nil {
		return result, err
	}
	closed = true

	stat, err := db.bucketFs.Stat(objectFilePath)
	if err != nil {
		return result, err
	}

	storedMeta := &Metadata{
		File:      objectPath,
		Hash:      hasher.Sum(nil),
		Meta:      meta,
		Size:      stat.Size(),
		ModTime:   stat.ModTime(),
		VersionID: string(versionID),
	}
	if err := db.metaStore.saveMeta(db.metaStore.metaPath(bucketName, objectName), storedMeta); err != nil {
		return result, err
	}

	if bucketVersioning == gofakes3.VersioningEnabled {
		// Save metadata for version
		if err := db.metaStore.saveVersionMeta(bucketName, objectName, storedMeta); err != nil {
			return result, err
		}
	}

	return result, nil
}

func (db *MultiBucketBackend) CopyObject(srcBucket, srcKey, dstBucket, dstKey string, meta map[string]string) (result gofakes3.CopyObjectResult, err error) {
	return gofakes3.CopyObject(db, srcBucket, srcKey, dstBucket, dstKey, meta)
}

func (db *MultiBucketBackend) DeleteObject(bucketName, objectName string) (result gofakes3.ObjectDeleteResult, rerr error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	// Another slighly racy check:
	exists, err := afero.Exists(db.bucketFs, bucketName)
	if err != nil {
		return result, err
	} else if !exists {
		return result, gofakes3.BucketNotFound(bucketName)
	}

	// If versioning is enabled, create a delete marker instead of actually deleting
	bucketVersioning := db.bucketVersioning[bucketName]
	if bucketVersioning == gofakes3.VersioningEnabled {

		// Create delete marker
		versionID := db.nextVersion()
		result.VersionID = versionID
		result.IsDeleteMarker = true

		// Create a delete marker metadata
		now := db.timeSource.Now()
		deleteMarkerMeta := &Metadata{
			File:         objectName,
			ModTime:      now,
			VersionID:    string(versionID),
			DeleteMarker: true,
		}

		if err := db.metaStore.saveMeta(db.metaStore.metaPath(bucketName, objectName), deleteMarkerMeta); err != nil {
			return result, err
		}

		// Save metadata for version
		if err := db.metaStore.saveVersionMeta(bucketName, objectName, deleteMarkerMeta); err != nil {
			return result, err
		}

		return result, nil
	}

	// If versioning is not enabled, delete the object normally
	err = db.deleteObjectLocked(bucketName, objectName)
	return result, err
}

func (db *MultiBucketBackend) deleteObjectLocked(bucketName, objectName string) error {
	fullPath := path.Join(bucketName, objectName)

	// S3 does not report an error when attemping to delete a key that does not exist, so
	// we need to skip IsNotExist errors.
	if err := db.bucketFs.Remove(filepath.FromSlash(fullPath)); err != nil && !os.IsNotExist(err) {
		return err
	}

	if err := db.metaStore.deleteMeta(db.metaStore.metaPath(bucketName, objectName)); err != nil {
		return err
	}

	return nil
}

func (db *MultiBucketBackend) DeleteMulti(bucketName string, objects ...string) (result gofakes3.MultiDeleteResult, rerr error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	// Another slighly racy check:
	exists, err := afero.Exists(db.bucketFs, bucketName)
	if err != nil {
		return result, err
	} else if !exists {
		return result, gofakes3.BucketNotFound(bucketName)
	}

	for _, object := range objects {
		if err := db.deleteObjectLocked(bucketName, object); err != nil {
			log.Println("delete object failed:", err)
			result.Error = append(result.Error, gofakes3.ErrorResult{
				Code:    gofakes3.ErrInternal,
				Message: gofakes3.ErrInternal.Message(),
				Key:     object,
			})
		} else {
			result.Deleted = append(result.Deleted, gofakes3.ObjectID{
				Key: object,
			})
		}
	}

	return result, nil
}
