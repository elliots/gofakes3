package s3afero

import (
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/spf13/afero"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/internal/s3io"
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
	versionGenerator *versionGenerator
	versionScratch   []byte
	timeSource       gofakes3.TimeSource

	// versioning stores the versioning status per bucket
	versioning map[string]gofakes3.VersioningStatus

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

	b := &MultiBucketBackend{}
	for _, opt := range opts {
		if err := opt(b); err != nil {
			return nil, err
		}
	}

	bucketsFs, err := NewBasePathFs(fs, "buckets", FsPathCreateAll)
	if err != nil {
		return nil, err
	}

	timeSource := gofakes3.DefaultTimeSource()

	b.baseFs = fs
	b.bucketFs = bucketsFs
	b.dirMode = 0700
	b.versioning = make(map[string]gofakes3.VersioningStatus)
	b.versionGenerator = newVersionGenerator(uint64(timeSource.Now().UnixNano()), 0)
	b.timeSource = timeSource

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

	db.lock.Lock()
	defer db.lock.Unlock()

	path, part, ok := prefix.FilePrefix()
	if ok {
		return db.getBucketWithFilePrefixLocked(bucket, path, part, page)
	} else {
		return db.getBucketWithArbitraryPrefixLocked(bucket, prefix, page)
	}
}

func (db *MultiBucketBackend) getBucketWithFilePrefixLocked(bucket string, prefixPath, prefixPart string, page gofakes3.ListBucketPage) (*gofakes3.ObjectList, error) {
	bucketPath := path.Join(bucket, prefixPath)

	dirEntries, err := afero.ReadDir(db.bucketFs, filepath.FromSlash(bucketPath))
	if os.IsNotExist(err) {
		// Prefix path doesn't exist - return empty list, not an error
		return gofakes3.NewObjectList(), nil
	} else if err != nil {
		return nil, err
	}

	response := gofakes3.NewObjectList()

	// Sort entries for consistent ordering
	sort.Slice(dirEntries, func(i, j int) bool {
		return dirEntries[i].Name() < dirEntries[j].Name()
	})

	var count int64
	var lastKey string
	passedMarker := !page.HasMarker

	for _, entry := range dirEntries {
		object := entry.Name()

		// Expected use of 'path'; see the "Path Handling" subheading in doc.go:
		objectPath := path.Join(prefixPath, object)

		if prefixPart != "" && !strings.HasPrefix(object, prefixPart) {
			continue
		}

		// Handle marker - skip until we pass it
		if !passedMarker {
			if objectPath > page.Marker {
				passedMarker = true
			} else {
				continue
			}
		}

		if entry.IsDir() {
			response.AddPrefix(path.Join(prefixPath, prefixPart, entry.Name()) + "/")
			lastKey = path.Join(prefixPath, prefixPart, entry.Name()) + "/"
		} else {
			size := entry.Size()
			mtime := entry.ModTime()

			meta, err := db.ensureMeta(bucket, objectPath, size, mtime)
			if err != nil {
				return nil, err
			}

			response.Add(&gofakes3.Content{
				Key:          objectPath,
				LastModified: gofakes3.NewContentTime(mtime),
				ETag:         gofakes3.FormatETag(meta.Hash),
				Size:         size,
			})
			lastKey = objectPath
		}

		count++
		if page.MaxKeys > 0 && count >= page.MaxKeys {
			// Check if there are more entries
			response.IsTruncated = (entry != dirEntries[len(dirEntries)-1])
			if response.IsTruncated {
				response.NextMarker = lastKey
			}
			break
		}
	}

	return response, nil
}

func (db *MultiBucketBackend) getBucketWithArbitraryPrefixLocked(bucket string, prefix *gofakes3.Prefix, page gofakes3.ListBucketPage) (*gofakes3.ObjectList, error) {
	stat, err := db.bucketFs.Stat(filepath.FromSlash(bucket))
	if os.IsNotExist(err) {
		return nil, gofakes3.BucketNotFound(bucket)
	} else if err != nil {
		return nil, err
	} else if !stat.IsDir() {
		return nil, fmt.Errorf("gofakes3: expected %q to be a bucket path", bucket)
	}

	// Collect all matching items first, then sort and paginate
	type item struct {
		key   string
		size  int64
		mtime time.Time
		hash  []byte
	}
	var items []item

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
		meta, err := db.ensureMeta(bucket, objectName, size, mtime)
		if err != nil {
			return err
		}

		items = append(items, item{
			key:   objectName,
			size:  size,
			mtime: mtime,
			hash:  meta.Hash,
		})

		return nil

	}); err != nil {
		return nil, err
	}

	// Sort items lexicographically
	sort.Slice(items, func(i, j int) bool {
		return items[i].key < items[j].key
	})

	response := gofakes3.NewObjectList()

	// Apply marker and pagination
	var count int64
	for i, item := range items {
		// Skip items until we pass the marker
		if page.HasMarker && item.key <= page.Marker {
			continue
		}

		response.Add(&gofakes3.Content{
			Key:          item.key,
			LastModified: gofakes3.NewContentTime(item.mtime),
			ETag:         gofakes3.FormatETag(item.hash),
			Size:         item.size,
		})

		count++
		if page.MaxKeys > 0 && count >= page.MaxKeys {
			// Check if there are more items
			if i < len(items)-1 {
				response.IsTruncated = true
				response.NextMarker = item.key
			}
			break
		}
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

	meta, err := db.ensureMeta(bucketName, objectName, size, mtime)
	if err != nil {
		return nil, err
	}

	return &gofakes3.Object{
		Name:         objectName,
		Hash:         meta.Hash,
		Metadata:     meta.Meta,
		Size:         size,
		LastModified: mtime,
		VersionID:    gofakes3.VersionID(meta.VersionID),
		Contents:     s3io.NoOpReadCloser{},
	}, nil
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

	meta, err := db.ensureMeta(bucketName, objectName, size, mtime)
	if err != nil {
		return nil, err
	}

	return &gofakes3.Object{
		Name:         objectName,
		Hash:         meta.Hash,
		Metadata:     meta.Meta,
		Range:        rnge,
		Size:         size,
		LastModified: mtime,
		VersionID:    gofakes3.VersionID(meta.VersionID),
		Contents:     rdr,
	}, nil
}

func (db *MultiBucketBackend) PutObject(
	bucketName, objectName string,
	meta map[string]string,
	input io.Reader,
	size int64,
	conditions *gofakes3.PutConditions,
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

	if conditions != nil {
		objectInfo, err := db.getConditionalObjectInfo(bucketName, objectName)
		if err != nil {
			return result, err
		}
		if err := gofakes3.CheckPutConditions(conditions, objectInfo); err != nil {
			return result, err
		}
	}

	// If versioning is enabled, generate version ID and possibly move existing version
	var versionID gofakes3.VersionID
	if db.versioning[bucketName] == gofakes3.VersioningEnabled {
		// Generate version ID for the NEW version being created
		versionID = db.nextVersion()

		// Check if object already exists - if so, move it to versions with its OLD version ID
		objectPath := path.Join(bucketName, objectName)
		objectFilePath := filepath.FromSlash(objectPath)
		stat, err := db.bucketFs.Stat(objectFilePath)
		if err == nil {
			// Load the old version's metadata to get its version ID
			oldMeta, err := db.metaStore.loadMeta(bucketName, objectName, stat.Size(), stat.ModTime())
			if err != nil {
				return result, err
			}

			// The old version ID should be stored in metadata
			oldVersionID := gofakes3.VersionID(oldMeta.VersionID)
			if oldVersionID == "" {
				// This shouldn't happen with versioning enabled, but handle it gracefully
				return result, fmt.Errorf("versioning enabled but no version ID in metadata")
			}

			// Move current version to .versions directory
			if err := db.moveToVersions(bucketName, objectName, oldVersionID); err != nil {
				return result, err
			}

			// Save metadata for the old version
			oldMetaPath := db.metaStore.metaPath(bucketName, objectName+"-version-"+string(oldVersionID))
			if err := db.metaStore.saveMeta(oldMetaPath, oldMeta); err != nil {
				return result, err
			}
		}
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
	w := io.MultiWriter(f, hasher)
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
		VersionID: string(versionID), // Store version ID in metadata
	}
	if err := db.metaStore.saveMeta(db.metaStore.metaPath(bucketName, objectName), storedMeta); err != nil {
		return result, err
	}

	// Return version ID if versioning is enabled
	if db.versioning[bucketName] == gofakes3.VersioningEnabled {
		result.VersionID = versionID
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
	if db.versioning[bucketName] == gofakes3.VersioningEnabled {
		// Check if object exists
		objectPath := path.Join(bucketName, objectName)
		objectFilePath := filepath.FromSlash(objectPath)
		if _, err := db.bucketFs.Stat(objectFilePath); err == nil {
			// Generate version ID for the CURRENT version before moving it
			oldVersionID := db.nextVersion()

			// Move current version to .versions directory
			if err := db.moveToVersions(bucketName, objectName, oldVersionID); err != nil {
				return result, err
			}

			// Copy metadata for the old version
			oldMeta, err := db.metaStore.loadMeta(bucketName, objectName, 0, time.Time{})
			if err == nil {
				oldMetaPath := db.metaStore.metaPath(bucketName, objectName+"-version-"+string(oldVersionID))
				if err := db.metaStore.saveMeta(oldMetaPath, oldMeta); err != nil {
					return result, err
				}
			}

			// Delete the current file (which represents the delete marker)
			if err := db.bucketFs.Remove(objectFilePath); err != nil {
				return result, err
			}
		}

		// Generate version ID for the delete marker
		deleteMarkerID := db.nextVersion()
		result.IsDeleteMarker = true
		result.VersionID = deleteMarkerID
		return result, nil
	}

	return result, db.deleteObjectLocked(bucketName, objectName)
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

// VersioningConfiguration returns the versioning configuration for the bucket
func (db *MultiBucketBackend) VersioningConfiguration(bucketName string) (gofakes3.VersioningConfiguration, error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	exists, err := afero.Exists(db.bucketFs, bucketName)
	if err != nil {
		return gofakes3.VersioningConfiguration{}, err
	}
	if !exists {
		return gofakes3.VersioningConfiguration{}, gofakes3.BucketNotFound(bucketName)
	}

	status := db.versioning[bucketName]
	return gofakes3.VersioningConfiguration{Status: status}, nil
}

// SetVersioningConfiguration sets the versioning configuration for the bucket
func (db *MultiBucketBackend) SetVersioningConfiguration(bucketName string, v gofakes3.VersioningConfiguration) error {
	if v.MFADelete.Enabled() {
		return gofakes3.ErrNotImplemented
	}

	db.lock.Lock()
	defer db.lock.Unlock()

	exists, err := afero.Exists(db.bucketFs, bucketName)
	if err != nil {
		return err
	}
	if !exists {
		return gofakes3.BucketNotFound(bucketName)
	}

	if v.Enabled() {
		db.versioning[bucketName] = gofakes3.VersioningEnabled
	} else if db.versioning[bucketName] == gofakes3.VersioningEnabled {
		db.versioning[bucketName] = gofakes3.VersioningSuspended
	}

	return nil
}

// GetObjectVersion retrieves a specific version of an object
func (db *MultiBucketBackend) GetObjectVersion(bucketName, objectName string, versionID gofakes3.VersionID, rangeRequest *gofakes3.ObjectRangeRequest) (*gofakes3.Object, error) {
	if versionID == "" {
		return db.GetObject(bucketName, objectName, rangeRequest)
	}

	db.lock.Lock()
	defer db.lock.Unlock()

	// Check bucket exists
	exists, err := afero.Exists(db.bucketFs, bucketName)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, gofakes3.BucketNotFound(bucketName)
	}

	versionPath := db.versionPath(bucketName, objectName, versionID)

	f, err := db.bucketFs.Open(versionPath)
	if os.IsNotExist(err) {
		return nil, gofakes3.ErrNoSuchVersion
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

	meta, err := db.metaStore.loadMeta(bucketName, objectName+"-version-"+string(versionID), size, mtime)
	if err != nil {
		return nil, err
	}

	return &gofakes3.Object{
		Name:      objectName,
		Hash:      meta.Hash,
		Metadata:  meta.Meta,
		Size:      size,
		Range:     rnge,
		VersionID: versionID,
		Contents:  rdr,
	}, nil
}

// HeadObjectVersion retrieves metadata for a specific version of an object
func (db *MultiBucketBackend) HeadObjectVersion(bucketName, objectName string, versionID gofakes3.VersionID) (*gofakes3.Object, error) {
	if versionID == "" {
		return db.HeadObject(bucketName, objectName)
	}

	db.lock.Lock()
	defer db.lock.Unlock()

	// Check bucket exists
	exists, err := afero.Exists(db.bucketFs, bucketName)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, gofakes3.BucketNotFound(bucketName)
	}

	versionPath := db.versionPath(bucketName, objectName, versionID)

	stat, err := db.bucketFs.Stat(versionPath)
	if os.IsNotExist(err) {
		return nil, gofakes3.ErrNoSuchVersion
	} else if err != nil {
		return nil, err
	}

	size, mtime := stat.Size(), stat.ModTime()
	meta, err := db.metaStore.loadMeta(bucketName, objectName+"-version-"+string(versionID), size, mtime)
	if err != nil {
		return nil, err
	}

	return &gofakes3.Object{
		Name:      objectName,
		Hash:      meta.Hash,
		Metadata:  meta.Meta,
		Size:      size,
		VersionID: versionID,
		Contents:  s3io.NoOpReadCloser{},
	}, nil
}

// DeleteObjectVersion permanently deletes a specific version of an object
func (db *MultiBucketBackend) DeleteObjectVersion(bucketName, objectName string, versionID gofakes3.VersionID) (gofakes3.ObjectDeleteResult, error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	// Check bucket exists
	exists, err := afero.Exists(db.bucketFs, bucketName)
	if err != nil {
		return gofakes3.ObjectDeleteResult{}, err
	}
	if !exists {
		return gofakes3.ObjectDeleteResult{}, gofakes3.BucketNotFound(bucketName)
	}

	versionPath := db.versionPath(bucketName, objectName, versionID)

	// S3 does not report an error when attempting to delete a version that does not exist
	if err := db.bucketFs.Remove(versionPath); err != nil && !os.IsNotExist(err) {
		return gofakes3.ObjectDeleteResult{}, err
	}

	metaPath := db.metaStore.metaPath(bucketName, objectName+"-version-"+string(versionID))
	if err := db.metaStore.deleteMeta(metaPath); err != nil {
		return gofakes3.ObjectDeleteResult{}, err
	}

	return gofakes3.ObjectDeleteResult{VersionID: versionID}, nil
}

// DeleteMultiVersions deletes multiple object versions
func (db *MultiBucketBackend) DeleteMultiVersions(bucketName string, objects ...gofakes3.ObjectID) (gofakes3.MultiDeleteResult, error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	// Check bucket exists
	exists, err := afero.Exists(db.bucketFs, bucketName)
	if err != nil {
		return gofakes3.MultiDeleteResult{}, err
	}
	if !exists {
		return gofakes3.MultiDeleteResult{}, gofakes3.BucketNotFound(bucketName)
	}

	var result gofakes3.MultiDeleteResult

	for _, object := range objects {
		if object.VersionID != "" {
			_, err := db.DeleteObjectVersion(bucketName, object.Key, gofakes3.VersionID(object.VersionID))
			if err != nil {
				log.Println("delete version failed:", err)
				result.Error = append(result.Error, gofakes3.ErrorResult{
					Code:    gofakes3.ErrInternal,
					Message: gofakes3.ErrInternal.Message(),
					Key:     object.Key,
				})
			} else {
				result.Deleted = append(result.Deleted, object)
			}
		} else {
			if err := db.deleteObjectLocked(bucketName, object.Key); err != nil {
				log.Println("delete object failed:", err)
				result.Error = append(result.Error, gofakes3.ErrorResult{
					Code:    gofakes3.ErrInternal,
					Message: gofakes3.ErrInternal.Message(),
					Key:     object.Key,
				})
			} else {
				result.Deleted = append(result.Deleted, object)
			}
		}
	}

	return result, nil
}

// ListBucketVersions lists all versions of all objects in a bucket
func (db *MultiBucketBackend) ListBucketVersions(bucketName string, prefix *gofakes3.Prefix, page *gofakes3.ListBucketVersionsPage) (*gofakes3.ListBucketVersionsResult, error) {
	if prefix == nil {
		prefix = emptyPrefix
	}
	if page == nil {
		page = &gofakes3.ListBucketVersionsPage{}
	}

	db.lock.Lock()
	defer db.lock.Unlock()

	// Check bucket exists
	stat, err := db.bucketFs.Stat(filepath.FromSlash(bucketName))
	if os.IsNotExist(err) {
		return nil, gofakes3.BucketNotFound(bucketName)
	} else if err != nil {
		return nil, err
	} else if !stat.IsDir() {
		return nil, fmt.Errorf("gofakes3: expected %q to be a bucket path", bucketName)
	}

	result := gofakes3.NewListBucketVersionsResult(bucketName, prefix, page)

	// Walk the filesystem and collect all objects and versions
	if err := afero.Walk(db.bucketFs, filepath.FromSlash(bucketName), func(filePath string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}

		objectPath := filepath.ToSlash(filePath)
		parts := strings.SplitN(objectPath, "/", 2)
		if len(parts) != 2 {
			return nil // Skip bucket root files
		}
		objectName := parts[1]

		// Skip .versions directory in main listing
		if strings.HasPrefix(objectName, ".versions/") {
			return nil
		}

		var match gofakes3.PrefixMatch
		if !prefix.Match(objectName, &match) {
			return nil
		}

		if match.CommonPrefix {
			result.AddPrefix(match.MatchedPart)
			return nil
		}

		size := info.Size()
		mtime := info.ModTime()
		meta, err := db.metaStore.loadMeta(bucketName, objectName, size, mtime)
		if err != nil {
			return err
		}

		// Add current version
		ver := &gofakes3.Version{
			Key:          objectName,
			IsLatest:     true,
			LastModified: gofakes3.NewContentTime(mtime),
			Size:         size,
			ETag:         gofakes3.FormatETag(meta.Hash),
		}
		status := db.versioning[bucketName]
		if status != gofakes3.VersioningNone {
			ver.VersionID = "null" // Current version when versioning is enabled
		}
		result.Versions = append(result.Versions, ver)

		// List old versions from .versions directory
		versionsDir := filepath.FromSlash(path.Join(bucketName, ".versions", objectName))
		versionEntries, err := afero.ReadDir(db.bucketFs, versionsDir)
		if err != nil && !os.IsNotExist(err) {
			return err
		}

		for _, vEntry := range versionEntries {
			if vEntry.IsDir() {
				continue
			}

			versionID := gofakes3.VersionID(vEntry.Name())
			vSize := vEntry.Size()
			vMtime := vEntry.ModTime()
			vMeta, err := db.metaStore.loadMeta(bucketName, objectName+"-version-"+string(versionID), vSize, vMtime)
			if err != nil {
				return err
			}

			oldVer := &gofakes3.Version{
				Key:          objectName,
				VersionID:    versionID,
				IsLatest:     false,
				LastModified: gofakes3.NewContentTime(vMtime),
				Size:         vSize,
				ETag:         gofakes3.FormatETag(vMeta.Hash),
			}
			result.Versions = append(result.Versions, oldVer)
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return result, nil
}

// getConditionalObjectInfo returns information about an object for conditional checking.
// This method assumes the backend lock is already held.
func (db *MultiBucketBackend) getConditionalObjectInfo(bucketName, objectName string) (*gofakes3.ConditionalObjectInfo, error) {
	fullPath := path.Join(bucketName, objectName)

	stat, err := db.bucketFs.Stat(filepath.FromSlash(fullPath))
	if os.IsNotExist(err) {
		return &gofakes3.ConditionalObjectInfo{Exists: false}, nil
	} else if err != nil {
		return nil, err
	} else if stat.IsDir() {
		return &gofakes3.ConditionalObjectInfo{Exists: false}, nil
	}

	size, mtime := stat.Size(), stat.ModTime()
	meta, err := db.metaStore.loadMeta(bucketName, objectName, size, mtime)
	if err != nil {
		return nil, err
	}

	return &gofakes3.ConditionalObjectInfo{
		Exists: true,
		Hash:   meta.Hash,
	}, nil
}

// nextVersion generates a new version ID. Assumes lock is held.
func (db *MultiBucketBackend) nextVersion() gofakes3.VersionID {
	v, scr := db.versionGenerator.Next(db.versionScratch)
	db.versionScratch = scr
	return v
}

// versionPath returns the filesystem path for a versioned object
func (db *MultiBucketBackend) versionPath(bucketName, objectName string, versionID gofakes3.VersionID) string {
	return filepath.FromSlash(path.Join(bucketName, ".versions", objectName, string(versionID)))
}

// moveToVersions moves the current version of an object to the versions directory
func (db *MultiBucketBackend) moveToVersions(bucketName, objectName string, versionID gofakes3.VersionID) error {
	srcPath := filepath.FromSlash(path.Join(bucketName, objectName))
	dstPath := db.versionPath(bucketName, objectName, versionID)
	dstDir := filepath.Dir(dstPath)

	// Create versions directory
	if err := db.bucketFs.MkdirAll(dstDir, db.dirMode); err != nil {
		return err
	}

	// Read the current file
	data, err := afero.ReadFile(db.bucketFs, srcPath)
	if err != nil {
		return err
	}

	// Write to version path
	if err := afero.WriteFile(db.bucketFs, dstPath, data, 0666); err != nil {
		return err
	}

	return nil
}

// ensureMeta loads or creates metadata for an object, generating a version ID if needed
func (db *MultiBucketBackend) ensureMeta(
	bucket string,
	objectName string,
	size int64,
	mtime time.Time,
) (meta *Metadata, err error) {
	existingMeta, err := db.metaStore.loadMeta(bucket, objectName, size, mtime)
	if errors.Is(err, os.ErrNotExist) {
		// File exists but no metadata - this is an externally added file
		fullPath := filepath.FromSlash(path.Join(bucket, objectName))
		f, err := db.bucketFs.Open(fullPath)
		if err != nil {
			return nil, err
		}
		defer f.Close()

		hasher := md5.New()
		if _, err := io.Copy(hasher, f); err != nil {
			return nil, err
		}

		hash := hasher.Sum(nil)

		// Generate version ID if versioning is enabled
		var versionID string
		if db.versioning[bucket] == gofakes3.VersioningEnabled {
			versionID = string(db.nextVersion())
		}

		newMeta := &Metadata{
			File:      path.Join(bucket, objectName),
			ModTime:   mtime,
			Size:      size,
			Hash:      hash,
			Meta:      map[string]string{},
			VersionID: versionID,
		}

		// Save the generated metadata
		if err := db.metaStore.saveMeta(db.metaStore.metaPath(bucket, objectName), newMeta); err != nil {
			return nil, err
		}

		return newMeta, nil

	} else if err != nil {
		return nil, err

	} else {
		// Metadata exists - check if we need to add a version ID
		if existingMeta.VersionID == "" && db.versioning[bucket] == gofakes3.VersioningEnabled {
			// File existed before versioning was enabled, assign it a version ID now
			existingMeta.VersionID = string(db.nextVersion())
			if err := db.metaStore.saveMeta(db.metaStore.metaPath(bucket, objectName), existingMeta); err != nil {
				return nil, err
			}
		}
		return existingMeta, nil
	}
}
