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
	"strings"
	"sync"
	"time"

	"github.com/spf13/afero"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/internal/s3io"
)

// SingleBucketBackend is a gofakes3.Backend that allows you to treat an existing
// filesystem as an S3 bucket directly. It does not support multiple buckets.
//
// A second afero.Fs, metaFs, may be passed; if this is nil,
// afero.NewMemMapFs() is used and the metadata will not persist between
// restarts of gofakes3.
//
// It is STRONGLY recommended that the metadata Fs is not contained within the
// `/buckets` subdirectory as that could make a significant mess, but this is
// infeasible to validate, so you're encouraged to be extremely careful!
type SingleBucketBackend struct {
	lock             sync.Mutex
	fs               afero.Fs
	metaStore        *metaStore
	name             string
	versioning       gofakes3.VersioningStatus
	versionGenerator *versionGenerator
	versionScratch   []byte
	timeSource       gofakes3.TimeSource
}

var _ gofakes3.Backend = &SingleBucketBackend{}
var _ gofakes3.VersionedBackend = &SingleBucketBackend{}

func SingleBucket(name string, fs afero.Fs, metaFs afero.Fs, opts ...SingleOption) (*SingleBucketBackend, error) {
	if err := ensureNoOsFs("fs", fs); err != nil {
		return nil, err
	}

	if metaFs == nil {
		metaFs = afero.NewMemMapFs()
	} else {
		if err := ensureNoOsFs("metaFs", metaFs); err != nil {
			return nil, err
		}
	}

	if err := gofakes3.ValidateBucketName(name); err != nil {
		return nil, err
	}

	timeSource := gofakes3.DefaultTimeSource()

	b := &SingleBucketBackend{
		name:             name,
		fs:               fs,
		metaStore:        newMetaStore(metaFs, modTimeFsCalc(fs)),
		versioning:       gofakes3.VersioningNone,
		versionGenerator: newVersionGenerator(uint64(timeSource.Now().UnixNano()), 0),
		timeSource:       timeSource,
	}
	for _, opt := range opts {
		if err := opt(b); err != nil {
			return nil, err
		}
	}

	return b, nil
}

func (db *SingleBucketBackend) ListBuckets() ([]gofakes3.BucketInfo, error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	var created time.Time

	stat, err := db.fs.Stat("")
	if os.IsNotExist(err) {
		created = time.Now()
	} else if err != nil {
		return nil, err
	} else {
		created = stat.ModTime()
	}

	// FIXME: "birth time" is not available cross-platform.
	// See MultiBucketBackend.ListBuckets for more details.
	return []gofakes3.BucketInfo{
		{Name: db.name, CreationDate: gofakes3.NewContentTime(created)},
	}, nil
}

func (db *SingleBucketBackend) ListBucket(bucket string, prefix *gofakes3.Prefix, page gofakes3.ListBucketPage) (*gofakes3.ObjectList, error) {
	if bucket != db.name {
		return nil, gofakes3.BucketNotFound(bucket)
	}
	if prefix == nil {
		prefix = emptyPrefix
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

func (db *SingleBucketBackend) getBucketWithFilePrefixLocked(bucket string, prefixPath, prefixPart string) (*gofakes3.ObjectList, error) {
	dirEntries, err := afero.ReadDir(db.fs, filepath.FromSlash(prefixPath))
	if err != nil {
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
			response.AddPrefix(path.Join(prefixPath, entry.Name()) + "/")

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
		}
	}

	return response, nil
}

func (db *SingleBucketBackend) getBucketWithArbitraryPrefixLocked(bucket string, prefix *gofakes3.Prefix) (*gofakes3.ObjectList, error) {
	response := gofakes3.NewObjectList()

	if err := afero.Walk(db.fs, filepath.FromSlash("."), func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}

		objectPath := filepath.ToSlash(path)
		if !prefix.Match(objectPath, nil) {
			return nil
		}

		size := info.Size()
		mtime := info.ModTime()
		meta, err := db.ensureMeta(bucket, objectPath, size, mtime)
		if err != nil {
			return err
		}

		response.Add(&gofakes3.Content{
			Key:          objectPath,
			LastModified: gofakes3.NewContentTime(mtime),
			ETag:         gofakes3.FormatETag(meta.Hash),
			Size:         size,
		})

		return nil

	}); err != nil {
		return nil, err
	}

	return response, nil
}

func (db *SingleBucketBackend) ensureMeta(
	bucket string,
	objectPath string,
	size int64,
	mtime time.Time,
) (meta *Metadata, err error) {
	existingMeta, err := db.metaStore.loadMeta(bucket, objectPath, size, mtime)
	if errors.Is(err, os.ErrNotExist) {
		// File exists but no metadata - this is an externally added file
		f, err := db.fs.Open(filepath.FromSlash(objectPath))
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
		if db.versioning == gofakes3.VersioningEnabled {
			versionID = string(db.nextVersion())
		}

		newMeta := &Metadata{
			File:      objectPath,
			ModTime:   mtime,
			Size:      size,
			Hash:      hash,
			Meta:      map[string]string{},
			VersionID: versionID,
		}

		// Save the generated metadata
		if err := db.metaStore.saveMeta(db.metaStore.metaPath(bucket, objectPath), newMeta); err != nil {
			return nil, err
		}

		return newMeta, nil

	} else if err != nil {
		return nil, err

	} else {
		// Metadata exists - check if we need to add a version ID
		if existingMeta.VersionID == "" && db.versioning == gofakes3.VersioningEnabled {
			// File existed before versioning was enabled, assign it a version ID now
			existingMeta.VersionID = string(db.nextVersion())
			if err := db.metaStore.saveMeta(db.metaStore.metaPath(bucket, objectPath), existingMeta); err != nil {
				return nil, err
			}
		}
		return existingMeta, nil
	}
}

func (db *SingleBucketBackend) HeadObject(bucketName, objectName string) (*gofakes3.Object, error) {
	if bucketName != db.name {
		return nil, gofakes3.BucketNotFound(bucketName)
	}

	db.lock.Lock()
	defer db.lock.Unlock()

	stat, err := db.fs.Stat(filepath.FromSlash(objectName))
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
		Name:     objectName,
		Hash:     meta.Hash,
		Metadata: meta.Meta,
		Size:     size,
		Contents: s3io.NoOpReadCloser{},
	}, nil
}

func (db *SingleBucketBackend) GetObject(bucketName, objectName string, rangeRequest *gofakes3.ObjectRangeRequest) (obj *gofakes3.Object, err error) {
	if bucketName != db.name {
		return nil, gofakes3.BucketNotFound(bucketName)
	}

	db.lock.Lock()
	defer db.lock.Unlock()

	f, err := db.fs.Open(filepath.FromSlash(objectName))
	if os.IsNotExist(err) {
		return nil, gofakes3.KeyNotFound(objectName)
	} else if err != nil {
		return nil, err
	}
	defer func() {
		// If an error occurs, the caller may not have access to Object.Body in order to close it:
		if err != nil && obj == nil {
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
		Name:      objectName,
		Hash:      meta.Hash,
		Metadata:  meta.Meta,
		Size:      size,
		Range:     rnge,
		VersionID: gofakes3.VersionID(meta.VersionID),
		Contents:  rdr,
	}, nil
}

func (db *SingleBucketBackend) PutObject(
	bucketName, objectName string,
	meta map[string]string,
	input io.Reader,
	size int64,
	conditions *gofakes3.PutConditions,
) (result gofakes3.PutObjectResult, err error) {

	if bucketName != db.name {
		return result, gofakes3.BucketNotFound(bucketName)
	}

	err = gofakes3.MergeMetadata(db, bucketName, objectName, meta)
	if err != nil {
		return result, err
	}

	db.lock.Lock()
	defer db.lock.Unlock()

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
	if db.versioning == gofakes3.VersioningEnabled {
		// Generate version ID for the NEW version being created
		versionID = db.nextVersion()

		// Check if object already exists - if so, move it to versions with its OLD version ID
		objectFilePath := filepath.FromSlash(objectName)
		stat, err := db.fs.Stat(objectFilePath)
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
			if err := db.moveToVersions(objectName, oldVersionID); err != nil {
				return result, err
			}

			// Save metadata for the old version
			oldMetaPath := db.metaStore.metaPath(bucketName, objectName+"-version-"+string(oldVersionID))
			if err := db.metaStore.saveMeta(oldMetaPath, oldMeta); err != nil {
				return result, err
			}
		}
	}

	objectFilePath := filepath.FromSlash(objectName)
	objectDir := filepath.Dir(objectFilePath)

	if objectDir != "." {
		if err := db.fs.MkdirAll(objectDir, 0777); err != nil {
			return result, err
		}
	}

	f, err := db.fs.Create(objectFilePath)
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

	stat, err := db.fs.Stat(objectFilePath)
	if err != nil {
		return result, err
	}

	storedMeta := &Metadata{
		File:      objectName,
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
	if db.versioning == gofakes3.VersioningEnabled {
		result.VersionID = versionID
	}

	return result, nil
}

func (db *SingleBucketBackend) DeleteMulti(bucketName string, objects ...string) (result gofakes3.MultiDeleteResult, rerr error) {
	if bucketName != db.name {
		return result, gofakes3.BucketNotFound(bucketName)
	}

	db.lock.Lock()
	defer db.lock.Unlock()

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

func (db *SingleBucketBackend) CopyObject(srcBucket, srcKey, dstBucket, dstKey string, meta map[string]string) (result gofakes3.CopyObjectResult, err error) {
	return gofakes3.CopyObject(db, srcBucket, srcKey, dstBucket, dstKey, meta)
}

func (db *SingleBucketBackend) DeleteObject(bucketName, objectName string) (result gofakes3.ObjectDeleteResult, rerr error) {
	if bucketName != db.name {
		return result, gofakes3.BucketNotFound(bucketName)
	}

	db.lock.Lock()
	defer db.lock.Unlock()

	// If versioning is enabled, create a delete marker instead of actually deleting
	if db.versioning == gofakes3.VersioningEnabled {
		// Check if object exists
		objectFilePath := filepath.FromSlash(objectName)
		if _, err := db.fs.Stat(objectFilePath); err == nil {
			// Generate version ID for the CURRENT version before moving it
			oldVersionID := db.nextVersion()

			// Move current version to .versions directory
			if err := db.moveToVersions(objectName, oldVersionID); err != nil {
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
			if err := db.fs.Remove(objectFilePath); err != nil {
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

func (db *SingleBucketBackend) deleteObjectLocked(bucketName, objectName string) error {
	// S3 does not report an error when attemping to delete a key that does not exist, so
	// we need to skip IsNotExist errors.
	if err := db.fs.Remove(filepath.FromSlash(objectName)); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := db.metaStore.deleteMeta(db.metaStore.metaPath(bucketName, objectName)); err != nil {
		return err
	}

	return nil
}

// CreateBucket cannot be implemented by this backend. See MultiBucketBackend if you
// need a backend that supports it.
func (db *SingleBucketBackend) CreateBucket(name string) error {
	return gofakes3.ErrNotImplemented
}

// DeleteBucket cannot be implemented by this backend. See MultiBucketBackend if you
// need a backend that supports it.
func (db *SingleBucketBackend) DeleteBucket(name string) error {
	return gofakes3.ErrNotImplemented
}

func (db *SingleBucketBackend) ForceDeleteBucket(name string) error {
	if name != db.name {
		return gofakes3.BucketNotFound(name)
	}

	db.lock.Lock()
	defer db.lock.Unlock()

	// Delete all objects in the bucket
	var objects []string
	err := afero.Walk(db.fs, ".", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			objects = append(objects, path)
		}
		return nil
	})
	if err != nil {
		return err
	}

	for _, object := range objects {
		if err := db.deleteObjectLocked(name, object); err != nil {
			return err
		}
	}

	// Delete the bucket itself
	if err := db.fs.RemoveAll("."); err != nil {
		return err
	}

	return nil
}

func (db *SingleBucketBackend) BucketExists(name string) (exists bool, err error) {
	return db.name == name, nil
}

// VersioningConfiguration returns the versioning configuration for the bucket
func (db *SingleBucketBackend) VersioningConfiguration(bucketName string) (gofakes3.VersioningConfiguration, error) {
	if bucketName != db.name {
		return gofakes3.VersioningConfiguration{}, gofakes3.BucketNotFound(bucketName)
	}

	db.lock.Lock()
	defer db.lock.Unlock()

	return gofakes3.VersioningConfiguration{Status: db.versioning}, nil
}

// SetVersioningConfiguration sets the versioning configuration for the bucket
func (db *SingleBucketBackend) SetVersioningConfiguration(bucketName string, v gofakes3.VersioningConfiguration) error {
	if bucketName != db.name {
		return gofakes3.BucketNotFound(bucketName)
	}

	if v.MFADelete.Enabled() {
		return gofakes3.ErrNotImplemented
	}

	db.lock.Lock()
	defer db.lock.Unlock()

	if v.Enabled() {
		db.versioning = gofakes3.VersioningEnabled
	} else if db.versioning == gofakes3.VersioningEnabled {
		db.versioning = gofakes3.VersioningSuspended
	}

	return nil
}

// GetObjectVersion retrieves a specific version of an object
func (db *SingleBucketBackend) GetObjectVersion(bucketName, objectName string, versionID gofakes3.VersionID, rangeRequest *gofakes3.ObjectRangeRequest) (*gofakes3.Object, error) {
	if bucketName != db.name {
		return nil, gofakes3.BucketNotFound(bucketName)
	}

	if versionID == "" {
		return db.GetObject(bucketName, objectName, rangeRequest)
	}

	db.lock.Lock()
	defer db.lock.Unlock()

	versionPath := db.versionPath(objectName, versionID)

	f, err := db.fs.Open(versionPath)
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
func (db *SingleBucketBackend) HeadObjectVersion(bucketName, objectName string, versionID gofakes3.VersionID) (*gofakes3.Object, error) {
	if bucketName != db.name {
		return nil, gofakes3.BucketNotFound(bucketName)
	}

	if versionID == "" {
		return db.HeadObject(bucketName, objectName)
	}

	db.lock.Lock()
	defer db.lock.Unlock()

	versionPath := db.versionPath(objectName, versionID)

	stat, err := db.fs.Stat(versionPath)
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
func (db *SingleBucketBackend) DeleteObjectVersion(bucketName, objectName string, versionID gofakes3.VersionID) (gofakes3.ObjectDeleteResult, error) {
	if bucketName != db.name {
		return gofakes3.ObjectDeleteResult{}, gofakes3.BucketNotFound(bucketName)
	}

	db.lock.Lock()
	defer db.lock.Unlock()

	versionPath := db.versionPath(objectName, versionID)

	// S3 does not report an error when attempting to delete a version that does not exist
	if err := db.fs.Remove(versionPath); err != nil && !os.IsNotExist(err) {
		return gofakes3.ObjectDeleteResult{}, err
	}

	metaPath := db.metaStore.metaPath(bucketName, objectName+"-version-"+string(versionID))
	if err := db.metaStore.deleteMeta(metaPath); err != nil {
		return gofakes3.ObjectDeleteResult{}, err
	}

	return gofakes3.ObjectDeleteResult{VersionID: versionID}, nil
}

// DeleteMultiVersions deletes multiple object versions
func (db *SingleBucketBackend) DeleteMultiVersions(bucketName string, objects ...gofakes3.ObjectID) (gofakes3.MultiDeleteResult, error) {
	if bucketName != db.name {
		return gofakes3.MultiDeleteResult{}, gofakes3.BucketNotFound(bucketName)
	}

	db.lock.Lock()
	defer db.lock.Unlock()

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
func (db *SingleBucketBackend) ListBucketVersions(bucketName string, prefix *gofakes3.Prefix, page *gofakes3.ListBucketVersionsPage) (*gofakes3.ListBucketVersionsResult, error) {
	if bucketName != db.name {
		return nil, gofakes3.BucketNotFound(bucketName)
	}

	if prefix == nil {
		prefix = emptyPrefix
	}
	if page == nil {
		page = &gofakes3.ListBucketVersionsPage{}
	}

	db.lock.Lock()
	defer db.lock.Unlock()

	result := gofakes3.NewListBucketVersionsResult(bucketName, prefix, page)

	// Walk the filesystem and collect all objects and versions
	if err := afero.Walk(db.fs, filepath.FromSlash("."), func(filePath string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}

		objectPath := filepath.ToSlash(filePath)

		// Skip .versions directory in main listing
		if strings.HasPrefix(objectPath, ".versions/") {
			return nil
		}

		var match gofakes3.PrefixMatch
		if !prefix.Match(objectPath, &match) {
			return nil
		}

		if match.CommonPrefix {
			result.AddPrefix(match.MatchedPart)
			return nil
		}

		size := info.Size()
		mtime := info.ModTime()
		meta, err := db.metaStore.loadMeta(bucketName, objectPath, size, mtime)
		if err != nil {
			return err
		}

		// Add current version
		ver := &gofakes3.Version{
			Key:          objectPath,
			IsLatest:     true,
			LastModified: gofakes3.NewContentTime(mtime),
			Size:         size,
			ETag:         gofakes3.FormatETag(meta.Hash),
		}
		if db.versioning != gofakes3.VersioningNone {
			ver.VersionID = "null" // Current version when versioning is enabled
		}
		result.Versions = append(result.Versions, ver)

		// List old versions from .versions directory
		versionsDir := filepath.FromSlash(path.Join(".versions", objectPath))
		versionEntries, err := afero.ReadDir(db.fs, versionsDir)
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
			vMeta, err := db.metaStore.loadMeta(bucketName, objectPath+"-version-"+string(versionID), vSize, vMtime)
			if err != nil {
				return err
			}

			oldVer := &gofakes3.Version{
				Key:          objectPath,
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
func (db *SingleBucketBackend) getConditionalObjectInfo(bucketName, objectName string) (*gofakes3.ConditionalObjectInfo, error) {
	stat, err := db.fs.Stat(filepath.FromSlash(objectName))
	if os.IsNotExist(err) {
		return &gofakes3.ConditionalObjectInfo{Exists: false}, nil
	} else if err != nil {
		return nil, err
	} else if stat.IsDir() {
		return &gofakes3.ConditionalObjectInfo{Exists: false}, nil
	}

	size, mtime := stat.Size(), stat.ModTime()
	meta, err := db.ensureMeta(bucketName, objectName, size, mtime)
	if err != nil {
		return nil, err
	}

	return &gofakes3.ConditionalObjectInfo{
		Exists: true,
		Hash:   meta.Hash,
	}, nil
}

// nextVersion generates a new version ID. Assumes lock is held.
func (db *SingleBucketBackend) nextVersion() gofakes3.VersionID {
	v, scr := db.versionGenerator.Next(db.versionScratch)
	db.versionScratch = scr
	return v
}

// versionPath returns the filesystem path for a versioned object
func (db *SingleBucketBackend) versionPath(objectName string, versionID gofakes3.VersionID) string {
	return filepath.FromSlash(path.Join(".versions", objectName, string(versionID)))
}

// moveToVersions moves the current version of an object to the versions directory
func (db *SingleBucketBackend) moveToVersions(objectName string, versionID gofakes3.VersionID) error {
	srcPath := filepath.FromSlash(objectName)
	dstPath := db.versionPath(objectName, versionID)
	dstDir := filepath.Dir(dstPath)

	// Create versions directory
	if err := db.fs.MkdirAll(dstDir, 0777); err != nil {
		return err
	}

	// Read the current file
	data, err := afero.ReadFile(db.fs, srcPath)
	if err != nil {
		return err
	}

	// Write to version path
	if err := afero.WriteFile(db.fs, dstPath, data, 0666); err != nil {
		return err
	}

	return nil
}
