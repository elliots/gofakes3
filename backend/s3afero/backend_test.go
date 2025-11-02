package s3afero

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"testing"

	"github.com/spf13/afero"

	"github.com/johannesboyne/gofakes3"
)

func testingBackends(t *testing.T) []gofakes3.Backend {
	t.Helper()

	single, err := SingleBucket("test", afero.NewMemMapFs(), nil)
	if err != nil {
		t.Fatal(err)
	}
	multi, err := MultiBucket(afero.NewMemMapFs())
	if err != nil {
		t.Fatal(err)
	}
	if err := multi.CreateBucket("test"); err != nil {
		t.Fatal(err)
	}

	backends := []gofakes3.Backend{single, multi}
	return backends
}

func TestPutGet(t *testing.T) {
	backends := testingBackends(t)

	for _, backend := range backends {
		t.Run("", func(t *testing.T) {
			meta := map[string]string{
				"foo": "bar",
			}

			contents := []byte("contents")
			if _, err := backend.PutObject("test", "yep", meta, bytes.NewReader(contents), int64(len(contents)), nil); err != nil {
				t.Fatal(err)
			}
			hasher := md5.New()
			hasher.Write(contents)
			hash := hasher.Sum(nil)

			obj, err := backend.GetObject("test", "yep", nil)
			if err != nil {
				t.Fatal(err)
			}

			if !reflect.DeepEqual(obj.Metadata, meta) {
				t.Fatal(obj.Metadata, "!=", meta)
			}

			result, err := ioutil.ReadAll(obj.Contents)
			defer obj.Contents.Close()
			if err != nil {
				t.Fatal(err)
			}

			if !bytes.Equal(contents, result) {
				t.Fatal(result, "!=", contents)
			}
			if obj.Size != int64(len(contents)) {
				t.Fatal(obj.Size, "!=", len(contents))
			}
			if !bytes.Equal(obj.Hash, hash) {
				t.Fatal(hex.EncodeToString(obj.Hash), "!=", hex.EncodeToString(hash))
			}
		})
	}
}

func TestPutGetRange(t *testing.T) {
	backends := testingBackends(t)

	for _, backend := range backends {
		t.Run("", func(t *testing.T) {
			meta := map[string]string{
				"foo": "bar",
			}

			contents := []byte("contents")
			expected := contents[1:7]
			if _, err := backend.PutObject("test", "yep", meta, bytes.NewReader(contents), int64(len(contents)), nil); err != nil {
				t.Fatal(err)
			}
			hasher := md5.New()
			hasher.Write(contents)
			hash := hasher.Sum(nil)

			obj, err := backend.GetObject("test", "yep", &gofakes3.ObjectRangeRequest{Start: 1, End: 6})
			if err != nil {
				t.Fatal(err)
			}

			if !reflect.DeepEqual(obj.Metadata, meta) {
				t.Fatal(obj.Metadata, "!=", meta)
			}

			result, err := ioutil.ReadAll(obj.Contents)
			defer obj.Contents.Close()
			if err != nil {
				t.Fatal(err)
			}

			if !bytes.Equal(expected, result) {
				t.Fatal(result, "!=", expected)
			}
			if obj.Size != int64(len(contents)) {
				t.Fatal(obj.Size, "!=", len(contents))
			}
			if !bytes.Equal(obj.Hash, hash) {
				t.Fatal(hex.EncodeToString(obj.Hash), "!=", hex.EncodeToString(hash))
			}
		})
	}
}

func TestPutListRoot(t *testing.T) {
	backends := testingBackends(t)

	for _, backend := range backends {
		t.Run(fmt.Sprintf("%T", backend), func(t *testing.T) {
			meta := map[string]string{
				"foo": "bar",
			}

			contents1 := []byte("contents1")
			if _, err := backend.PutObject("test", "foo", meta, bytes.NewReader(contents1), int64(len(contents1)), nil); err != nil {
				t.Fatal(err)
			}

			contents2 := []byte("contents2")
			if _, err := backend.PutObject("test", "bar", meta, bytes.NewReader(contents2), int64(len(contents2)), nil); err != nil {
				t.Fatal(err)
			}

			result, err := backend.ListBucket("test",
				&gofakes3.Prefix{HasPrefix: true, HasDelimiter: true, Delimiter: "/"},
				gofakes3.ListBucketPage{})
			if err != nil {
				t.Fatal(err)
			}

			if len(result.Contents) != 2 {
				t.Fatal()
			}

			if result.Contents[0].ETag != `"b2d0efbdc48f4b7bf42f8ab76d71f84e"` {
				t.Fatal("etag", result.Contents[0].ETag, "!=", `"b2d0efbdc48f4b7bf42f8ab76d71f84e"`)
			}
			if result.Contents[0].Size != 9 {
				t.Fatal("size", result.Contents[0].Size, "!=", 9)
			}
			if result.Contents[1].ETag != `"4891e2a24026da4dea5b4119e1dc1863"` {
				t.Fatal("etag", result.Contents[1].ETag, "!=", `"4891e2a24026da4dea5b4119e1dc1863"`)
			}
			if result.Contents[1].Size != 9 {
				t.Fatal("size", result.Contents[1].Size, "!=", 9)
			}
		})
	}
}

func TestPutListDir(t *testing.T) {
	backends := testingBackends(t)

	for _, backend := range backends {
		t.Run(fmt.Sprintf("%T", backend), func(t *testing.T) {
			meta := map[string]string{
				"foo": "bar",
			}

			contents1 := []byte("contents1")
			if _, err := backend.PutObject("test", "foo/bar", meta, bytes.NewReader(contents1), int64(len(contents1)), nil); err != nil {
				t.Fatal(err)
			}

			contents2 := []byte("contents2")
			if _, err := backend.PutObject("test", "foo/baz", meta, bytes.NewReader(contents2), int64(len(contents2)), nil); err != nil {
				t.Fatal(err)
			}

			{
				result, err := backend.ListBucket("test",
					&gofakes3.Prefix{Prefix: "foo/", HasPrefix: true, HasDelimiter: true, Delimiter: "/"},
					gofakes3.ListBucketPage{})
				if err != nil {
					t.Fatal(err)
				}
				if len(result.Contents) != 2 {
					t.Fatal()
				}
			}

			{
				result, err := backend.ListBucket("test",
					&gofakes3.Prefix{Prefix: "foo/bar", HasPrefix: true, HasDelimiter: true, Delimiter: "/"},
					gofakes3.ListBucketPage{})
				if err != nil {
					t.Fatal(err)
				}
				if len(result.Contents) != 1 {
					t.Fatal()
				}
			}
		})
	}
}

func TestPutDelete(t *testing.T) {
	backends := testingBackends(t)

	for _, backend := range backends {
		t.Run(fmt.Sprintf("%T", backend), func(t *testing.T) {
			meta := map[string]string{
				"foo": "bar",
			}

			contents := []byte("contents1")
			if _, err := backend.PutObject("test", "foo", meta, bytes.NewReader(contents), int64(len(contents)), nil); err != nil {
				t.Fatal(err)
			}

			if _, err := backend.DeleteObject("test", "foo"); err != nil {
				t.Fatal(err)
			}

			result, err := backend.ListBucket("test",
				&gofakes3.Prefix{HasPrefix: true, HasDelimiter: true, Delimiter: "/"},
				gofakes3.ListBucketPage{})
			if err != nil {
				t.Fatal(err)
			}

			if len(result.Contents) != 0 {
				t.Fatal()
			}
		})
	}
}

func TestPutDeleteMulti(t *testing.T) {
	backends := testingBackends(t)

	for _, backend := range backends {
		t.Run(fmt.Sprintf("%T", backend), func(t *testing.T) {
			meta := map[string]string{
				"foo": "bar",
			}

			contents1 := []byte("contents1")
			if _, err := backend.PutObject("test", "foo/bar", meta, bytes.NewReader(contents1), int64(len(contents1)), nil); err != nil {
				t.Fatal(err)
			}

			contents2 := []byte("contents2")
			if _, err := backend.PutObject("test", "foo/baz", meta, bytes.NewReader(contents2), int64(len(contents2)), nil); err != nil {
				t.Fatal(err)
			}

			deleteResult, err := backend.DeleteMulti("test", "foo/bar", "foo/baz")
			if err != nil {
				t.Fatal(err)
			}
			if err := deleteResult.AsError(); err != nil {
				t.Fatal(err)
			}

			bucketContents, err := backend.ListBucket("test",
				&gofakes3.Prefix{HasPrefix: true, HasDelimiter: true, Delimiter: "/"},
				gofakes3.ListBucketPage{})
			if err != nil {
				t.Fatal(err)
			}

			if len(bucketContents.Contents) != 0 {
				t.Fatal()
			}
		})
	}
}

func TestMultiCreateBucket(t *testing.T) {
	// Some bugs surfaced in the bucket creation logic in the MultiBucket backend
	// that only occur when using a real FS.
	tmp, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmp)

	fs, err := FsPath(tmp, 0)
	if err != nil {
		t.Fatal(err)
	}

	multi, err := MultiBucket(fs)
	if err != nil {
		t.Fatal(err)
	}
	if ok, _ := multi.BucketExists("test"); ok {
		t.Fatal()
	}
	if err := multi.CreateBucket("test"); err != nil {
		t.Fatal(err)
	}
	if ok, _ := multi.BucketExists("test"); !ok {
		t.Fatal()
	}
}

// versionedTestingBackends returns backends that implement VersionedBackend
func versionedTestingBackends(t *testing.T) []gofakes3.VersionedBackend {
	t.Helper()

	// Use in-memory filesystems for testing to avoid persistence issues
	// Use separate filesystems for data and metadata to avoid conflicts
	singleDataFs := afero.NewMemMapFs()
	singleMetaFs := afero.NewMemMapFs()
	single, err := SingleBucket("test", singleDataFs, singleMetaFs)
	if err != nil {
		t.Fatal(err)
	}

	multiFs := afero.NewMemMapFs()
	// multiFs := afero.NewBasePathFs(afero.NewOsFs(), fmt.Sprintf("tmpdata/%s-%d", t.Name(), time.Now().Unix()))
	multi, err := MultiBucket(multiFs)
	if err != nil {
		t.Fatal(err)
	}
	if err := multi.CreateBucket("test"); err != nil {
		t.Fatal(err)
	}

	backends := []gofakes3.VersionedBackend{single, multi}
	return backends
}

func TestAferoVersioningBasic(t *testing.T) {
	backends := versionedTestingBackends(t)

	for _, backend := range backends {
		t.Run(fmt.Sprintf("%T", backend), func(t *testing.T) {
			// Initially, versioning should be disabled
			config, err := backend.VersioningConfiguration("test")
			if err != nil {
				t.Fatal(err)
			}
			if config.Status != gofakes3.VersioningNone {
				t.Fatalf("expected VersioningNone, got %q", config.Status)
			}

			// Enable versioning
			err = backend.SetVersioningConfiguration("test", gofakes3.VersioningConfiguration{
				Status: gofakes3.VersioningEnabled,
			})
			if err != nil {
				t.Fatal(err)
			}

			// Verify versioning is enabled
			config, err = backend.VersioningConfiguration("test")
			if err != nil {
				t.Fatal(err)
			}
			if config.Status != gofakes3.VersioningEnabled {
				t.Fatalf("expected VersioningEnabled, got %q", config.Status)
			}

			// Suspend versioning
			err = backend.SetVersioningConfiguration("test", gofakes3.VersioningConfiguration{
				Status: gofakes3.VersioningSuspended,
			})
			if err != nil {
				t.Fatal(err)
			}

			// Verify versioning is suspended
			config, err = backend.VersioningConfiguration("test")
			if err != nil {
				t.Fatal(err)
			}
			if config.Status != gofakes3.VersioningSuspended {
				t.Fatalf("expected VersioningSuspended, got %q", config.Status)
			}
		})
	}
}

func TestAferoObjectVersions(t *testing.T) {
	backends := versionedTestingBackends(t)

	for _, backend := range backends {
		t.Run(fmt.Sprintf("%T", backend), func(t *testing.T) {
			// Enable versioning
			err := backend.SetVersioningConfiguration("test", gofakes3.VersioningConfiguration{
				Status: gofakes3.VersioningEnabled,
			})
			if err != nil {
				t.Fatal(err)
			}

			// Create first version
			contents1 := []byte("version 1 content")
			result1, err := backend.(gofakes3.Backend).PutObject("test", "object", map[string]string{}, bytes.NewReader(contents1), int64(len(contents1)), nil)
			if err != nil {
				t.Fatal(err)
			}
			if result1.VersionID == "" {
				t.Fatal("expected version ID for first version")
			}
			v1 := result1.VersionID
			t.Logf("Version 1 ID: %s", v1)

			// Create second version
			contents2 := []byte("version 2 content")
			result2, err := backend.(gofakes3.Backend).PutObject("test", "object", map[string]string{}, bytes.NewReader(contents2), int64(len(contents2)), nil)
			if err != nil {
				t.Fatal(err)
			}
			if result2.VersionID == "" {
				t.Fatal("expected version ID for second version")
			}
			if result2.VersionID == v1 {
				t.Fatal("version IDs should be different")
			}
			v2 := result2.VersionID
			t.Logf("Version 2 ID: %s", v2)

			// Create third version
			contents3 := []byte("version 3 content")
			result3, err := backend.(gofakes3.Backend).PutObject("test", "object", map[string]string{}, bytes.NewReader(contents3), int64(len(contents3)), nil)
			if err != nil {
				t.Fatal(err)
			}
			if result3.VersionID == "" {
				t.Fatal("expected version ID for third version")
			}
			v3 := result3.VersionID
			t.Logf("Version 3 ID: %s", v3)

			// Get current version (should be version 3)
			obj, err := backend.(gofakes3.Backend).GetObject("test", "object", nil)
			if err != nil {
				t.Fatal(err)
			}
			body, err := ioutil.ReadAll(obj.Contents)
			obj.Contents.Close()
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(body, contents3) {
				t.Fatalf("expected %q, got %q", contents3, body)
			}

			// CRITICAL TEST: Verify old versions are still accessible after new versions created
			// Get version 1 (the oldest version, should still be accessible)
			t.Logf("Verifying old version 1 is accessible: %q", v1)
			obj1, err := backend.GetObjectVersion("test", "object", v1, nil)
			if err != nil {
				t.Fatalf("failed to get old version 1 (%q): %v", v1, err)
			}
			body1, err := ioutil.ReadAll(obj1.Contents)
			obj1.Contents.Close()
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(body1, contents1) {
				t.Fatalf("version 1 content mismatch: expected %q, got %q", contents1, body1)
			}
			t.Logf("✓ Version 1 accessible and content correct")

			// Get version 2 (middle version, should also be accessible)
			t.Logf("Verifying old version 2 is accessible: %q", v2)
			obj2, err := backend.GetObjectVersion("test", "object", v2, nil)
			if err != nil {
				t.Fatalf("failed to get old version 2 (%q): %v", v2, err)
			}
			body2, err := ioutil.ReadAll(obj2.Contents)
			obj2.Contents.Close()
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(body2, contents2) {
				t.Fatalf("version 2 content mismatch: expected %q, got %q", contents2, body2)
			}
			t.Logf("✓ Version 2 accessible and content correct")

			// List versions
			versions, err := backend.ListBucketVersions("test", nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			if len(versions.Versions) != 3 {
				t.Fatalf("expected 3 versions, got %d", len(versions.Versions))
			}

			// Delete version 1
			_, err = backend.DeleteObjectVersion("test", "object", v1)
			if err != nil {
				t.Fatal(err)
			}

			// Verify version 1 is gone
			_, err = backend.GetObjectVersion("test", "object", v1, nil)
			if err == nil {
				t.Fatal("expected error when getting deleted version")
			}

			// List versions again (should have 2)
			versions, err = backend.ListBucketVersions("test", nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			if len(versions.Versions) != 2 {
				t.Fatalf("expected 2 versions after delete, got %d", len(versions.Versions))
			}

			// Test delete marker
			deleteResult, err := backend.(gofakes3.Backend).DeleteObject("test", "object")
			if err != nil {
				t.Fatal(err)
			}
			if !deleteResult.IsDeleteMarker {
				t.Fatal("expected delete marker")
			}
			if deleteResult.VersionID == "" {
				t.Fatal("expected version ID for delete marker")
			}

			// Getting current object should fail (delete marker)
			_, err = backend.(gofakes3.Backend).GetObject("test", "object", nil)
			if err == nil {
				t.Fatal("expected error when getting object with delete marker")
			}

			// But we can still get old versions
			obj2again, err := backend.GetObjectVersion("test", "object", v2, nil)
			if err != nil {
				t.Fatal(err)
			}
			body2again, err := ioutil.ReadAll(obj2again.Contents)
			obj2again.Contents.Close()
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(body2again, contents2) {
				t.Fatalf("expected %q, got %q", contents2, body2again)
			}
		})
	}
}

func TestAferoExternalFileWithVersioning(t *testing.T) {
	backends := versionedTestingBackends(t)

	for _, backend := range backends {
		t.Run(fmt.Sprintf("%T", backend), func(t *testing.T) {
			// Enable versioning
			err := backend.SetVersioningConfiguration("test", gofakes3.VersioningConfiguration{
				Status: gofakes3.VersioningEnabled,
			})
			if err != nil {
				t.Fatal(err)
			}

			// Simulate an external file by writing directly to the filesystem
			var fs afero.Fs
			switch b := backend.(type) {
			case *SingleBucketBackend:
				fs = b.fs
			case *MultiBucketBackend:
				fs = b.bucketFs
			}

			externalContents := []byte("external file content")
			var filePath string
			switch backend.(type) {
			case *SingleBucketBackend:
				filePath = "external-file"
			case *MultiBucketBackend:
				filePath = "test/external-file"
			}

			err = afero.WriteFile(fs, filePath, externalContents, 0666)
			if err != nil {
				t.Fatal(err)
			}

			// Now read the file through the backend - it should auto-generate metadata and version ID
			obj, err := backend.(gofakes3.Backend).GetObject("test", "external-file", nil)
			if err != nil {
				t.Fatal(err)
			}
			body, err := ioutil.ReadAll(obj.Contents)
			obj.Contents.Close()
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(body, externalContents) {
				t.Fatalf("expected %q, got %q", externalContents, body)
			}

			// Check that a version ID was assigned
			if obj.VersionID == "" {
				t.Fatal("expected version ID to be auto-generated for external file")
			}

			// Now overwrite the file through PutObject
			newContents := []byte("updated via PutObject")
			result, err := backend.(gofakes3.Backend).PutObject("test", "external-file", map[string]string{}, bytes.NewReader(newContents), int64(len(newContents)), nil)
			if err != nil {
				t.Fatal(err)
			}
			if result.VersionID == "" {
				t.Fatal("expected version ID for new version")
			}

			// The external file should now be a version
			oldVersion, err := backend.GetObjectVersion("test", "external-file", obj.VersionID, nil)
			if err != nil {
				t.Fatalf("failed to get old version of external file: %v", err)
			}
			oldBody, err := ioutil.ReadAll(oldVersion.Contents)
			oldVersion.Contents.Close()
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(oldBody, externalContents) {
				t.Fatalf("expected old version to contain %q, got %q", externalContents, oldBody)
			}

			// Current version should have new contents
			current, err := backend.(gofakes3.Backend).GetObject("test", "external-file", nil)
			if err != nil {
				t.Fatal(err)
			}
			currentBody, err := ioutil.ReadAll(current.Contents)
			current.Contents.Close()
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(currentBody, newContents) {
				t.Fatalf("expected current version to contain %q, got %q", newContents, currentBody)
			}
		})
	}
}

func TestListBucketPagination(t *testing.T) {
	backends := testingBackends(t)

	for _, backend := range backends {
		t.Run(fmt.Sprintf("%T", backend), func(t *testing.T) {
			// Create 10 objects
			for i := 0; i < 10; i++ {
				key := fmt.Sprintf("object-%02d", i)
				contents := []byte("test")
				_, err := backend.PutObject("test", key, nil, bytes.NewReader(contents), int64(len(contents)), nil)
				if err != nil {
					t.Fatal(err)
				}
			}

			// Test 1: List all objects without pagination
			t.Run("list-all", func(t *testing.T) {
				prefix := &gofakes3.Prefix{}
				result, err := backend.ListBucket("test", prefix, gofakes3.ListBucketPage{})
				if err != nil {
					t.Fatal(err)
				}
				if len(result.Contents) != 10 {
					t.Fatalf("expected 10 objects, got %d", len(result.Contents))
				}
				if result.IsTruncated {
					t.Fatal("expected IsTruncated=false when listing all objects")
				}
			})

			// Test 2: List with MaxKeys=3
			t.Run("list-with-maxkeys", func(t *testing.T) {
				prefix := &gofakes3.Prefix{}
				result, err := backend.ListBucket("test", prefix, gofakes3.ListBucketPage{MaxKeys: 3})
				if err != nil {
					t.Fatal(err)
				}
				if len(result.Contents) != 3 {
					t.Fatalf("expected 3 objects, got %d", len(result.Contents))
				}
				if !result.IsTruncated {
					t.Fatal("expected IsTruncated=true when MaxKeys < total objects")
				}
				if result.NextMarker == "" {
					t.Fatal("expected NextMarker to be set when truncated")
				}
				// First page should contain object-00, object-01, object-02
				if result.Contents[0].Key != "object-00" {
					t.Fatalf("expected first key to be object-00, got %s", result.Contents[0].Key)
				}
				if result.Contents[2].Key != "object-02" {
					t.Fatalf("expected third key to be object-02, got %s", result.Contents[2].Key)
				}
			})

			// Test 3: Use marker to get next page
			t.Run("list-with-marker", func(t *testing.T) {
				prefix := &gofakes3.Prefix{}
				// Get first page
				firstPage, err := backend.ListBucket("test", prefix, gofakes3.ListBucketPage{MaxKeys: 3})
				if err != nil {
					t.Fatal(err)
				}

				// Get second page using marker
				secondPage, err := backend.ListBucket("test", prefix, gofakes3.ListBucketPage{
					MaxKeys:   3,
					Marker:    firstPage.NextMarker,
					HasMarker: true,
				})
				if err != nil {
					t.Fatal(err)
				}
				if len(secondPage.Contents) != 3 {
					t.Fatalf("expected 3 objects in second page, got %d", len(secondPage.Contents))
				}
				// Second page should contain object-03, object-04, object-05
				if secondPage.Contents[0].Key != "object-03" {
					t.Fatalf("expected first key in second page to be object-03, got %s", secondPage.Contents[0].Key)
				}
				if !secondPage.IsTruncated {
					t.Fatal("expected second page to be truncated")
				}
			})

			// Test 4: Iterate through all pages
			t.Run("iterate-all-pages", func(t *testing.T) {
				prefix := &gofakes3.Prefix{}
				var allKeys []string
				var marker string
				hasMarker := false

				for {
					result, err := backend.ListBucket("test", prefix, gofakes3.ListBucketPage{
						MaxKeys:   3,
						Marker:    marker,
						HasMarker: hasMarker,
					})
					if err != nil {
						t.Fatal(err)
					}

					for _, obj := range result.Contents {
						allKeys = append(allKeys, obj.Key)
					}

					if !result.IsTruncated {
						break
					}

					marker = result.NextMarker
					hasMarker = true
				}

				if len(allKeys) != 10 {
					t.Fatalf("expected to collect 10 objects across pages, got %d", len(allKeys))
				}

				// Verify all keys are present
				for i := 0; i < 10; i++ {
					expectedKey := fmt.Sprintf("object-%02d", i)
					if allKeys[i] != expectedKey {
						t.Fatalf("expected key %s at position %d, got %s", expectedKey, i, allKeys[i])
					}
				}
			})

			// Clean up
			for i := 0; i < 10; i++ {
				key := fmt.Sprintf("object-%02d", i)
				backend.DeleteObject("test", key)
			}
		})
	}
}

func TestListBucketPaginationWithPrefix(t *testing.T) {
	backends := testingBackends(t)

	for _, backend := range backends {
		t.Run(fmt.Sprintf("%T", backend), func(t *testing.T) {
			// Create objects with prefixes
			for i := 0; i < 5; i++ {
				key := fmt.Sprintf("prefix/object-%02d", i)
				contents := []byte("test")
				_, err := backend.PutObject("test", key, nil, bytes.NewReader(contents), int64(len(contents)), nil)
				if err != nil {
					t.Fatal(err)
				}
			}

			// Test pagination with file prefix
			t.Run("file-prefix-pagination", func(t *testing.T) {
				prefix := &gofakes3.Prefix{Prefix: "prefix/", HasPrefix: true, Delimiter: "/", HasDelimiter: true}
				result, err := backend.ListBucket("test", prefix, gofakes3.ListBucketPage{MaxKeys: 2})
				if err != nil {
					t.Fatal(err)
				}
				if len(result.Contents) != 2 {
					t.Fatalf("expected 2 objects, got %d", len(result.Contents))
				}
				if !result.IsTruncated {
					t.Fatal("expected IsTruncated=true")
				}

				// Get next page
				result, err = backend.ListBucket("test", prefix, gofakes3.ListBucketPage{
					MaxKeys:   2,
					Marker:    result.NextMarker,
					HasMarker: true,
				})
				if err != nil {
					t.Fatal(err)
				}
				if len(result.Contents) != 2 {
					t.Fatalf("expected 2 objects in second page, got %d", len(result.Contents))
				}
			})

			// Clean up
			for i := 0; i < 5; i++ {
				key := fmt.Sprintf("prefix/object-%02d", i)
				backend.DeleteObject("test", key)
			}
		})
	}
}

func TestListBucketEmptyPrefix(t *testing.T) {
	backends := testingBackends(t)

	for _, backend := range backends {
		t.Run(fmt.Sprintf("%T", backend), func(t *testing.T) {
			// Create one object at root
			contents := []byte("test")
			_, err := backend.PutObject("test", "root-object", nil, bytes.NewReader(contents), int64(len(contents)), nil)
			if err != nil {
				t.Fatal(err)
			}

			// Test 1: List non-existent prefix path (should return empty, not error)
			t.Run("nonexistent-prefix", func(t *testing.T) {
				prefix := &gofakes3.Prefix{Prefix: "nonexistent/", HasPrefix: true}
				result, err := backend.ListBucket("test", prefix, gofakes3.ListBucketPage{})
				if err != nil {
					t.Fatalf("expected no error for non-existent prefix, got: %v", err)
				}
				if len(result.Contents) != 0 {
					t.Fatalf("expected 0 objects for non-existent prefix, got %d", len(result.Contents))
				}
			})

			// Test 2: List non-existent file prefix path (should return empty, not error)
			t.Run("nonexistent-file-prefix", func(t *testing.T) {
				prefix := &gofakes3.Prefix{Prefix: "nonexistent/", HasPrefix: true, Delimiter: "/", HasDelimiter: true}
				result, err := backend.ListBucket("test", prefix, gofakes3.ListBucketPage{})
				if err != nil {
					t.Fatalf("expected no error for non-existent file prefix, got: %v", err)
				}
				if len(result.Contents) != 0 {
					t.Fatalf("expected 0 objects for non-existent file prefix, got %d", len(result.Contents))
				}
			})

			// Clean up
			backend.DeleteObject("test", "root-object")
		})
	}
}
