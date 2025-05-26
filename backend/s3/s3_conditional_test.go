//go:build integration
// +build integration

package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/fstest"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/fs/object"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testS3ConditionalUploads is a helper function containing the conditional upload tests.
// It's called by an exported test function registered with fstests.
func testS3ConditionalUploads(t *testing.T, rt *fstests.RunTests) {
	f := rt.Fremote
	ctx := context.Background()
	remoteName := "s3_cond_test-" + rt.CaseName + ".txt"
	remoteNameNew := "s3_cond_test_new-" + rt.CaseName + ".txt"
	content1 := "initial content for " + rt.CaseName
	content2 := "updated content for " + rt.CaseName

	// getS3ETag attempts to retrieve the raw ETag for an S3 object.
	// S3 ETags are quoted (e.g., "\"md5hex\"" or "\"multipart-etag-maybe-with-hyphen\"").
	// The fs.Object.Hash(hash.ETag) method is the ideal way to get this.
	// If it returns empty or an error, it implies the backend or fs.Object implementation
	// doesn't fully support exposing the raw ETag via this hash type.
	getS3ETag := func(obj fs.Object) string {
		// Try hash.ETag first, which should be the correct type for non-MD5 ETags.
		etag, err := obj.Hash(ctx, hash.ETag)
		if err == nil && etag != "" {
			return etag
		}
		// If ETag hash type failed or returned empty, log it.
		// This might indicate a limitation in the backend's Hash implementation for ETag.
		if err != nil {
			t.Logf("Warning: obj.Hash(ctx, hash.ETag) for %q failed: %v. Test reliability for ETag conditions might be affected.", obj.Remote(), err)
		} else if etag == "" {
			t.Logf("Warning: obj.Hash(ctx, hash.ETag) for %q returned empty. Test reliability for ETag conditions might be affected.", obj.Remote())
		}
		// As a fallback for S3, if the ETag is an MD5 sum, it might be available via hash.MD5.
		// S3 ETags that are MD5 sums are typically returned quoted by the API.
		// The fs.IfMatchOption requires the ETag string as returned by S3, including quotes.
		md5sum, md5Err := obj.Hash(ctx, hash.MD5)
		if md5Err == nil && md5sum != "" {
			// If we get a plain MD5, quote it, as S3 ETags are quoted.
			// This is an assumption based on common S3 behavior for MD5-based ETags.
			return `"` + md5sum + `"`
		}
		t.Logf("Warning: Could not retrieve a definitive ETag for %q. MD5 fallback also failed or was empty (md5Err: %v).", obj.Remote(), md5Err)
		return "" // Return empty if no reliable ETag can be found.
	}

	// Initial cleanup, then ensure cleanup happens after tests.
	rt.Purge()
	t.Cleanup(rt.Purge)

	t.Run("IfMatch", func(t *testing.T) {
		rt.Purge() // Clean before this specific sub-test
		// 1. Setup: Upload initial object
		initialObj := fstest.PutString(ctx, t, f, remoteName, content1)
		initialEtag := getS3ETag(initialObj)
		require.NotEmpty(t, initialEtag, "Initial ETag should not be empty for IfMatch tests")

		// 2. Attempt overwrite with correct ETag (should succeed)
		_, err := f.Put(ctx, strings.NewReader(content2), object.NewStaticObjectInfo(remoteName, time.Now(), int64(len(content2)), true, nil, f), fs.IfMatchOption(initialEtag))
		assert.NoError(t, err, "Put with correct If-Match ETag should succeed")
		fstest.CheckObject(ctx, t, f, remoteName, content2, initialObj.ModTime(ctx)) // ModTime might be preserved or updated by S3

		// 3. Get new ETag after successful overwrite
		objUpdated, err := f.NewObject(ctx, remoteName)
		require.NoError(t, err)
		updatedEtag := getS3ETag(objUpdated)
		require.NotEmpty(t, updatedEtag, "Updated ETag should not be empty")
		if content1 != content2 { // ETags should differ if content differs
			assert.NotEqual(t, initialEtag, updatedEtag, "ETag should change after successful overwrite with different content")
		}

		// 4. Attempt overwrite with stale ETag (should fail)
		_, err = f.Put(ctx, strings.NewReader(content1), object.NewStaticObjectInfo(remoteName, time.Now(), int64(len(content1)), true, nil, f), fs.IfMatchOption(initialEtag))
		assert.Error(t, err, "Put with stale If-Match ETag should fail")
		require.True(t, errors.Is(err, fs.ErrorPreconditionFailed), "Error should be fs.ErrorPreconditionFailed for stale If-Match, got: %v", err)
		fstest.CheckObject(ctx, t, f, remoteName, content2, objUpdated.ModTime(ctx))

		// 5. Attempt overwrite with a completely wrong/non-existent ETag (should fail)
		_, err = f.Put(ctx, strings.NewReader(content1), object.NewStaticObjectInfo(remoteName, time.Now(), int64(len(content1)), true, nil, f), fs.IfMatchOption("\"random-nonexistent-etag\""))
		assert.Error(t, err, "Put with wrong If-Match ETag should fail")
		require.True(t, errors.Is(err, fs.ErrorPreconditionFailed), "Error should be fs.ErrorPreconditionFailed for wrong If-Match, got: %v", err)
		fstest.CheckObject(ctx, t, f, remoteName, content2, objUpdated.ModTime(ctx))
	})

	rt.Purge()

	t.Run("IfNoneMatch", func(t *testing.T) {
		rt.Purge()
		// 1. Attempt to write a new object (remoteNameNew) with IfNoneMatchOption("*") (should succeed)
		_, err := f.Put(ctx, strings.NewReader(content1), object.NewStaticObjectInfo(remoteNameNew, time.Now(), int64(len(content1)), true, nil, f), fs.IfNoneMatchOption("*"))
		assert.NoError(t, err, "Put new object with If-None-Match: * should succeed")
		objNew := fstest.CheckObject(ctx, t, f, remoteNameNew, content1, time.Now()) // ModTime check is approximate

		// 2. Get ETag of the newly created object
		newEtag := getS3ETag(objNew)
		require.NotEmpty(t, newEtag, "ETag for new object should not be empty for IfNoneMatch tests")

		// 3. Attempt to overwrite existing object (remoteNameNew) with IfNoneMatchOption(<correct_etag>) (should fail)
		_, err = f.Put(ctx, strings.NewReader(content2), object.NewStaticObjectInfo(remoteNameNew, time.Now(), int64(len(content2)), true, nil, f), fs.IfNoneMatchOption(newEtag))
		assert.Error(t, err, "Put with If-None-Match ETag matching existing object should fail")
		require.True(t, errors.Is(err, fs.ErrorPreconditionFailed), "Error should be fs.ErrorPreconditionFailed, got: %v", err)
		fstest.CheckObject(ctx, t, f, remoteNameNew, content1, objNew.ModTime(ctx))

		// 4. Attempt to overwrite existing object (remoteNameNew) with IfNoneMatchOption("random-non-matching-etag") (should succeed)
		_, err = f.Put(ctx, strings.NewReader(content2), object.NewStaticObjectInfo(remoteNameNew, time.Now(), int64(len(content2)), true, nil, f), fs.IfNoneMatchOption("\"random-non-matching-etag\""))
		assert.NoError(t, err, "Put with If-None-Match ETag not matching existing object should succeed")
		objUpdated := fstest.CheckObject(ctx, t, f, remoteNameNew, content2, time.Now()) // ModTime check is approximate

		// 5. Attempt to overwrite existing object (remoteNameNew, now content2) with IfNoneMatchOption("*") (should fail)
		_, err = f.Put(ctx, strings.NewReader(content1), object.NewStaticObjectInfo(remoteNameNew, time.Now(), int64(len(content1)), true, nil, f), fs.IfNoneMatchOption("*"))
		assert.Error(t, err, "Put existing object with If-None-Match: * should fail")
		require.True(t, errors.Is(err, fs.ErrorPreconditionFailed), "Error should be fs.ErrorPreconditionFailed, got: %v", err)
		fstest.CheckObject(ctx, t, f, remoteNameNew, content2, objUpdated.ModTime(ctx))
	})

	rt.Purge()

	t.Run("IfModifiedSince", func(t *testing.T) {
		rt.Purge()
		// 1. Setup: Upload initial object
		initialObj := fstest.PutString(ctx, t, f, remoteName, content1)
		modTime := initialObj.ModTime(ctx).Truncate(time.Second) // Truncate for reliable comparison

		// 2. Attempt overwrite with IfModifiedSince(modTime - 1 hour) (object *was* modified after this time, so PUT should proceed)
		_, err := f.Put(ctx, strings.NewReader(content2), object.NewStaticObjectInfo(remoteName, time.Now(), int64(len(content2)), true, nil, f), fs.IfModifiedSinceOption(modTime.Add(-time.Hour)))
		assert.NoError(t, err, "Put with If-Modified-Since (object modified after this time) should succeed")
		objUpdated := fstest.CheckObject(ctx, t, f, remoteName, content2, time.Now())

		// 3. Update ModTime reference
		currentModTime := objUpdated.ModTime(ctx).Truncate(time.Second)

		// 4. Attempt overwrite with IfModifiedSince(currentModTime + 1 hour) (object was *not* modified after this time, so PUT should fail)
		_, err = f.Put(ctx, strings.NewReader(content1), object.NewStaticObjectInfo(remoteName, time.Now(), int64(len(content1)), true, nil, f), fs.IfModifiedSinceOption(currentModTime.Add(time.Hour)))
		assert.Error(t, err, "Put with If-Modified-Since (object not modified after this time) should fail")
		require.True(t, errors.Is(err, fs.ErrorPreconditionFailed), "Error should be fs.ErrorPreconditionFailed, got: %v", err)
		fstest.CheckObject(ctx, t, f, remoteName, content2, currentModTime)
		
		// 5. Attempt overwrite with IfModifiedSince(currentModTime) (object was *not* modified after this time (it was modified *at* this time), so PUT should fail)
		_, err = f.Put(ctx, strings.NewReader(content1), object.NewStaticObjectInfo(remoteName, time.Now(), int64(len(content1)), true, nil, f), fs.IfModifiedSinceOption(currentModTime))
		assert.Error(t, err, "Put with If-Modified-Since (object modified at this exact time) should fail")
		require.True(t, errors.Is(err, fs.ErrorPreconditionFailed), "Error should be fs.ErrorPreconditionFailed, got: %v", err)
		fstest.CheckObject(ctx, t, f, remoteName, content2, currentModTime)
	})
	
	rt.Purge()

	t.Run("IfUnmodifiedSince", func(t *testing.T) {
		rt.Purge()
		// 1. Setup: Upload initial object
		initialObj := fstest.PutString(ctx, t, f, remoteName, content1)
		modTime := initialObj.ModTime(ctx).Truncate(time.Second)

		// 2. Attempt overwrite with IfUnmodifiedSince(modTime + 1 hour) (object *was* unmodified since this time, so PUT should succeed)
		_, err := f.Put(ctx, strings.NewReader(content2), object.NewStaticObjectInfo(remoteName, time.Now(), int64(len(content2)), true, nil, f), fs.IfUnmodifiedSinceOption(modTime.Add(time.Hour)))
		assert.NoError(t, err, "Put with If-Unmodified-Since (object unmodified since this time) should succeed")
		objUpdated := fstest.CheckObject(ctx, t, f, remoteName, content2, time.Now())

		// 3. Update ModTime reference
		currentModTime := objUpdated.ModTime(ctx).Truncate(time.Second)

		// 4. Attempt overwrite with IfUnmodifiedSince(currentModTime - 1 hour) (object *was* modified after this time (i.e. it's not unmodified since the condition time), so PUT should fail)
		_, err = f.Put(ctx, strings.NewReader(content1), object.NewStaticObjectInfo(remoteName, time.Now(), int64(len(content1)), true, nil, f), fs.IfUnmodifiedSinceOption(currentModTime.Add(-time.Hour)))
		assert.Error(t, err, "Put with If-Unmodified-Since (object modified after this time) should fail")
		require.True(t, errors.Is(err, fs.ErrorPreconditionFailed), "Error should be fs.ErrorPreconditionFailed, got: %v", err)
		fstest.CheckObject(ctx, t, f, remoteName, content2, currentModTime)

		// 5. Attempt overwrite with IfUnmodifiedSince(currentModTime) (object *was* unmodified since this time (modified at this time), so PUT should succeed)
		_, err = f.Put(ctx, strings.NewReader(content1), object.NewStaticObjectInfo(remoteName, time.Now(), int64(len(content1)), true, nil, f), fs.IfUnmodifiedSinceOption(currentModTime))
		assert.NoError(t, err, "Put with If-Unmodified-Since (object modified at this exact time) should succeed")
		fstest.CheckObject(ctx, t, f, remoteName, content1, time.Now())
	})
}

// This function needs to be registered with fstests to be run.
func TestCustomS3ConditionalUploads(t *testing.T) {
	fstests.Run(t, &fstests.Opt{
		RemoteName: "TestS3:", // Ensure this matches your S3 test remote name
		NilObject:  (*Object)(nil),
		ShortTestNames: map[string]string{
			// This name should be unique and descriptive
			"TestS3ConditionalUploads": "TestS3ConditionalUploads",
		},
		CustomTestFunctions: map[string]fstests.CustomTestFunction{
			// The key here is the short name defined above
			"TestS3ConditionalUploads": testS3ConditionalUploads,
		},
		// Skip other standard tests if only focusing on conditional uploads for this run
		// Or, let them run if you want full integration testing.
		SkipObjectTests:    true,
		SkipDirectoryTests: true,
		SkipFsMatchChecks:  true, // We are testing specific S3 behaviors, not generic Fs matching
	})
}
