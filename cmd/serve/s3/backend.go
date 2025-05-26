// Package s3 implements an s3 server for rclone
package s3

import (
	"context"
	"encoding/hex"
	"io"
	"maps"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/ncw/swift/v2"
	"github.com/rclone/gofakes3"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/vfs"
)

var (
	emptyPrefix = &gofakes3.Prefix{}
)

// s3Backend implements the gofacess3.Backend interface to make an S3
// backend for gofakes3
type s3Backend struct {
	s    *Server
	meta *sync.Map
}

// newBackend creates a new SimpleBucketBackend.
func newBackend(s *Server) gofakes3.Backend {
	return &s3Backend{
		s:    s,
		meta: new(sync.Map),
	}
}

// ListBuckets always returns the default bucket.
func (b *s3Backend) ListBuckets(ctx context.Context) ([]gofakes3.BucketInfo, error) {
	_vfs, err := b.s.getVFS(ctx)
	if err != nil {
		return nil, err
	}
	dirEntries, err := getDirEntries("/", _vfs)
	if err != nil {
		return nil, err
	}
	var response []gofakes3.BucketInfo
	for _, entry := range dirEntries {
		if entry.IsDir() {
			response = append(response, gofakes3.BucketInfo{
				Name:         entry.Name(),
				CreationDate: gofakes3.NewContentTime(entry.ModTime()),
			})
		}
		// FIXME: handle files in root dir
	}

	return response, nil
}

// ListBucket lists the objects in the given bucket.
func (b *s3Backend) ListBucket(ctx context.Context, bucket string, prefix *gofakes3.Prefix, page gofakes3.ListBucketPage) (*gofakes3.ObjectList, error) {
	_vfs, err := b.s.getVFS(ctx)
	if err != nil {
		return nil, err
	}
	_, err = _vfs.Stat(bucket)
	if err != nil {
		return nil, gofakes3.BucketNotFound(bucket)
	}
	if prefix == nil {
		prefix = emptyPrefix
	}

	// workaround
	if strings.TrimSpace(prefix.Prefix) == "" {
		prefix.HasPrefix = false
	}
	if strings.TrimSpace(prefix.Delimiter) == "" {
		prefix.HasDelimiter = false
	}

	response := gofakes3.NewObjectList()
	path, remaining := prefixParser(prefix)

	err = b.entryListR(_vfs, bucket, path, remaining, prefix.HasDelimiter, response)
	if err == gofakes3.ErrNoSuchKey {
		// AWS just returns an empty list
		response = gofakes3.NewObjectList()
	} else if err != nil {
		return nil, err
	}

	return b.pager(response, page)
}

// formatHeaderTime makes an timestamp which is the same as that used by AWS.
//
// This is like RFC1123 always in UTC, but has GMT instead of UTC
func formatHeaderTime(t time.Time) string {
	return t.UTC().Format("Mon, 02 Jan 2006 15:04:05") + " GMT"
}

// HeadObject returns the fileinfo for the given object name.
//
// Note that the metadata is not supported yet.
func (b *s3Backend) HeadObject(ctx context.Context, bucketName, objectName string) (*gofakes3.Object, error) {
	_vfs, err := b.s.getVFS(ctx)
	if err != nil {
		return nil, err
	}
	_, err = _vfs.Stat(bucketName)
	if err != nil {
		return nil, gofakes3.BucketNotFound(bucketName)
	}

	fp := path.Join(bucketName, objectName)
	node, err := _vfs.Stat(fp)
	if err != nil {
		return nil, gofakes3.KeyNotFound(objectName)
	}

	if !node.IsFile() {
		return nil, gofakes3.KeyNotFound(objectName)
	}

	entry := node.DirEntry()
	if entry == nil {
		return nil, gofakes3.KeyNotFound(objectName)
	}

	fobj := entry.(fs.Object)
	size := node.Size()
	hash := getFileHashByte(fobj, b.s.etagHashType)

	meta := map[string]string{
		"Last-Modified": formatHeaderTime(node.ModTime()),
		"Content-Type":  fs.MimeType(context.Background(), fobj),
	}

	if val, ok := b.meta.Load(fp); ok {
		metaMap := val.(map[string]string)
		maps.Copy(meta, metaMap)
	}

	return &gofakes3.Object{
		Name:     objectName,
		Hash:     hash,
		Metadata: meta,
		Size:     size,
		Contents: noOpReadCloser{},
	}, nil
}

// GetObject fetches the object from the filesystem.
func (b *s3Backend) GetObject(ctx context.Context, bucketName, objectName string, rangeRequest *gofakes3.ObjectRangeRequest) (obj *gofakes3.Object, err error) {
	_vfs, err := b.s.getVFS(ctx)
	if err != nil {
		return nil, err
	}
	_, err = _vfs.Stat(bucketName)
	if err != nil {
		return nil, gofakes3.BucketNotFound(bucketName)
	}

	fp := path.Join(bucketName, objectName)
	node, err := _vfs.Stat(fp)
	if err != nil {
		return nil, gofakes3.KeyNotFound(objectName)
	}

	if !node.IsFile() {
		return nil, gofakes3.KeyNotFound(objectName)
	}

	entry := node.DirEntry()
	if entry == nil {
		return nil, gofakes3.KeyNotFound(objectName)
	}

	fobj := entry.(fs.Object)
	file := node.(*vfs.File)

	size := node.Size()
	hash := getFileHashByte(fobj, b.s.etagHashType)

	in, err := file.Open(os.O_RDONLY)
	if err != nil {
		return nil, gofakes3.ErrInternal
	}
	defer func() {
		// If an error occurs, the caller may not have access to Object.Body in order to close it:
		if err != nil {
			_ = in.Close()
		}
	}()

	var rdr io.ReadCloser = in
	rnge, err := rangeRequest.Range(size)
	if err != nil {
		return nil, err
	}

	if rnge != nil {
		if _, err := in.Seek(rnge.Start, io.SeekStart); err != nil {
			return nil, err
		}
		rdr = limitReadCloser(rdr, in.Close, rnge.Length)
	}

	meta := map[string]string{
		"Last-Modified": formatHeaderTime(node.ModTime()),
		"Content-Type":  fs.MimeType(context.Background(), fobj),
	}

	if val, ok := b.meta.Load(fp); ok {
		metaMap := val.(map[string]string)
		maps.Copy(meta, metaMap)
	}

	return &gofakes3.Object{
		Name:     objectName,
		Hash:     hash,
		Metadata: meta,
		Size:     size,
		Range:    rnge,
		Contents: rdr,
	}, nil
}

// storeModtime sets both "mtime" and "X-Amz-Meta-Mtime" to val in b.meta.
// Call this whenever modtime is updated.
func (b *s3Backend) storeModtime(fp string, meta map[string]string, val string) {
	meta["X-Amz-Meta-Mtime"] = val
	meta["mtime"] = val
	b.meta.Store(fp, meta)
}

// TouchObject creates or updates meta on specified object.
func (b *s3Backend) TouchObject(ctx context.Context, fp string, meta map[string]string) (result gofakes3.PutObjectResult, err error) {
	_vfs, err := b.s.getVFS(ctx)
	if err != nil {
		return result, err
	}
	_, err = _vfs.Stat(fp)
	if err == vfs.ENOENT {
		f, err := _vfs.Create(fp)
		if err != nil {
			return result, err
		}
		_ = f.Close()
		return b.TouchObject(ctx, fp, meta)
	} else if err != nil {
		return result, err
	}

	_, err = _vfs.Stat(fp)
	if err != nil {
		return result, err
	}

	b.meta.Store(fp, meta)

	if val, ok := meta["X-Amz-Meta-Mtime"]; ok {
		ti, err := swift.FloatStringToTime(val)
		if err == nil {
			b.storeModtime(fp, meta, val)
			return result, _vfs.Chtimes(fp, ti, ti)
		}
		// ignore error since the file is successfully created
	}

	if val, ok := meta["mtime"]; ok {
		ti, err := swift.FloatStringToTime(val)
		if err == nil {
			b.storeModtime(fp, meta, val)
			return result, _vfs.Chtimes(fp, ti, ti)
		}
		// ignore error since the file is successfully created
	}

	return result, nil
}

// PutObject creates or overwrites the object with the given name.
func (b *s3Backend) PutObject(
	ctx context.Context,
	bucketName, objectName string,
	meta map[string]string,
	input io.Reader, size int64,
) (result gofakes3.PutObjectResult, err error) {
	_vfs, err := b.s.getVFS(ctx)
	if err != nil {
		return result, err
	}
	_, err = _vfs.Stat(bucketName)
	if err != nil {
		return result, gofakes3.BucketNotFound(bucketName)
	}

	fp := path.Join(bucketName, objectName)
	objectDir := path.Dir(fp)
	// _, err = db.fs.Stat(objectDir)
	// if err == vfs.ENOENT {
	// 	fs.Errorf(objectDir, "PutObject failed: path not found")
	// 	return result, gofakes3.KeyNotFound(objectName)
	// }

	if objectDir != "." {
		if err := mkdirRecursive(objectDir, _vfs); err != nil {
			return result, err
		}
	}

	// Determine modTime for RcatSize
	modTime := time.Now()
	if mtimeStr, ok := meta["x-amz-meta-mtime"]; ok { // S3 specific metadata
		if t, err := swift.FloatStringToTime(mtimeStr); err == nil {
			modTime = t
		} else {
			fs.Debugf("serve s3", "Failed to parse x-amz-meta-mtime %q: %v", mtimeStr, err)
		}
	} else if mtimeStr, ok := meta["mtime"]; ok { // Internal metadata if already processed
		if t, err := swift.FloatStringToTime(mtimeStr); err == nil {
			modTime = t
		} else {
			fs.Debugf("serve s3", "Failed to parse mtime %q: %v", mtimeStr, err)
		}
	}

	// Construct fs.OpenOption slice from meta for conditional headers
	var openOptions []fs.OpenOption
	// Using X-Rclone-Internal-* prefix as a workaround until gofakes3 supports these natively
	if ifMatchEtag, ok := meta["X-Rclone-Internal-If-Match"]; ok && ifMatchEtag != "" {
		openOptions = append(openOptions, fs.IfMatchOption(ifMatchEtag))
	}
	if ifNoneMatchEtag, ok := meta["X-Rclone-Internal-If-None-Match"]; ok && ifNoneMatchEtag != "" {
		openOptions = append(openOptions, fs.IfNoneMatchOption(ifNoneMatchEtag))
	}
	if ifModSinceStr, ok := meta["X-Rclone-Internal-If-Modified-Since"]; ok && ifModSinceStr != "" {
		// S3 specific headers use RFC1123 format
		if t, err := http.ParseTime(ifModSinceStr); err == nil {
			openOptions = append(openOptions, fs.IfModifiedSinceOption(t))
		} else {
			fs.Debugf("serve s3", "Failed to parse X-Rclone-Internal-If-Modified-Since %q: %v. Expected RFC1123 format.", ifModSinceStr, err)
			// According to S3 spec, an invalid date format for If-Modified-Since might be ignored or result in an error.
			// For now, we log and ignore. If strict S3 behavior is needed, an error should be returned.
			// return result, gofakes3.ErrInvalidRequest // Or a more specific error
		}
	}
	if ifUnmodSinceStr, ok := meta["X-Rclone-Internal-If-Unmodified-Since"]; ok && ifUnmodSinceStr != "" {
		// S3 specific headers use RFC1123 format
		if t, err := http.ParseTime(ifUnmodSinceStr); err == nil {
			openOptions = append(openOptions, fs.IfUnmodifiedSinceOption(t))
		} else {
			fs.Debugf("serve s3", "Failed to parse X-Rclone-Internal-If-Unmodified-Since %q: %v. Expected RFC1123 format.", ifUnmodSinceStr, err)
			// Similar to If-Modified-Since, log and ignore for now.
			// return result, gofakes3.ErrInvalidRequest // Or a more specific error
		}
	}

	// Use operations.RcatSize to handle the upload with options
	// _vfs is an fs.Fs, fp is the remote path
	newObj, err := operations.RcatSize(ctx, _vfs, fp, input, size, modTime, openOptions...)
	if err != nil {
		// Attempt to map fs errors to gofakes3 errors
		if errors.Is(err, fs.ErrorPreconditionFailed) {
			return result, gofakes3.ErrPreconditionFailed
		}
		// Add more specific error mappings if needed, e.g. fs.ErrorNotModified
		// For now, fall back to internal error for other RcatSize failures.
		fs.Errorf("serve s3", "RcatSize failed for %q: %v", fp, err)
		return result, gofakes3.ErrInternal
	}

	// RcatSize has already set the modTime.
	// We store the client-provided 'meta' map as gofakes3 expects.
	// Any x-amz-meta-* headers from the original request are in 'meta'.
	// 'operations.RcatSize' itself doesn't directly consume this 'meta' map for fs.Object metadata,
	// but it does take `modTime`. If other `x-amz-meta-*` headers need to be translated to
	// `fs.OpenOption` for `RcatSize`, that would be an extension here.
	b.meta.Store(fp, meta) // Store original meta for gofakes3 compatibility and retrieval in Head/GetObject

	// Populate result from the fs.Object returned by RcatSize
	result.ETag = getFileHash(newObj, b.s.etagHashType) // getFileHash is an existing helper
	result.LastModified = gofakes3.NewContentTime(newObj.ModTime(ctx))
	// gofakes3.PutObjectResult has no direct field for version ID.
	// If versioning were fully implemented and returned by RcatSize/VFS,
	// and gofakes3 supported it, it would be set here.

	return result, nil
}

// DeleteMulti deletes multiple objects in a single request.
func (b *s3Backend) DeleteMulti(ctx context.Context, bucketName string, objects ...string) (result gofakes3.MultiDeleteResult, rerr error) {
	for _, object := range objects {
		if err := b.deleteObject(ctx, bucketName, object); err != nil {
			fs.Errorf("serve s3", "delete object failed: %v", err)
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

// DeleteObject deletes the object with the given name.
func (b *s3Backend) DeleteObject(ctx context.Context, bucketName, objectName string) (result gofakes3.ObjectDeleteResult, rerr error) {
	return result, b.deleteObject(ctx, bucketName, objectName)
}

// deleteObject deletes the object from the filesystem.
func (b *s3Backend) deleteObject(ctx context.Context, bucketName, objectName string) error {
	_vfs, err := b.s.getVFS(ctx)
	if err != nil {
		return err
	}
	_, err = _vfs.Stat(bucketName)
	if err != nil {
		return gofakes3.BucketNotFound(bucketName)
	}

	fp := path.Join(bucketName, objectName)
	// S3 does not report an error when attempting to delete a key that does not exist, so
	// we need to skip IsNotExist errors.
	if err := _vfs.Remove(fp); err != nil && !os.IsNotExist(err) {
		return err
	}

	// FIXME: unsafe operation
	rmdirRecursive(fp, _vfs)
	return nil
}

// CreateBucket creates a new bucket.
func (b *s3Backend) CreateBucket(ctx context.Context, name string) error {
	_vfs, err := b.s.getVFS(ctx)
	if err != nil {
		return err
	}
	_, err = _vfs.Stat(name)
	if err != nil && err != vfs.ENOENT {
		return gofakes3.ErrInternal
	}

	if err == nil {
		return gofakes3.ErrBucketAlreadyExists
	}

	if err := _vfs.Mkdir(name, 0755); err != nil {
		return gofakes3.ErrInternal
	}
	return nil
}

// DeleteBucket deletes the bucket with the given name.
func (b *s3Backend) DeleteBucket(ctx context.Context, name string) error {
	_vfs, err := b.s.getVFS(ctx)
	if err != nil {
		return err
	}
	_, err = _vfs.Stat(name)
	if err != nil {
		return gofakes3.BucketNotFound(name)
	}

	if err := _vfs.Remove(name); err != nil {
		return gofakes3.ErrBucketNotEmpty
	}

	return nil
}

// BucketExists checks if the bucket exists.
func (b *s3Backend) BucketExists(ctx context.Context, name string) (exists bool, err error) {
	_vfs, err := b.s.getVFS(ctx)
	if err != nil {
		return false, err
	}
	_, err = _vfs.Stat(name)
	if err != nil {
		return false, nil
	}

	return true, nil
}

// CopyObject copy specified object from srcKey to dstKey.
func (b *s3Backend) CopyObject(ctx context.Context, srcBucket, srcKey, dstBucket, dstKey string, meta map[string]string) (result gofakes3.CopyObjectResult, err error) {
	_vfs, err := b.s.getVFS(ctx)
	if err != nil {
		return result, err
	}
	fp := path.Join(srcBucket, srcKey)
	if srcBucket == dstBucket && srcKey == dstKey {
		b.meta.Store(fp, meta)

		val, ok := meta["X-Amz-Meta-Mtime"]
		if !ok {
			if val, ok = meta["mtime"]; !ok {
				return
			}
		}
		// update modtime
		ti, err := swift.FloatStringToTime(val)
		if err != nil {
			return result, nil
		}
		b.storeModtime(fp, meta, val)

		return result, _vfs.Chtimes(fp, ti, ti)
	}

	cStat, err := _vfs.Stat(fp)
	if err != nil {
		return
	}

	c, err := b.GetObject(ctx, srcBucket, srcKey, nil)
	if err != nil {
		return
	}
	defer func() {
		_ = c.Contents.Close()
	}()

	for k, v := range c.Metadata {
		if _, found := meta[k]; !found && k != "X-Amz-Acl" {
			meta[k] = v
		}
	}
	if _, ok := meta["mtime"]; !ok {
		meta["mtime"] = swift.TimeToFloatString(cStat.ModTime())
	}

	_, err = b.PutObject(ctx, dstBucket, dstKey, meta, c.Contents, c.Size)
	if err != nil {
		return
	}

	return gofakes3.CopyObjectResult{
		ETag:         `"` + hex.EncodeToString(c.Hash) + `"`,
		LastModified: gofakes3.NewContentTime(cStat.ModTime()),
	}, nil
}
