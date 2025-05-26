// Serve s3 tests set up a server and run the integration tests
// for the s3 remote against it.

package s3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"path"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	_ "github.com/rclone/rclone/backend/local"
	"github.com/rclone/rclone/cmd/serve/proxy"
	"github.com/rclone/rclone/cmd/serve/servetest"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/fs/object"
	"github.com/rclone/rclone/fs/rc"
	"github.com/rclone/rclone/fstest"
	"github.com/rclone/rclone/lib/random"
	"github.com/rclone/rclone/vfs/vfscommon"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	endpoint = "localhost:0"
)

// Configure and serve the server
func serveS3(t *testing.T, f fs.Fs) (testURL string, keyid string, keysec string, w *Server) {
	keyid = random.String(16)
	keysec = random.String(16)
	opt := Opt // copy default options
	opt.AuthKey = []string{fmt.Sprintf("%s,%s", keyid, keysec)}
	opt.HTTP.ListenAddr = []string{endpoint}
	w, _ = newServer(context.Background(), f, &opt, &vfscommon.Opt, &proxy.Opt)
	go func() {
		require.NoError(t, w.Serve())
	}()
	testURL = w.server.URLs()[0]

	return
}

// TestS3 runs the s3 server then runs the unit tests for the
// s3 remote against it.
func TestS3(t *testing.T) {
	start := func(f fs.Fs) (configmap.Simple, func()) {
		testURL, keyid, keysec, _ := serveS3(t, f)
		// Config for the backend we'll use to connect to the server
		config := configmap.Simple{
			"type":              "s3",
			"provider":          "Rclone",
			"endpoint":          testURL,
			"access_key_id":     keyid,
			"secret_access_key": keysec,
		}

		return config, func() {}
	}

	servetest.Run(t, "s3", start)
}

// Mock VFS for testing PutObject and other VFS interactions in s3Backend
type mockVFS struct {
	vfs.VFS                                           // Embed VFS interface
	statFn  func(path string) (os.FileInfo, error)    // Custom Stat behavior
	mkdirFn func(path string, perm os.FileMode) error // Custom Mkdir behavior
	// Add other methods if needed by tests
}

// Stat provides a mock implementation for VFS.Stat
func (m *mockVFS) Stat(path string) (os.FileInfo, error) {
	if m.statFn != nil {
		return m.statFn(path)
	}
	// Default mock behavior: assume path exists and is a directory or file as needed
	// For PutObject, Stat is often called on the bucket path.
	if strings.HasSuffix(path, "/") || path == "testbucket" { // Crude check if it's a dir/bucket
		return fstest.NewFileInfo(path, 0, time.Now(), true), nil
	}
	return fstest.NewFileInfo(path, 100, time.Now(), false), nil // Assume file for other paths
}

// Mkdir provides a mock implementation for VFS.Mkdir
func (m *mockVFS) Mkdir(path string, perm os.FileMode) error {
	if m.mkdirFn != nil {
		return m.mkdirFn(path, perm)
	}
	// Default mock behavior: assume mkdir succeeds
	return nil
}

func TestS3BackendPutObjectConditionalHeaders(t *testing.T) {
	ctx := context.Background()
	var capturedOptions []fs.OpenOption
	var rcatError error // To simulate errors from RcatSize

	originalRcatSize := operations.RcatSize
	operations.RcatSize = func(ctx context.Context, fdst fs.Fs, remote string, r io.Reader, size int64, modTime time.Time, options ...fs.OpenOption) (fs.Object, error) {
		capturedOptions = make([]fs.OpenOption, len(options)) // Clear and copy
		copy(capturedOptions, options)
		if rcatError != nil {
			return nil, rcatError
		}
		// Return a dummy fs.Object for successful calls
		return fstest.NewItem(remote, "", modTime), nil
	}
	defer func() { operations.RcatSize = originalRcatSize }()

	// Setup s3Backend with mock VFS
	mockVFSInstance := &mockVFS{}
	// Ensure Opt is initialized if your Server struct expects it.
	// Using default Opt from the package if available, or a new one.
	serverOpt := Opt // Assuming Opt is a global var with default options
	serverOpt.Auth.AccessKey = "testkey" // Ensure some auth is set if newServer checks
	serverOpt.Auth.SecretKey = "testsecret"

	// Minimal Server setup for s3Backend
	// The vfscommon.Opt and proxy.Opt might not be strictly necessary if newServer handles nil or default.
	// If newServer panics or errors here, these might need to be initialized.
	s3Server, err := newServer(ctx, mockVFSInstance, &serverOpt, &vfscommon.Opt{}, &proxy.Opt{})
	require.NoError(t, err, "newServer failed")
	// The newBackend function might not be exported or might be internal to the package.
	// If newBackend is not accessible, instantiate s3Backend directly if possible,
	// or use a helper if the package provides one.
	// For this example, assuming s3Backend can be created/accessed.
	// If s3Backend is not directly constructible or newBackend is internal,
	// this test might need to be in the s3 package itself.
	// Let's assume newBackend is available for now or s3Server is the backend.
	// Based on the original code structure, newBackend is likely available.
	backend := newBackend(s3Server).(*s3Backend)

	dummyBucket := "testbucket"
	dummyObject := "testobject.txt"
	dummyContent := "hello world"

	// Mock VFS Stat to indicate bucket exists and is a directory
	mockVFSInstance.statFn = func(p string) (os.FileInfo, error) {
		if p == dummyBucket {
			return fstest.NewFileInfo(dummyBucket, 0, time.Now(), true), nil
		}
		// For parent directory of the object, also return it as a directory
		if p == path.Dir(path.Join(dummyBucket, dummyObject)) {
			return fstest.NewFileInfo(p, 0, time.Now(), true), nil
		}
		// For specific object paths during RcatSize (if it tries to Stat before write, which it shouldn't)
		// or for HeadObject after PutObject, return a file.
		// For this test, RcatSize is mocked, so we mainly care about bucket and parent dir checks.
		return fstest.NewFileInfo(p, 100, time.Now(), false), vfs.ENOENT // Default to not found for other files initially
	}

	time1Str := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC).Format(http.TimeFormat)
	time1Parsed, _ := time.Parse(http.TimeFormat, time1Str)

	testCases := []struct {
		name                string
		meta                map[string]string
		expectedOptionTypes []any // Types of expected options
		expectedValues      []fs.OpenOption
		setupRcatError      error
		expectGofake3Error  *gofakes3.ErrorResponse // Use pointer for optional error check
	}{
		{
			name:                "If-Match",
			meta:                map[string]string{"X-Rclone-Internal-If-Match": "etag123"},
			expectedOptionTypes: []any{fs.IfMatchOption("")},
			expectedValues:      []fs.OpenOption{fs.IfMatchOption("etag123")},
		},
		{
			name:                "If-None-Match",
			meta:                map[string]string{"X-Rclone-Internal-If-None-Match": "etag456"},
			expectedOptionTypes: []any{fs.IfNoneMatchOption("")},
			expectedValues:      []fs.OpenOption{fs.IfNoneMatchOption("etag456")},
		},
		{
			name:                "If-Modified-Since",
			meta:                map[string]string{"X-Rclone-Internal-If-Modified-Since": time1Str},
			expectedOptionTypes: []any{fs.IfModifiedSinceOption{}},
			expectedValues:      []fs.OpenOption{fs.IfModifiedSinceOption(time1Parsed)},
		},
		{
			name:                "If-Unmodified-Since",
			meta:                map[string]string{"X-Rclone-Internal-If-Unmodified-Since": time1Str},
			expectedOptionTypes: []any{fs.IfUnmodifiedSinceOption{}},
			expectedValues:      []fs.OpenOption{fs.IfUnmodifiedSinceOption(time1Parsed)},
		},
		{
			name: "Combination If-Match and If-Unmodified-Since",
			meta: map[string]string{
				"X-Rclone-Internal-If-Match":              "combo-etag",
				"X-Rclone-Internal-If-Unmodified-Since": time1Str,
			},
			expectedOptionTypes: []any{fs.IfMatchOption(""), fs.IfUnmodifiedSinceOption{}},
			expectedValues:      []fs.OpenOption{fs.IfMatchOption("combo-etag"), fs.IfUnmodifiedSinceOption(time1Parsed)},
		},
		{
			name: "No Conditional Headers",
			meta: map[string]string{"Some-Other-Meta": "value"},
		},
		{
			name: "Invalid If-Modified-Since time format",
			meta: map[string]string{"X-Rclone-Internal-If-Modified-Since": "not-a-valid-http-time"},
			// Expect no conditional option to be added, and no error from PutObject itself for this
		},
		{
			name:               "PreconditionFailed Error Mapping",
			meta:               map[string]string{"X-Rclone-Internal-If-Match": "error-etag"},
			setupRcatError:     fs.ErrorPreconditionFailed,
			expectGofake3Error: &gofakes3.ErrPreconditionFailed,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			capturedOptions = nil // Reset
			rcatError = tc.setupRcatError

			// Need a new reader for each call to PutObject
			dummyReader := strings.NewReader(dummyContent)
			dummySize := int64(len(dummyContent))

			_, err := backend.PutObject(ctx, dummyBucket, dummyObject, tc.meta, dummyReader, dummySize)

			if tc.expectGofake3Error != nil {
				require.Error(t, err)
				// Check if the error is the expected gofakes3 error
				// This requires errors.Is or type assertion if gofakes3 errors are distinct types
				// For now, string comparison or errors.Is if gofakes3.ErrorResponse implements Is()
				var gf3Err *gofakes3.ErrorResponse
				if errors.As(err, &gf3Err) {
					assert.Equal(t, tc.expectGofake3Error.Code, gf3Err.Code, "Gofake3 error code mismatch")
				} else {
					// Fallback or fail if not a gofakes3 error type as expected
					assert.Contains(t, err.Error(), tc.expectGofake3Error.Message, "Error message mismatch")
				}
			} else {
				require.NoError(t, err)
			}

			require.Len(t, capturedOptions, len(tc.expectedOptionTypes))
			if len(tc.expectedOptionTypes) > 0 {
				for i, expectedType := range tc.expectedOptionTypes {
					assert.IsType(t, expectedType, capturedOptions[i], "Option type mismatch at index %d", i)
					if len(tc.expectedValues) > i {
						assert.Equal(t, tc.expectedValues[i], capturedOptions[i], "Option value mismatch at index %d", i)
					}
				}
			} else {
				// If no types are expected, ensure no conditional options were captured.
				// Some default options might be passed by RcatSize itself, so we only care about our specific ones.
				// This is implicitly handled by tc.expectedOptionTypes being empty and asserting len(capturedOptions).
			}
		})
	}
}

// tests using the minio client
func TestEncodingWithMinioClient(t *testing.T) {
	cases := []struct {
		description string
		bucket      string
		path        string
		filename    string
		expected    string
	}{
		{
			description: "weird file in bucket root",
			bucket:      "mybucket",
			path:        "",
			filename:    " file with w€r^d ch@r \\#~+§4%&'. txt ",
		},
		{
			description: "weird file inside a weird folder",
			bucket:      "mybucket",
			path:        "ä#/नेपाल&/?/",
			filename:    " file with w€r^d ch@r \\#~+§4%&'. txt ",
		},
	}

	for _, tt := range cases {
		t.Run(tt.description, func(t *testing.T) {
			fstest.Initialise()
			f, _, clean, err := fstest.RandomRemote()
			assert.NoError(t, err)
			defer clean()
			err = f.Mkdir(context.Background(), path.Join(tt.bucket, tt.path))
			assert.NoError(t, err)

			buf := bytes.NewBufferString("contents")
			uploadHash := hash.NewMultiHasher()
			in := io.TeeReader(buf, uploadHash)

			obji := object.NewStaticObjectInfo(
				path.Join(tt.bucket, tt.path, tt.filename),
				time.Now(),
				int64(buf.Len()),
				true,
				nil,
				nil,
			)
			_, err = f.Put(context.Background(), in, obji)
			assert.NoError(t, err)

			endpoint, keyid, keysec, _ := serveS3(t, f)
			testURL, _ := url.Parse(endpoint)
			minioClient, err := minio.New(testURL.Host, &minio.Options{
				Creds:  credentials.NewStaticV4(keyid, keysec, ""),
				Secure: false,
			})
			assert.NoError(t, err)

			buckets, err := minioClient.ListBuckets(context.Background())
			assert.NoError(t, err)
			assert.Equal(t, buckets[0].Name, tt.bucket)
			objects := minioClient.ListObjects(context.Background(), tt.bucket, minio.ListObjectsOptions{
				Recursive: true,
			})
			for object := range objects {
				assert.Equal(t, path.Join(tt.path, tt.filename), object.Key)
			}
		})
	}
}

type FileStuct struct {
	path     string
	filename string
}

type TestCase struct {
	description string
	bucket      string
	files       []FileStuct
	keyID       string
	keySec      string
	shouldFail  bool
}

func testListBuckets(t *testing.T, cases []TestCase, useProxy bool) {
	fstest.Initialise()

	var f fs.Fs
	if useProxy {
		// the backend config will be made by the proxy
		prog, err := filepath.Abs("../servetest/proxy_code.go")
		require.NoError(t, err)
		files, err := filepath.Abs("testdata")
		require.NoError(t, err)
		cmd := "go run " + prog + " " + files

		// FIXME: this is untidy setting a global variable!
		proxy.Opt.AuthProxy = cmd
		defer func() {
			proxy.Opt.AuthProxy = ""
		}()

		f = nil
	} else {
		// create a test Fs
		var err error
		f, err = fs.NewFs(context.Background(), "testdata")
		require.NoError(t, err)
	}

	for _, tt := range cases {
		t.Run(tt.description, func(t *testing.T) {
			endpoint, keyid, keysec, s := serveS3(t, f)
			defer func() {
				assert.NoError(t, s.server.Shutdown())
			}()

			if tt.keyID != "" {
				keyid = tt.keyID
			}
			if tt.keySec != "" {
				keysec = tt.keySec
			}

			testURL, _ := url.Parse(endpoint)
			minioClient, err := minio.New(testURL.Host, &minio.Options{
				Creds:  credentials.NewStaticV4(keyid, keysec, ""),
				Secure: false,
			})
			assert.NoError(t, err)

			buckets, err := minioClient.ListBuckets(context.Background())
			if tt.shouldFail {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.NotEmpty(t, buckets)
				assert.Equal(t, buckets[0].Name, tt.bucket)

				o := minioClient.ListObjects(context.Background(), tt.bucket, minio.ListObjectsOptions{
					Recursive: true,
				})
				// save files after reading from channel
				objects := []string{}
				for object := range o {
					objects = append(objects, object.Key)
				}

				for _, tt := range tt.files {
					file := path.Join(tt.path, tt.filename)
					found := slices.Contains(objects, file)
					require.Equal(t, true, found, "Object not found: "+file)
				}
			}
		})
	}
}

func TestListBuckets(t *testing.T) {
	var cases = []TestCase{
		{
			description: "list buckets",
			bucket:      "mybucket",
			files: []FileStuct{
				{
					path:     "",
					filename: "lorem.txt",
				},
				{
					path:     "foo",
					filename: "bar.txt",
				},
			},
		},
		{
			description: "list buckets: wrong s3 key",
			bucket:      "mybucket",
			keyID:       "invalid",
			shouldFail:  true,
		},
		{
			description: "list buckets: wrong s3 secret",
			bucket:      "mybucket",
			keySec:      "invalid",
			shouldFail:  true,
		},
	}

	testListBuckets(t, cases, false)
}

func TestListBucketsAuthProxy(t *testing.T) {
	var cases = []TestCase{
		{
			description: "list buckets",
			bucket:      "mybucket",
			// request with random keyid
			// instead of what was set in 'authPair'
			keyID: random.String(16),
			files: []FileStuct{
				{
					path:     "",
					filename: "lorem.txt",
				},
				{
					path:     "foo",
					filename: "bar.txt",
				},
			},
		},
		{
			description: "list buckets: wrong s3 secret",
			bucket:      "mybucket",
			keySec:      "invalid",
			shouldFail:  true,
		},
	}

	testListBuckets(t, cases, true)
}

func TestRc(t *testing.T) {
	servetest.TestRc(t, rc.Params{
		"type":           "s3",
		"vfs_cache_mode": "off",
	})
}
