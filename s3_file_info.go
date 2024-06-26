package filesystem

import (
	"io/fs"
	"path"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
)

// S3FileInfo implements FileInfo
type S3FileInfo struct {
	oi minio.ObjectInfo
	fs FileSystem
}

// NewS3FileInfoStub returns new stub S3FileInfo object
func NewS3FileInfoStub(fs *S3, key string, modTime time.Time) S3FileInfo {
	return S3FileInfo{oi: minio.ObjectInfo{Key: key, LastModified: modTime, Size: 0}, fs: fs}
}

// NewS3FileInfo returns new S3FileInfo object
func NewS3FileInfo(fs *S3, oi minio.ObjectInfo) S3FileInfo { return S3FileInfo{oi: oi, fs: fs} }

// FS makes S3FileInfo to implement FileInfo. Returns same as Sys() method, but an interface.
func (s S3FileInfo) FS() FileSystem { return s.fs }

// Name makes S3FileInfo to implement FileInfo. Returns last part (file name) of the key of S3 object
func (s S3FileInfo) Name() string { return path.Base(s.FullName()) }

// FullName makes S3FileInfo to implement FileInfo. Returns last part (file name) of the key of S3 object
func (s S3FileInfo) FullName() string { return s.oi.Key }

// Size makes S3FileInfo to implement FileInfo. Returns size of S3 object
func (s S3FileInfo) Size() int64 { return s.oi.Size }

// Mode makes S3FileInfo to implement FileInfo. It always returns 0
func (s S3FileInfo) Mode() fs.FileMode { return 0 }

// ModTime makes S3FileInfo to implement FileInfo. Returns last modified time
func (s S3FileInfo) ModTime() time.Time { return s.oi.LastModified }

// IsDir makes S3FileInfo to implement FileInfo. It returns whether an object key
// ends in '/' (and it's size is 0)
func (s S3FileInfo) IsDir() bool {
	return strings.HasSuffix(s.oi.Key, "/") // && s.oi.Size == 0
}

// Sys makes S3FileInfo to implement FileInfo. It returns a value of type *S3:
// a pointer to the underlying FileSystem-implementing object
func (s S3FileInfo) Sys() interface{} { return s.fs.(*S3) }
