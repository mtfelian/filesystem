package filesystem

import (
	"io/fs"

	"github.com/minio/minio-go/v7"
)

// S3DirEntry implements DirEntry
type S3DirEntry struct {
	oi minio.ObjectInfo
	fi FileInfo
	s3 *S3
}

// FullName makes S3DirEntry to implement DirEntry. Returns full name of the underlying FileInfo object
func (s S3DirEntry) FullName() string { return s.fi.FullName() }

// Name makes S3DirEntry to implement DirEntry. Returns name of the underlying FileInfo object
func (s S3DirEntry) Name() string { return s.fi.Name() }

// IsDir makes S3DirEntry to implement DirEntry. It calls IsDir() of the underlying FileInfo object
func (s S3DirEntry) IsDir() bool { return s.fi.IsDir() }

// Type makes S3DirEntry to implement DirEntry. It calls Mode() of the underlying FileInfo object
func (s S3DirEntry) Type() fs.FileMode { return s.fi.Mode() }

// Info makes S3DirEntry to implement DirEntry. It returns the underlying FileInfo object
func (s S3DirEntry) Info() (fs.FileInfo, error) { return s.fi, nil }
