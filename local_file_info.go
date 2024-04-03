package filesystem

import (
	"io/fs"
	"os"
	"time"
)

// LocalFileInfo implements FileInfo
type LocalFileInfo struct {
	fs       FileSystem
	fi       os.FileInfo
	fullName string
}

// NewLocalFileInfo returns new LocalFileInfo object
func NewLocalFileInfo(fs *Local, fi os.FileInfo, fullName string) LocalFileInfo {
	return LocalFileInfo{fs: fs, fi: fi, fullName: fullName}
}

// FS makes LocalFileInfo to implement FileInfo
func (s LocalFileInfo) FS() FileSystem { return s.fs }

// Name makes LocalFileInfo to implement FileInfo
func (s LocalFileInfo) Name() string { return s.fi.Name() }

// FullName makes LocalFileInfo to implement FileInfo
func (s LocalFileInfo) FullName() string { return s.fullName }

// Size makes LocalFileInfo to implement FileInfo
func (s LocalFileInfo) Size() int64 { return s.fi.Size() }

// Mode makes LocalFileInfo to implement FileInfo
func (s LocalFileInfo) Mode() fs.FileMode { return s.fi.Mode() }

// ModTime makes LocalFileInfo to implement FileInfo
func (s LocalFileInfo) ModTime() time.Time { return s.fi.ModTime() }

// IsDir makes LocalFileInfo to implement FileInfo
func (s LocalFileInfo) IsDir() bool { return s.fi.IsDir() }

// Sys makes LocalFileInfo to implement FileInfo
func (s LocalFileInfo) Sys() interface{} { return s.fi.Sys() }
