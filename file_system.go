package filesystem

import (
	"context"
	"io"
	"io/fs"
)

// File abstracts a file
type File interface {
	fs.File
	io.ReaderAt
	io.Writer
	io.Seeker
	Truncate(int64) error
	Sync() error
}

// FilesInfo is a slice of FileInfo
type FilesInfo []FileInfo

// Names returns a slice of names derived from the receiver slice
func (fsi FilesInfo) Names() []string {
	res := make([]string, len(fsi))
	for i, el := range fsi {
		res[i] = el.Name()
	}
	return res
}

// FileInfo abstracts file information
type FileInfo interface{ fs.FileInfo }

// DirEntry abstracts directory walkDirEntry
type DirEntry interface{ fs.DirEntry }

// WalkDirFunc is a wrapper around fs.WalkDirFunc
type WalkDirFunc func(string, DirEntry, error) error

// FileSystem abstracts a file system
type FileSystem interface {
	WithContext(context.Context) FileSystem
	Create(string) (File, error)
	Open(string) (File, error)
	ReadFile(string) ([]byte, error)
	WriteFile(string, []byte) error
	Reader(string) (io.ReadCloser, error)
	Exists(string) (bool, error)
	MakePathAll(string) error
	Remove(string) error
	RemoveAll(string) error
	IsNotExist(error) bool
	IsEmptyPath(string) (bool, error)
	PreparePath(string) (string, error)
	Rename(string, string) error
	Stat(string) (FileInfo, error)
	ReadDir(string) (FilesInfo, error)
	WalkDir(string, WalkDirFunc) error
}
