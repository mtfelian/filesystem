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
	Name() string
}

// FilesInfo is a slice of FileInfo
type FilesInfo []FileInfo

// FullNames returns a slice of full names derived from the receiver slice
func (fsi FilesInfo) FullNames() []string {
	res := make([]string, len(fsi))
	for i, el := range fsi {
		res[i] = el.FullName()
	}
	return res
}

// FileInfo abstracts file information
type FileInfo interface {
	fs.FileInfo
	FullName() string
}

// DirEntry abstracts directory walkDirEntry
type DirEntry interface {
	fs.DirEntry
	FullName() string
}

// WalkDirFunc is a wrapper around fs.WalkDirFunc
type WalkDirFunc func(string, DirEntry, error) error

// FileNameData represents file name and data
type FileNameData struct {
	Name string
	Data []byte
}

// FileSystem abstracts a file system
type FileSystem interface {
	Create(context.Context, string) (File, error)
	Open(context.Context, string) (File, error)
	OpenW(context.Context, string) (File, error)
	ReadFile(context.Context, string) ([]byte, error)
	WriteFile(context.Context, string, []byte) error
	WriteFiles(context.Context, []FileNameData) error
	Reader(context.Context, string) (io.ReadCloser, error)
	Exists(context.Context, string) (bool, error)
	MakePathAll(context.Context, string) error
	Remove(context.Context, string) error
	RemoveFiles(context.Context, []string) error
	RemoveAll(context.Context, string) error
	IsNotExist(error) bool
	IsEmptyPath(context.Context, string) (bool, error)
	PreparePath(context.Context, string) (string, error)
	Rename(context.Context, string, string) error
	Stat(context.Context, string) (FileInfo, error)
	ReadDir(context.Context, string) (FilesInfo, error)
	WalkDir(context.Context, string, WalkDirFunc) error
	Join(...string) string
	Dir(string) string
	Ext(string) string
}
