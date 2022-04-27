package filesystem

import (
	"context"
	"io"
	"io/fs"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"

	"github.com/mtfelian/utils"
)

// Local implements FileSystem. The implementation is not concurrent-safe
type Local struct{}

// NewLocal returns a pointer to a new Local object
func NewLocal() FileSystem { return &Local{} }

// WithContext in Local implementation does nothing
func (l *Local) WithContext(ctx context.Context) FileSystem { return l }

// Open file in the FileSystem
func (l *Local) Open(name string) (File, error) { return os.Open(name) }

// Create file in the FileSystem
func (l *Local) Create(name string) (File, error) {
	if err := os.MkdirAll(path.Dir(name), 0777); err != nil {
		return nil, err
	}
	return os.Create(name)
}

// OpenW opens file in the FileSystem for writing
func (l *Local) OpenW(name string) (File, error) {
	if err := os.MkdirAll(path.Dir(name), 0777); err != nil {
		return nil, err
	}
	return os.OpenFile(name, os.O_WRONLY, 0666)
}

// ReadFile by name
func (l *Local) ReadFile(name string) ([]byte, error) { return os.ReadFile(name) }

// WriteFile by name
func (l *Local) WriteFile(name string, data []byte) error {
	if err := os.MkdirAll(path.Dir(name), 0777); err != nil {
		return err
	}
	return os.WriteFile(name, data, 0644)
}

// Reader returns io.Reader file abstraction
func (l *Local) Reader(name string) (io.ReadCloser, error) { return os.Open(name) }

// Exists returns whether file exists or not
func (l *Local) Exists(name string) (bool, error) {
	_, err := os.Stat(name)
	switch {
	case err == nil:
		return true, nil
	case l.IsNotExist(err):
		return false, nil
	default:
		return false, err
	}
}

// MakePathAll makes name recursively
func (l *Local) MakePathAll(name string) error { return os.MkdirAll(name, 0777) }

// Remove file
func (l *Local) Remove(name string) error { return os.Remove(name) }

// RemoveAll removes the entire name
func (l *Local) RemoveAll(name string) error { return os.RemoveAll(name) }

// IsEmptyPath returns whether given name is empty (does not contain any subpaths)
func (l *Local) IsEmptyPath(name string) (bool, error) { return utils.IsEmptyDir(name) }

// IsNotExist returns whether err is a "file not exists" error
func (l *Local) IsNotExist(err error) bool { return os.IsNotExist(err) }

// PreparePath constructs an absolute name from. If it does not exists, creates it.
func (l *Local) PreparePath(name string) (string, error) {
	absolutePath, err := filepath.Abs(name)
	if err != nil {
		return "", err
	}

	if exists, err := l.Exists(absolutePath); !exists && err == nil {
		if err := l.MakePathAll(absolutePath); err != nil {
			return "", err
		}
	}

	return absolutePath, nil
}

// Rename file
func (l *Local) Rename(from, to string) error {
	if filepath.Clean(from) == filepath.Clean(to) {
		return nil
	}
	return os.Rename(from, to)
}

// Stat returns a FileInfo describing the named file
func (l *Local) Stat(name string) (FileInfo, error) {
	stat, err := os.Stat(name)
	if err != nil {
		return nil, err
	}
	return NewLocalFileInfo(stat, name), nil
}

// ReadDir with the name given
func (l *Local) ReadDir(name string) (FilesInfo, error) {
	fi, err := ioutil.ReadDir(name)
	if err != nil {
		return nil, err
	}
	res := make(FilesInfo, len(fi))
	for i := range fi {
		res[i] = NewLocalFileInfo(fi[i], name)
	}
	return res, nil
}

// WalkDir traverses the filesystem from the given directory
func (l *Local) WalkDir(root string, walkDirFunc WalkDirFunc) error {
	return filepath.WalkDir(root, func(path string, info fs.DirEntry, err error) error {
		infoInfo, err := info.Info()
		if err != nil {
			return err
		}
		return walkDirFunc(path, LocalDirEntry{fi: NewLocalFileInfo(infoInfo, path)}, err)
	})
}
