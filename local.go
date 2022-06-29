package filesystem

import (
	"context"
	"io"
	"io/fs"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/mtfelian/utils"
)

// Local implements FileSystem. The implementation is not concurrent-safe
type Local struct{}

// NewLocal returns a pointer to a new Local object
func NewLocal() FileSystem { return &Local{} }

// Open file in the FileSystem
func (l *Local) Open(ctx context.Context, name string) (File, error) { return os.Open(name) }

// Create file in the FileSystem
func (l *Local) Create(ctx context.Context, name string) (File, error) {
	if err := os.MkdirAll(filepath.Dir(name), 0777); err != nil {
		return nil, err
	}
	return os.Create(name)
}

// OpenW opens file in the FileSystem for writing
func (l *Local) OpenW(ctx context.Context, name string) (File, error) {
	if err := os.MkdirAll(filepath.Dir(name), 0777); err != nil {
		return nil, err
	}
	return os.OpenFile(name, os.O_WRONLY, 0666)
}

// ReadFile by name
func (l *Local) ReadFile(ctx context.Context, name string) ([]byte, error) { return os.ReadFile(name) }

// WriteFile by name
func (l *Local) WriteFile(ctx context.Context, name string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(name), 0777); err != nil {
		return err
	}
	return os.WriteFile(name, data, 0644)
}

// WriteFiles by the data given
func (l *Local) WriteFiles(ctx context.Context, f []FileNameData) error {
	for _, el := range f {
		if err := os.MkdirAll(filepath.Dir(el.Name), 0777); err != nil {
			return err
		}
		if err := os.WriteFile(el.Name, el.Data, 0644); err != nil {
			return err
		}
	}
	return nil
}

// Reader returns io.Reader file abstraction
func (l *Local) Reader(ctx context.Context, name string) (io.ReadCloser, error) { return os.Open(name) }

// Exists returns whether file exists or not
func (l *Local) Exists(ctx context.Context, name string) (bool, error) {
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
func (l *Local) MakePathAll(ctx context.Context, name string) error { return os.MkdirAll(name, 0777) }

// Remove file
func (l *Local) Remove(ctx context.Context, name string) error { return os.Remove(name) }

// RemoveFiles files given
func (l *Local) RemoveFiles(ctx context.Context, names []string) error {
	for _, name := range names {
		if err := l.Remove(ctx, name); err != nil {
			return err
		}
	}
	return nil
}

// RemoveAll removes the entire name
func (l *Local) RemoveAll(ctx context.Context, name string) error { return os.RemoveAll(name) }

// IsEmptyPath returns whether given name is empty (does not contain any subpaths)
func (l *Local) IsEmptyPath(ctx context.Context, name string) (bool, error) {
	return utils.IsEmptyDir(name)
}

// IsNotExist returns whether err is a "file not exists" error
func (l *Local) IsNotExist(err error) bool { return os.IsNotExist(err) }

// PreparePath constructs an absolute name from. If it does not exists, creates it.
func (l *Local) PreparePath(ctx context.Context, name string) (string, error) {
	absolutePath, err := filepath.Abs(name)
	if err != nil {
		return "", err
	}

	if exists, err := l.Exists(ctx, absolutePath); !exists && err == nil {
		if err := l.MakePathAll(ctx, absolutePath); err != nil {
			return "", err
		}
	}

	return absolutePath, nil
}

// Rename file
func (l *Local) Rename(ctx context.Context, from, to string) error {
	if filepath.Clean(from) == filepath.Clean(to) {
		return nil
	}
	return os.Rename(from, to)
}

// Stat returns a FileInfo describing the named file
func (l *Local) Stat(ctx context.Context, name string) (FileInfo, error) {
	stat, err := os.Stat(name)
	if err != nil {
		return nil, err
	}
	return NewLocalFileInfo(stat, name), nil
}

// ReadDir with the name given
func (l *Local) ReadDir(ctx context.Context, name string) (FilesInfo, error) {
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
func (l *Local) WalkDir(ctx context.Context, root string, walkDirFunc WalkDirFunc) error {
	return filepath.WalkDir(root, func(path string, info fs.DirEntry, err error) error {
		infoInfo, err := info.Info()
		if err != nil {
			return err
		}
		return walkDirFunc(path, LocalDirEntry{fi: NewLocalFileInfo(infoInfo, path)}, err)
	})
}
