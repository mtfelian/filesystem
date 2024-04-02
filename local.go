package filesystem

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/mtfelian/utils"
)

// Local implements FileSystem. The implementation is not concurrent-safe
type Local struct{}

// NewLocal returns a pointer to a new Local object
func NewLocal() FileSystem { return &Local{} }

// Open file in the FileSystem
func (l *Local) Open(ctx context.Context, name string) (f File, err error) {
	if ctx, err = invokeBeforeOperationCB(ctx); err != nil {
		return
	}
	defer func() {
		if errcb := invokeAfterOperationCB(ctx); err == nil {
			err = errcb
		} // else drop callback error
	}()

	return os.Open(name)
}

// Create file in the FileSystem
func (l *Local) Create(ctx context.Context, name string) (f File, err error) {
	if ctx, err = invokeBeforeOperationCB(ctx); err != nil {
		return
	}
	defer func() {
		if errcb := invokeAfterOperationCB(ctx); err == nil {
			err = errcb
		} // else drop callback error
	}()

	if err = os.MkdirAll(l.Dir(name), 0777); err != nil {
		return
	}
	return os.Create(name)
}

// OpenW opens file in the FileSystem for writing
func (l *Local) OpenW(ctx context.Context, name string) (f File, err error) {
	if ctx, err = invokeBeforeOperationCB(ctx); err != nil {
		return
	}
	defer func() {
		if errcb := invokeAfterOperationCB(ctx); err == nil {
			err = errcb
		} // else drop callback error
	}()

	if err = os.MkdirAll(l.Dir(name), 0777); err != nil {
		return
	}
	return os.OpenFile(name, os.O_WRONLY, 0666)
}

// ReadFile by name
func (l *Local) ReadFile(ctx context.Context, name string) (b []byte, err error) {
	if ctx, err = invokeBeforeOperationCB(ctx); err != nil {
		return
	}
	defer func() {
		if errcb := invokeAfterOperationCB(ctx); err == nil {
			err = errcb
		} // else drop callback error
	}()

	return os.ReadFile(name)
}

// WriteFile by name
func (l *Local) WriteFile(ctx context.Context, name string, data []byte) (err error) {
	if ctx, err = invokeBeforeOperationCB(ctx); err != nil {
		return
	}
	defer func() {
		if errcb := invokeAfterOperationCB(ctx); err == nil {
			err = errcb
		} // else drop callback error
	}()

	if err = os.MkdirAll(l.Dir(name), 0777); err != nil {
		return
	}
	return os.WriteFile(name, data, 0644)
}

// WriteFiles by the data given
func (l *Local) WriteFiles(ctx context.Context, f []FileNameData) (err error) {
	if ctx, err = invokeBeforeOperationCB(ctx); err != nil {
		return
	}
	defer func() {
		if errcb := invokeAfterOperationCB(ctx); err == nil {
			err = errcb
		} // else drop callback error
	}()

	for _, el := range f {
		if err = os.MkdirAll(l.Dir(el.Name), 0777); err != nil {
			return
		}
		if err = os.WriteFile(el.Name, el.Data, 0644); err != nil {
			return
		}
	}
	return
}

// Reader returns io.Reader file abstraction
func (l *Local) Reader(ctx context.Context, name string) (r io.ReadCloser, err error) {
	if ctx, err = invokeBeforeOperationCB(ctx); err != nil {
		return
	}
	defer func() {
		if errcb := invokeAfterOperationCB(ctx); err == nil {
			err = errcb
		} // else drop callback error
	}()

	return os.Open(name)
}

// Exists returns whether file exists or not
func (l *Local) Exists(ctx context.Context, name string) (e bool, err error) {
	if ctx, err = invokeBeforeOperationCB(ctx); err != nil {
		return
	}
	defer func() {
		if errcb := invokeAfterOperationCB(ctx); err == nil {
			err = errcb
		} // else drop callback error
	}()

	_, err = os.Stat(name)
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
func (l *Local) MakePathAll(ctx context.Context, name string) (err error) {
	if ctx, err = invokeBeforeOperationCB(ctx); err != nil {
		return
	}
	defer func() {
		if errcb := invokeAfterOperationCB(ctx); err == nil {
			err = errcb
		} // else drop callback error
	}()

	return os.MkdirAll(name, 0777)
}

// Remove file
func (l *Local) Remove(ctx context.Context, name string) (err error) {
	if ctx, err = invokeBeforeOperationCB(ctx); err != nil {
		return
	}
	defer func() {
		if errcb := invokeAfterOperationCB(ctx); err == nil {
			err = errcb
		} // else drop callback error
	}()

	return os.Remove(name)
}

// RemoveFiles files given
func (l *Local) RemoveFiles(ctx context.Context, names []string) (failed []string, err error) {
	if ctx, err = invokeBeforeOperationCB(ctx); err != nil {
		return
	}
	defer func() {
		if errcb := invokeAfterOperationCB(ctx); err == nil {
			err = errcb
		} // else drop callback error
	}()

	for _, name := range names {
		if err = l.Remove(ctx, name); err != nil {
			failed = append(failed, name)
		}
	}
	return
}

// RemoveAll removes the entire name
func (l *Local) RemoveAll(ctx context.Context, name string) (err error) {
	if ctx, err = invokeBeforeOperationCB(ctx); err != nil {
		return
	}
	defer func() {
		if errcb := invokeAfterOperationCB(ctx); err == nil {
			err = errcb
		} // else drop callback error
	}()

	return os.RemoveAll(name)
}

// IsEmptyPath returns whether given name is empty (does not contain any subpaths)
func (l *Local) IsEmptyPath(ctx context.Context, name string) (e bool, err error) {
	if ctx, err = invokeBeforeOperationCB(ctx); err != nil {
		return
	}
	defer func() {
		if errcb := invokeAfterOperationCB(ctx); err == nil {
			err = errcb
		} // else drop callback error
	}()

	return utils.IsEmptyDir(name)
}

// IsNotExist returns whether err is a "file not exists" error
func (l *Local) IsNotExist(err error) bool { return os.IsNotExist(err) }

// PreparePath constructs an absolute name from. If it does not exists, creates it.
func (l *Local) PreparePath(ctx context.Context, name string) (absolutePath string, err error) {
	if ctx, err = invokeBeforeOperationCB(ctx); err != nil {
		return
	}
	defer func() {
		if errcb := invokeAfterOperationCB(ctx); err == nil {
			err = errcb
		} // else drop callback error
	}()

	if absolutePath, err = filepath.Abs(name); err != nil {
		return "", err
	}

	var exists bool
	if exists, err = l.Exists(ctx, absolutePath); !exists && err == nil {
		if err = l.MakePathAll(ctx, absolutePath); err != nil {
			return "", err
		}
	}

	return absolutePath, nil
}

// Rename file
func (l *Local) Rename(ctx context.Context, from, to string) (err error) {
	if ctx, err = invokeBeforeOperationCB(ctx); err != nil {
		return
	}
	defer func() {
		if errcb := invokeAfterOperationCB(ctx); err == nil {
			err = errcb
		} // else drop callback error
	}()

	if filepath.Clean(from) == filepath.Clean(to) {
		return
	}
	return os.Rename(from, to)
}

// Stat returns a FileInfo describing the named file
func (l *Local) Stat(ctx context.Context, name string) (fi FileInfo, err error) {
	if ctx, err = invokeBeforeOperationCB(ctx); err != nil {
		return
	}
	defer func() {
		if errcb := invokeAfterOperationCB(ctx); err == nil {
			err = errcb
		} // else drop callback error
	}()

	var osfi os.FileInfo
	if osfi, err = os.Stat(name); err != nil {
		return
	}
	return NewLocalFileInfo(osfi, name), nil
}

// ReadDir with the name given
func (l *Local) ReadDir(ctx context.Context, name string) (fi FilesInfo, err error) {
	if ctx, err = invokeBeforeOperationCB(ctx); err != nil {
		return
	}
	defer func() {
		if errcb := invokeAfterOperationCB(ctx); err == nil {
			err = errcb
		} // else drop callback error
	}()

	var nameFi FileInfo
	if nameFi, err = l.Stat(ctx, name); err != nil {
		return
	}
	if !nameFi.IsDir() {
		err = ErrNotADirectory
		return
	}

	var fsfi []os.DirEntry
	if fsfi, err = os.ReadDir(name); err != nil {
		return
	}
	fi = make(FilesInfo, len(fsfi))
	for i := range fsfi {
		var finfo os.FileInfo
		if finfo, err = fsfi[i].Info(); err != nil {
			return
		}
		fi[i] = NewLocalFileInfo(finfo, l.Join(name, fsfi[i].Name()))
	}
	return
}

// WalkDir traverses the filesystem from the given directory
func (l *Local) WalkDir(ctx context.Context, root string, walkDirFunc WalkDirFunc) (err error) {
	if ctx, err = invokeBeforeOperationCB(ctx); err != nil {
		return
	}
	defer func() {
		if errcb := invokeAfterOperationCB(ctx); err == nil {
			err = errcb
		} // else drop callback error
	}()

	return filepath.WalkDir(root, func(path string, info fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if info == nil {
			return fs.ErrInvalid
		}
		infoInfo, err := info.Info()
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, os.ErrPermission) {
			return nil
		}
		if err != nil {
			return err
		}
		return walkDirFunc(path, LocalDirEntry{fi: NewLocalFileInfo(infoInfo, path)}, err)
	})
}

// Join the path segments
func (l *Local) Join(names ...string) string { return filepath.Join(names...) }

// Dir returns parent directory path
func (l *Local) Dir(name string) string { return filepath.Dir(name) }

// Ext returns file name extension
func (l *Local) Ext(name string) string { return filepath.Ext(name) }

// Base returns the last element of path
func (l *Local) Base(name string) string { return filepath.Base(name) }
