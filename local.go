package filesystem

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/mtfelian/utils"
)

// ErrPathEscapesRoot means a rooted local filesystem path resolves outside its root.
var ErrPathEscapesRoot = errors.New("path escapes filesystem root")

// Local implements FileSystem. The implementation is not concurrent-safe.
type Local struct {
	root     string
	isClosed func() bool
}

// NewLocal returns a pointer to a new Local object
func NewLocal() FileSystem { return &Local{} }

// NewRootedLocal returns a Local filesystem scoped to root.
func NewRootedLocal(root string) FileSystem {
	if root == "" {
		root = "."
	}
	return &Local{root: filepath.Clean(root)}
}

// Close is a no-op here
func (l *Local) Close() error { return nil }

func (l *Local) ensureOpen() error {
	if l == nil {
		return ErrFileSystemClosed
	}
	if l.isClosed != nil && l.isClosed() {
		return ErrFileSystemClosed
	}
	return nil
}

func (l *Local) rooted() bool { return l != nil && l.root != "" }

func localPathEscapesRoot(name string) bool {
	return name == ".." || strings.HasPrefix(name, ".."+string(filepath.Separator))
}

func cleanLocalName(name string) (string, error) {
	name = filepath.FromSlash(name)
	if volume := filepath.VolumeName(name); volume != "" {
		name = strings.TrimPrefix(name, volume)
	}
	name = strings.TrimLeft(name, `\/`)
	if name == "" {
		return ".", nil
	}

	name = filepath.Clean(name)
	if localPathEscapesRoot(name) {
		return "", ErrPathEscapesRoot
	}
	return name, nil
}

func (l *Local) physicalName(name string) (string, error) {
	if err := l.ensureOpen(); err != nil {
		return "", err
	}
	if !l.rooted() {
		return name, nil
	}

	cleanName, err := cleanLocalName(name)
	if err != nil {
		return "", err
	}
	if cleanName == "." {
		return l.root, nil
	}

	physicalName := filepath.Join(l.root, cleanName)
	relName, err := filepath.Rel(l.root, physicalName)
	if err != nil {
		return "", err
	}
	if localPathEscapesRoot(relName) {
		return "", ErrPathEscapesRoot
	}
	return physicalName, nil
}

func (l *Local) logicalName(name string) string {
	if !l.rooted() {
		return name
	}
	cleanName, err := cleanLocalName(name)
	if err != nil {
		return name
	}
	return cleanName
}

func (l *Local) logicalNameFromPhysical(name string) string {
	if !l.rooted() {
		return name
	}
	relName, err := filepath.Rel(l.root, name)
	if err != nil {
		return name
	}
	return relName
}

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

	physicalName, err := l.physicalName(name)
	if err != nil {
		return nil, err
	}

	var osFile *os.File
	osFile, err = os.Open(physicalName)
	if err != nil {
		return nil, err
	}
	return &LocalFile{File: osFile, local: l, name: l.logicalName(name)}, err
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

	physicalName, err := l.physicalName(name)
	if err != nil {
		return nil, err
	}

	if err = os.MkdirAll(filepath.Dir(physicalName), 0777); err != nil {
		return
	}

	var osFile *os.File
	osFile, err = os.Create(physicalName)
	if err != nil {
		return nil, err
	}
	return &LocalFile{File: osFile, local: l, name: l.logicalName(name)}, err
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

	physicalName, err := l.physicalName(name)
	if err != nil {
		return nil, err
	}

	if err = os.MkdirAll(filepath.Dir(physicalName), 0777); err != nil {
		return
	}

	var osFile *os.File
	osFile, err = os.OpenFile(physicalName, os.O_WRONLY, 0666)
	if err != nil {
		return nil, err
	}
	return &LocalFile{File: osFile, local: l, name: l.logicalName(name)}, err
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

	physicalName, err := l.physicalName(name)
	if err != nil {
		return nil, err
	}
	return os.ReadFile(physicalName)
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

	return l.writeReader(ctx, name, bytes.NewReader(data), int64(len(data)))
}

// WriteReader writes all bytes from r to name without holding the whole payload in memory.
func (l *Local) WriteReader(ctx context.Context, name string, r io.Reader, size int64) (err error) {
	if ctx, err = invokeBeforeOperationCB(ctx); err != nil {
		return
	}
	defer func() {
		if errcb := invokeAfterOperationCB(ctx); err == nil {
			err = errcb
		} // else drop callback error
	}()

	return l.writeReader(ctx, name, r, size)
}

func (l *Local) writeReader(ctx context.Context, name string, r io.Reader, size int64) (err error) {
	if size < 0 {
		return ErrInvalidSize
	}

	physicalName, err := l.physicalName(name)
	if err != nil {
		return err
	}
	dir := filepath.Dir(physicalName)
	if err = os.MkdirAll(dir, 0777); err != nil {
		return err
	}

	tempFile, err := os.CreateTemp(dir, filepath.Base(physicalName)+".tmp.*")
	if err != nil {
		return err
	}
	tempName := tempFile.Name()
	var keepTemp bool
	defer func() {
		if keepTemp {
			return
		}
		if removeErr := os.Remove(tempName); err != nil && removeErr != nil && !os.IsNotExist(removeErr) {
			err = errors.Join(err, removeErr)
		}
	}()

	var written int64
	written, err = io.Copy(tempFile, r)
	closeErr := tempFile.Close()
	if err != nil || closeErr != nil {
		return errors.Join(err, closeErr)
	}
	if written != size {
		return fmt.Errorf("%w: expected %d, got %d", ErrUnexpectedSize, size, written)
	}
	if err = os.Chmod(tempName, 0644); err != nil {
		return err
	}
	if err = os.Rename(tempName, physicalName); err != nil {
		return err
	}
	keepTemp = true
	return nil
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
		var physicalName string
		physicalName, err = l.physicalName(el.Name)
		if err != nil {
			return
		}
		if err = os.MkdirAll(filepath.Dir(physicalName), 0777); err != nil {
			return
		}
		if err = os.WriteFile(physicalName, el.Data, 0644); err != nil {
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

	physicalName, err := l.physicalName(name)
	if err != nil {
		return nil, err
	}
	return os.Open(physicalName)
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

	physicalName, err := l.physicalName(name)
	if err != nil {
		return false, err
	}

	_, err = os.Stat(physicalName)
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

	physicalName, err := l.physicalName(name)
	if err != nil {
		return err
	}
	return os.MkdirAll(physicalName, 0777)
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

	physicalName, err := l.physicalName(name)
	if err != nil {
		return err
	}
	return os.Remove(physicalName)
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
		if removeErr := l.Remove(ctx, name); removeErr != nil {
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

	physicalName, err := l.physicalName(name)
	if err != nil {
		return err
	}
	return os.RemoveAll(physicalName)
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

	physicalName, err := l.physicalName(name)
	if err != nil {
		return false, err
	}
	return utils.IsEmptyDir(physicalName)
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

	physicalName, err := l.physicalName(name)
	if err != nil {
		return "", err
	}

	if absolutePath, err = filepath.Abs(physicalName); err != nil {
		return "", err
	}

	var exists bool
	if exists, err = l.Exists(ctx, name); err != nil {
		return "", err
	}
	if !exists {
		if err = l.MakePathAll(ctx, name); err != nil {
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

	physicalFrom, err := l.physicalName(from)
	if err != nil {
		return err
	}
	physicalTo, err := l.physicalName(to)
	if err != nil {
		return err
	}

	if filepath.Clean(physicalFrom) == filepath.Clean(physicalTo) {
		return
	}
	if err = os.MkdirAll(filepath.Dir(physicalTo), 0777); err != nil {
		return err
	}
	return os.Rename(physicalFrom, physicalTo)
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

	physicalName, err := l.physicalName(name)
	if err != nil {
		return nil, err
	}

	var osfi os.FileInfo
	if osfi, err = os.Stat(physicalName); err != nil {
		return
	}
	return NewLocalFileInfo(l, osfi, l.logicalNameFromPhysical(physicalName)), nil
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

	physicalName, err := l.physicalName(name)
	if err != nil {
		return nil, err
	}

	var nameFi os.FileInfo
	if nameFi, err = os.Stat(physicalName); err != nil {
		return
	}
	if !nameFi.IsDir() {
		err = ErrNotADirectory
		return
	}

	var dirEntries []os.DirEntry
	if dirEntries, err = os.ReadDir(physicalName); err != nil {
		return
	}
	fi = make(FilesInfo, len(dirEntries))
	for i := range dirEntries {
		var finfo os.FileInfo
		if finfo, err = dirEntries[i].Info(); err != nil {
			return
		}
		fi[i] = NewLocalFileInfo(l, finfo,
			l.logicalNameFromPhysical(filepath.Join(physicalName, dirEntries[i].Name())))
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

	physicalRoot, err := l.physicalName(root)
	if err != nil {
		return err
	}

	return filepath.WalkDir(physicalRoot, func(name string, info fs.DirEntry, err error) error {
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
		logicalName := l.logicalNameFromPhysical(name)
		return walkDirFunc(logicalName, LocalDirEntry{fi: NewLocalFileInfo(l, infoInfo, logicalName)}, err)
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
