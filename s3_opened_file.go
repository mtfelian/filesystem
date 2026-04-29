package filesystem

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"sync"
)

// S3OpenedFile implements a wrapper around File
type S3OpenedFile struct {
	s3  *S3 // pointer back to S3 control object
	ctx context.Context

	underlyingMu sync.Mutex
	underlying   File // underlying local file

	localName  string // underlying local file path
	objectName string // S3 object key

	changed bool
}

// Underlying returns an underlying file
func (of *S3OpenedFile) Underlying() File {
	of.underlyingMu.Lock()
	defer of.underlyingMu.Unlock()
	return of.underlying
}

// SetUnderlying file
func (of *S3OpenedFile) SetUnderlying(f File) {
	of.underlyingMu.Lock()
	defer of.underlyingMu.Unlock()
	of.underlying = f
}

// Sync makes S3OpenedFile to implement File
func (of *S3OpenedFile) Sync() error {
	underlying := of.Underlying()
	if underlying == nil {
		return fs.ErrClosed
	}
	if err := underlying.Sync(); err != nil {
		return err
	}
	if err := of.writeLocalFileToObject(); err != nil {
		return err
	}
	of.changed = false
	return nil
}

func (of *S3OpenedFile) writeLocalFileToObject() (err error) {
	f, err := of.s3.openedFilesLocalFS.Open(of.ctx, of.localName)
	if err != nil {
		return err
	}

	fi, err := f.Stat()
	if err != nil {
		closeErr := f.Close()
		return errors.Join(err, closeErr)
	}

	contentType, err := detectContentType(f)
	if err != nil {
		closeErr := f.Close()
		return errors.Join(err, closeErr)
	}
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		closeErr := f.Close()
		return errors.Join(err, closeErr)
	}

	err = of.s3.writeReader(of.ctx, of.objectName, f, fi.Size(), contentType, true)
	closeErr := f.Close()
	if err != nil || closeErr != nil {
		return errors.Join(err, closeErr)
	}
	return nil
}

// detectContentType samples the file using the same first-512-byte sniffing rules as http.DetectContentType.
func detectContentType(f File) (string, error) {
	header := make([]byte, 512)
	n, err := f.Read(header)
	if err != nil && !errors.Is(err, io.EOF) {
		return "", err
	}
	return http.DetectContentType(header[:n]), nil
}

// Truncate makes S3OpenedFile to implement File
func (of *S3OpenedFile) Truncate(size int64) error {
	of.changed = true
	return of.Underlying().Truncate(size)
}

// Seek makes S3OpenedFile to implement File
func (of *S3OpenedFile) Seek(offset int64, whence int) (int64, error) {
	return of.Underlying().Seek(offset, whence)
}

// Stat makes S3OpenedFile to implement File
func (of *S3OpenedFile) Stat() (fs.FileInfo, error) { return of.Underlying().Stat() }

// Read makes S3OpenedFile to implement File
func (of *S3OpenedFile) Read(bytes []byte) (int, error) { return of.Underlying().Read(bytes) }

// ReadAt makes S3OpenedFile to implement File
func (of *S3OpenedFile) ReadAt(p []byte, off int64) (n int, err error) {
	return of.Underlying().ReadAt(p, off)
}

// Write makes S3OpenedFile to implement File
func (of *S3OpenedFile) Write(p []byte) (n int, err error) {
	of.changed = true
	return of.Underlying().Write(p)
}

// Close makes S3OpenedFile to implement File. It closed the underlying File and removes it from local file system.
func (of *S3OpenedFile) Close() error {
	// unlock and delete opened files list entry
	defer of.s3.OpenedFilesList().DeleteAndUnlockEntry(of.localName)

	underlying := of.Underlying()
	if underlying == nil {
		of.s3.logger.Warnf("S3OpenedFile.Close: underlying is nil on file %q", of.localName)
		return nil
	}

	if err := underlying.Close(); err != nil { // close the underlying file
		of.s3.logger.Errorf("failed to underlying.Close() on file %q: %v", of.localName, err)
		return err
	}

	if of.changed {
		if err := of.writeLocalFileToObject(); err != nil {
			return err
		}
		of.changed = false
	}

	exists, err := of.s3.openedFilesLocalFS.Exists(of.ctx, of.localName) // if local file still exists...
	if err != nil {
		of.s3.logger.Errorf("failed to of.fsLocal.Exists() on file %q: %v", of.localName, err)
		return err
	}
	if exists { // then remove it
		if err := of.s3.openedFilesLocalFS.Remove(of.ctx, of.localName); err != nil {
			of.s3.logger.Errorf("failed to of.fsLocal.Remove() on file %q: %v", of.localName, err)
			return err
		}
	}
	return nil
}

// LocalName returns local file name
func (of *S3OpenedFile) LocalName() string { return of.localName }

// Name returns S3 object name
func (of *S3OpenedFile) Name() string { return of.objectName }

// FS provides access to the associated FileSystem
func (of *S3OpenedFile) FS() FileSystem { return of.s3 }
