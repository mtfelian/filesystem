package filesystem

import (
	"bytes"
	"context"
	"io"
	"io/fs"
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
func (of *S3OpenedFile) Sync() error { // todo does it work as intended?
	if err := of.Underlying().Sync(); err != nil { // does this work?
		return err
	}
	offset, err := of.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	if _, err := of.Seek(0, io.SeekStart); err != nil {
		return err
	}
	buf := bytes.NewBuffer([]byte{})
	if _, err := io.Copy(buf, of.Underlying()); err != nil {
		return err
	}
	if _, err := of.Seek(offset, io.SeekStart); err != nil {
		return err
	}
	if err := of.s3.WriteFile(of.ctx, of.objectName, buf.Bytes()); err != nil { // write it into S3 storage
		return err
	}
	buf.Reset()
	of.changed = false
	return nil
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

	b, err := of.s3.openedFilesLocalFS.ReadFile(of.ctx, of.localName) // re-read local file
	if err != nil {
		return err
	}

	if of.changed {
		if err := of.s3.WriteFile(of.ctx, of.objectName, b); err != nil { // write it into S3 storage
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
