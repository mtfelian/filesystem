package filesystem

import (
	"bytes"
	"io"
	"io/fs"
)

// S3OpenedFile implements a wrapper around File
type S3OpenedFile struct {
	s3         *S3    // pointer back to S3 control object
	underlying File   // underlying local file
	localName  string // underlying local file path
	objectName string // S3 object key
	mode       int

	changed bool
}

// Sync makes S3OpenedFile to implement File
func (of *S3OpenedFile) Sync() error { // todo does it work as intended?
	if err := of.underlying.Sync(); err != nil { // does this work?
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
	if _, err := io.Copy(buf, of.underlying); err != nil {
		return err
	}
	if _, err := of.Seek(offset, io.SeekStart); err != nil {
		return err
	}
	if err := of.s3.WriteFile(of.objectName, buf.Bytes()); err != nil { // write it into S3 storage
		return err
	}
	buf.Reset()
	of.changed = false
	return nil
}

// Truncate makes S3OpenedFile to implement File
func (of *S3OpenedFile) Truncate(size int64) error {
	of.changed = true
	return of.underlying.Truncate(size)
}

// Seek makes S3OpenedFile to implement File
func (of *S3OpenedFile) Seek(offset int64, whence int) (int64, error) {
	return of.underlying.Seek(offset, whence)
}

// Stat makes S3OpenedFile to implement File
func (of *S3OpenedFile) Stat() (fs.FileInfo, error) { return of.underlying.Stat() }

// Read makes S3OpenedFile to implement File
func (of *S3OpenedFile) Read(bytes []byte) (int, error) { return of.underlying.Read(bytes) }

// ReadAt makes S3OpenedFile to implement File
func (of *S3OpenedFile) ReadAt(p []byte, off int64) (n int, err error) {
	return of.underlying.ReadAt(p, off)
}

// Write makes S3OpenedFile to implement File
func (of *S3OpenedFile) Write(p []byte) (n int, err error) {
	of.changed = true
	return of.underlying.Write(p)
}

// Close makes S3OpenedFile to implement File. It closed the underlying File and removes it from local file system.
func (of *S3OpenedFile) Close() error {
	if err := of.underlying.Close(); err != nil { // close the underlying file
		of.s3.logger.Errorf("failed to of.underlying.Close() on file %q: %v", of.localName, err)
		return err
	}

	b, err := of.s3.openedFilesLocalFS.ReadFile(of.localName) // re-read local file
	if err != nil {
		return err
	}

	if err := of.s3.WriteFile(of.objectName, b); err != nil { // write it into S3 storage
		return err
	}
	of.changed = false

	defer func() { // unlock and delete opened files list entry
		of.s3.OpenedFilesListLock()
		defer of.s3.OpenedFilesListUnlock()
		if entry := of.s3.OpenedFilesList().m[of.localName]; entry != nil {
			entry.Unlock()
			delete(of.s3.OpenedFilesList().m, of.localName)
		}
	}()

	exists, err := of.s3.openedFilesLocalFS.Exists(of.localName) // if local file still exists...
	if err != nil {
		of.s3.logger.Errorf("failed to of.fsLocal.Exists() on file %q: %v", of.localName, err)
		return err
	}
	if exists { // then remove it
		if err := of.s3.openedFilesLocalFS.Remove(of.localName); err != nil {
			of.s3.logger.Errorf("failed to of.fsLocal.Remove() on file %q: %v", of.localName, err)
			return err
		}
	}
	return nil
}

// Name returns local file name
func (of *S3OpenedFile) Name() string { return of.localName }
