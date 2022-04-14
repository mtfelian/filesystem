package filesystem

import (
	"sync"
	"time"
)

// S3OpenedFilesListEntry is an walkDirEntry of opened files list
type S3OpenedFilesListEntry struct {
	mu     sync.RWMutex
	Added  time.Time
	S3File *S3OpenedFile
}

// Lock or RLock the entry according to S3File Open mode
func (s3ofle *S3OpenedFilesListEntry) Lock() {
	if s3ofle.S3File.mode == fileModeOpen {
		s3ofle.mu.RLock()
	} else {
		s3ofle.mu.Lock()
	}
}

// Unlock or RUnlock the entry according to S3File Open mode
func (s3ofle *S3OpenedFilesListEntry) Unlock() {
	if s3ofle.S3File.mode == fileModeOpen {
		s3ofle.mu.RUnlock()
	} else {
		s3ofle.mu.Unlock()
	}
}

// S3OpenedFilesList represents S3 opened files list
type S3OpenedFilesList struct {
	sync.Mutex
	m map[string]*S3OpenedFilesListEntry // map of local file name to S3OpenedFilesListEntry
}

// NewS3OpenedFilesList returns a pointer to new S3 opened files list instance
func NewS3OpenedFilesList() *S3OpenedFilesList {
	return &S3OpenedFilesList{m: make(map[string]*S3OpenedFilesListEntry)}
}

// Map returns opened file list underlying map
func (s3ofl *S3OpenedFilesList) Map() map[string]*S3OpenedFilesListEntry { return s3ofl.m }
