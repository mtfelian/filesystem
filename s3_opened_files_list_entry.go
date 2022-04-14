package filesystem

import (
	"sync"
	"time"
)

// S3OpenedFilesListEntry is an walkDirEntry of opened files list
type S3OpenedFilesListEntry struct {
	mu     sync.Mutex
	Added  time.Time
	S3File *S3OpenedFile
}

// Lock or RLock the entry according to S3File Open mode
func (ofle *S3OpenedFilesListEntry) Lock() { ofle.mu.Lock() }

// Unlock or RUnlock the entry according to S3File Open mode
func (ofle *S3OpenedFilesListEntry) Unlock() { ofle.mu.Unlock() }
