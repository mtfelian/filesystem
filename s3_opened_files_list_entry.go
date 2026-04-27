package filesystem

import (
	"sync"
	"time"
)

// S3OpenedFilesListEntry is an walkDirEntry of opened files list
type S3OpenedFilesListEntry struct {
	mu        sync.Mutex
	readyOnce sync.Once
	ready     chan struct{}
	openErr   error
	Added     time.Time
	S3File    *S3OpenedFile
}

// Lock or RLock the entry according to S3File Open mode
func (ofle *S3OpenedFilesListEntry) Lock() { ofle.mu.Lock() }

// Unlock or RUnlock the entry according to S3File Open mode
func (ofle *S3OpenedFilesListEntry) Unlock() { ofle.mu.Unlock() }

// MarkReady unblocks callers waiting for the initial open to finish.
func (ofle *S3OpenedFilesListEntry) MarkReady(err error) {
	ofle.openErr = err
	ofle.readyOnce.Do(func() {
		if ofle.ready != nil {
			close(ofle.ready)
		}
	})
}
