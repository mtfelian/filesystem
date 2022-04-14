package filesystem

import (
	"sync"
)

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
func (ofl *S3OpenedFilesList) Map() map[string]*S3OpenedFilesListEntry {
	ofl.Lock()
	defer ofl.Unlock()
	return ofl.m
}

// Len returns length of list
func (ofl *S3OpenedFilesList) Len() int {
	ofl.Lock()
	defer ofl.Unlock()
	return len(ofl.m)
}

// AddAndLockEntry adds an entry to the list and locks it
func (ofl *S3OpenedFilesList) AddAndLockEntry(localFileName string, entry *S3OpenedFilesListEntry) {
	entry.Lock()
	ofl.Lock()
	defer ofl.Unlock()
	ofl.m[localFileName] = entry
}

// DeleteAndUnlockEntry deletes and unlocks an entry from the list if it exists
func (ofl *S3OpenedFilesList) DeleteAndUnlockEntry(localFileName string) {
	ofl.Lock()
	defer ofl.Unlock()
	if entry := ofl.m[localFileName]; entry != nil {
		delete(ofl.m, localFileName)
		entry.Unlock()
	}
}

// existsEntry returns whether the entry exists
func (ofl *S3OpenedFilesList) existsEntry(localFileName string) bool {
	ofl.Lock()
	defer ofl.Unlock()
	_, ok := ofl.m[localFileName]
	return ok
}
