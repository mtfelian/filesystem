package filesystem

import (
	"sync"
	"time"
)

// S3OpenedFilesListEntry is an walkDirEntry of opened files list
type S3OpenedFilesListEntry struct {
	Added  time.Time
	S3File *S3OpenedFile
}

// S3OpenedFilesList represents S3 opened files list
type S3OpenedFilesList struct {
	sync.Mutex
	m map[string]S3OpenedFilesListEntry // map of local file name to S3OpenedFilesListEntry
}

// NewS3OpenedFilesList returns a pointer to new S3 opened files list instance
func NewS3OpenedFilesList() *S3OpenedFilesList {
	return &S3OpenedFilesList{m: make(map[string]S3OpenedFilesListEntry)}
}

// Map returns opened file list underlying map
func (s3c *S3OpenedFilesList) Map() map[string]S3OpenedFilesListEntry { return s3c.m }
