package filesystem

import (
	"io/fs"
)

// LocalDirEntry implements DirEntry
type LocalDirEntry struct{ fi FileInfo }

// FullName makes LocalDirEntry to implement DirEntry. Returns full name of the underlying FileInfo object
func (s LocalDirEntry) FullName() string { return s.fi.FullName() }

// Name makes LocalDirEntry to implement DirEntry. Returns name of the underlying FileInfo object
func (s LocalDirEntry) Name() string { return s.fi.Name() }

// IsDir makes LocalDirEntry to implement DirEntry. It calls IsDir() of the underlying FileInfo object
func (s LocalDirEntry) IsDir() bool { return s.fi.IsDir() }

// Type makes LocalDirEntry to implement DirEntry. It calls Mode() of the underlying FileInfo object
func (s LocalDirEntry) Type() fs.FileMode { return s.fi.Mode() }

// Info makes LocalDirEntry to implement DirEntry. It returns the underlying FileInfo object
func (s LocalDirEntry) Info() (fs.FileInfo, error) { return s.fi, nil }
