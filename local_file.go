package filesystem

import "os"

// LocalFile represents local file
type LocalFile struct {
	*os.File
	local *Local
}

// FS provides access to the associated FileSystem
func (lf *LocalFile) FS() FileSystem { return lf.local }
