package filesystem

import "os"

// LocalFile represents local file
type LocalFile struct {
	*os.File
	local *Local
	name  string
}

// FS provides access to the associated FileSystem
func (lf *LocalFile) FS() FileSystem { return lf.local }

// Name returns the filesystem-scoped file name.
func (lf *LocalFile) Name() string {
	if lf.name != "" || lf.local.rooted() {
		return lf.name
	}
	return lf.File.Name()
}
