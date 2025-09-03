package filesystem

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/sirupsen/logrus"
)

// constants regarding stubs and directories
const (
	DirStubFileName    = ".dir" // name of stub file to imitate empty folder
	DirStubFileContent = "!"    // content of stub file to imitate empty folder
	TempDir            = "tmp"
)

// ErrSkipDir should be returned from WalkDirFunc to skip walking inside directory
var ErrSkipDir = fs.SkipDir

// errors
var (
	ErrCantOpenS3Directory           = errors.New("can't open S3 directory")
	ErrDirectoryNotEmpty             = errors.New("directory not empty")
	ErrDestinationPathIsNotDirectory = errors.New("destination path is not directory while source is")
	ErrCantUseRenameWithStubObject   = errors.New("can't use rename with stub object")
	ErrRenamingNonExistentDirectory  = errors.New("can't rename non-existent directory")
	ErrNotADirectory                 = errors.New("given path is not a directory")
	ErrDirectoryNotExists            = errors.New("directory not exists")
	ErrUnknownFileMode               = errors.New("unknown file mode")
	ErrFileAlreadyOpened             = errors.New("file already opened")
)

// S3 implements FileSystem. The implementation is not concurrent-safe
type S3 struct {
	endpoint  string
	region    string
	accessKey string
	secretKey string
	logger    logrus.FieldLogger

	useSSL      bool
	bucketName  string
	minioClient *minio.Client

	openedFilesLocalFS *Local
	openedFilesList    *S3OpenedFilesList
	openedFilesTTL     time.Duration
	openedFilesTempDir string
	cleaningC          chan struct{}

	emulateEmptyDirs     bool
	listDirectoryEntries bool
}

// NewS3 returns a pointer to a new Local object
func NewS3(ctx context.Context, p S3Params) (s3 *S3, err error) {
	if ctx, err = invokeBeforeOperationCB(ctx); err != nil {
		return
	}
	defer func() {
		if errcb := invokeAfterOperationCB(ctx); err == nil {
			err = errcb
		} // else drop callback error
	}()

	p.applyDefaults()
	s3 = &S3{
		endpoint:   p.Endpoint,
		region:     p.Region,
		accessKey:  p.AccessKey,
		secretKey:  p.SecretKey,
		useSSL:     p.UseSSL,
		bucketName: p.BucketName,
		logger:     p.Logger,

		openedFilesList:    NewS3OpenedFilesList(),
		openedFilesTTL:     p.OpenedFilesTTL,
		openedFilesLocalFS: NewLocal().(*Local),
		openedFilesTempDir: p.OpenedFilesTempDir,
		cleaningC:          make(chan struct{}),

		emulateEmptyDirs:     p.EmulateEmptyDirs,
		listDirectoryEntries: p.ListDirectoryEntries,
	}

	if s3.minioClient, err = minio.New(s3.endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(s3.accessKey, s3.secretKey, ""),
		Secure: s3.useSSL,
		Region: s3.region,
	}); err != nil {
		return
	}

	var exists bool
	if exists, err = s3.minioClient.BucketExists(ctx, s3.bucketName); err != nil {
		return
	}
	if !exists {
		err = s3.minioClient.MakeBucket(ctx, s3.bucketName, minio.MakeBucketOptions{
			Region:        s3.region,
			ObjectLocking: false,
		})
	}
	if s3.emulateEmptyDirs {
		if err = s3.putStubObject(ctx, ""); err != nil {
			return s3, err
		}
	}

	go s3.openedFilesListCleaning()
	return
}

// Close attempts to forcibly close opened files and release resources
func (s *S3) Close() error {
	close(s.cleaningC)
	return nil
}

// Logger provides access to a logger
func (s *S3) Logger() logrus.FieldLogger { return s.logger }

// SetListDirectoryEntries or unset it, use mainly for tests
func (s *S3) SetListDirectoryEntries(v bool) { s.listDirectoryEntries = v }

// MinioClient provides access to Minio Client, use mainly for tests
func (s *S3) MinioClient() *minio.Client { return s.minioClient }

// OpenedFilesList provides access to opened files list, use mainly for tests
func (s *S3) OpenedFilesList() *S3OpenedFilesList { return s.openedFilesList }

// OpenedFilesListLock locks opened files list associated mutex, use mainly for tests
func (s *S3) OpenedFilesListLock() { s.openedFilesList.Lock() }

// OpenedFilesListUnlock unlocks opened files list associated mutex, use mainly for tests
func (s *S3) OpenedFilesListUnlock() { s.openedFilesList.Unlock() }

func (s *S3) now() time.Time { return time.Now() }

var driveLetterRegexp = regexp.MustCompile(`^[A-Za-z?]:`)

func (s *S3) nameToDir(name string) string {
	if !strings.HasSuffix(name, "/") {
		name += "/"
	}
	return name
}
func (s *S3) nameToStub(name string) string {
	return s.nameToDir(name) + DirStubFileName
}
func (s *S3) nameIsADirectory(name string) bool {
	return s.nameIsADirectoryPath(name) || s.nameIsADirectoryStub(name)
}
func (s *S3) nameIsADirectoryPath(name string) bool { return strings.HasSuffix(name, "/") }
func (s *S3) nameIsADirectoryStub(name string) bool {
	return strings.HasSuffix(name, "/"+DirStubFileName)
}
func (s *S3) stubToDir(name string) string {
	if !s.nameIsADirectoryStub(name) {
		return name
	}
	return strings.TrimPrefix(s.Dir(name)+"/", ".")
}
func (s *S3) normalizeName(name string) string {
	if len(name) == 0 {
		name = "/"
	}
	isDir := s.nameIsADirectoryPath(name)
	name = driveLetterRegexp.ReplaceAllString(name, "")
	name = path.Clean(name)
	name = strings.ReplaceAll(name, `\`, `/`)
	if !strings.HasPrefix(name, "/") {
		name = "/" + name
	}
	if isDir && name != "/" {
		name += "/"
	}
	return name
}

func (s *S3) openedFilesListCleaning() {
	cleanupFunc := func(all bool) {
		var s3FilesToClose []*S3OpenedFile
		func() {
			s.OpenedFilesListLock()
			defer s.OpenedFilesListUnlock()
			for key, value := range s.openedFilesList.m {
				if !all && s.now().Before(value.Added.Add(s.openedFilesTTL)) { // should not be purged yet
					continue
				}
				s3FilesToClose = append(s3FilesToClose, s.openedFilesList.m[key].S3File)
			}
		}()
		if len(s3FilesToClose) > 0 {
			s.logger.Infof("S3.openedFilesListCleaning: closing %d file(s)", len(s3FilesToClose))
		}
		for _, s3File := range s3FilesToClose {
			s.logger.Infof("S3.openedFilesListCleaning: autoclosing file %q", s3File.localName)
			if err := s3File.Close(); err != nil {
				s.logger.Errorf("S3.openedFilesListCleaning: failed to s3File.Close(): %v", err)
			}
		}
	}
	t := time.NewTicker(s.openedFilesTTL)
	for {
		select {
		case <-t.C:
			cleanupFunc(false)
		case <-s.cleaningC:
			s.logger.Info("S3.openedFilesListCleaning doing last cleanup")
			cleanupFunc(true)
			s.logger.Info("S3.openedFilesListCleaning terminated")
			return
		}
	}
}

// TempFileName converts file name to a temporary file name
func (s *S3) TempFileName(name string) string {
	return s.openedFilesLocalFS.Join(s.openedFilesTempDir, TempDir, strings.ReplaceAll(name, "/", "__"))
}

const (
	fileModeOpen = iota
	fileModeCreate
	fileModeWrite
)

func (s *S3) openFile(ctx context.Context, name string, fileMode int) (f File, err error) {
	if ctx, err = invokeBeforeOperationCB(ctx); err != nil {
		return
	}
	defer func() {
		if errcb := invokeAfterOperationCB(ctx); err == nil {
			err = errcb
		} // else drop callback error
	}()

	if fileMode > fileModeWrite || fileMode < fileModeOpen {
		return nil, ErrUnknownFileMode
	}
	if name = s.normalizeName(name); s.nameIsADirectory(name) {
		return nil, ErrCantOpenS3Directory
	}

	localFileName := s.TempFileName(name)

	var s3OpenedFile *S3OpenedFilesListEntry
	func() { // checking lock on entry
		s.OpenedFilesListLock()
		defer s.OpenedFilesListUnlock()
		s3OpenedFile, _ = s.openedFilesList.Map()[localFileName]
	}()

	if s3OpenedFile != nil {
		return s3OpenedFile.S3File, ErrFileAlreadyOpened
	}

	if err = s.openedFilesLocalFS.MakePathAll(ctx, s.openedFilesLocalFS.Dir(localFileName)); err != nil {
		return nil, err
	}

	if s3OpenedFile == nil {
		s3OpenedFile = &S3OpenedFilesListEntry{
			Added: s.now(),
			S3File: &S3OpenedFile{
				ctx:        ctx,
				s3:         s,
				underlying: nil, // to be written below
				localName:  localFileName,
				objectName: name,
			},
		}
	}
	s.openedFilesList.AddAndLockEntry(localFileName, s3OpenedFile)

	defer func() {
		if err == nil {
			return
		}
		// only if error occured later, so we are in defer:
		s.openedFilesList.DeleteAndUnlockEntry(localFileName)
		if f != nil {
			_ = f.Close()
		}
	}()

	if fileMode != fileModeCreate { // fileModeOpen or fileModeWrite, so we create local file from S3 object
		var object *minio.Object
		if object, err = s.minioClient.GetObject(ctx, s.bucketName, name, minio.GetObjectOptions{}); err != nil {
			return nil, err
		}
		defer object.Close()
		if err = func() error {
			localFile, err := s.openedFilesLocalFS.Create(ctx, localFileName)
			if err != nil {
				return err
			}
			defer localFile.Close()
			_, err = io.Copy(localFile, object)
			return err
		}(); err != nil {
			return nil, err
		}
	}

	switch fileMode {
	case fileModeOpen:
		f, err = s.openedFilesLocalFS.Open(ctx, localFileName)
	case fileModeCreate:
		f, err = s.openedFilesLocalFS.Create(ctx, localFileName)
	case fileModeWrite:
		f, err = s.openedFilesLocalFS.OpenW(ctx, localFileName)
	}
	s3OpenedFile.S3File.SetUnderlying(f)

	return s3OpenedFile.S3File, err
}

// Open file with given name in the client's bucket.
// An object will be downloaded from S3 storage and opened as a local file for reading.
// To remove the actual local file and write out into S3 object
// it should be properly closed by calling Close() on the caller's side.
// Calls to Open, Create, OpenW and S3OpenedFile.Close are concurrent-safe and mutually locking.
func (s *S3) Open(ctx context.Context, name string) (f File, err error) {
	if ctx, err = invokeBeforeOperationCB(ctx); err != nil {
		return
	}
	defer func() {
		if errcb := invokeAfterOperationCB(ctx); err == nil {
			err = errcb
		} // else drop callback error
	}()

	return s.openFile(ctx, name, fileModeOpen)
}

// Create file with given name in the client's bucket.
// A file will be created locally for reading, writing, truncating.
// To remove the actual local file and write out into S3 object
// it should be properly closed by calling Close() on the caller's side.
// Calls to Open, Create, OpenW and S3OpenedFile.Close are concurrent-safe and mutually locking.
func (s *S3) Create(ctx context.Context, name string) (f File, err error) {
	if ctx, err = invokeBeforeOperationCB(ctx); err != nil {
		return
	}
	defer func() {
		if errcb := invokeAfterOperationCB(ctx); err == nil {
			err = errcb
		} // else drop callback error
	}()

	if dir := s.Dir(name); dir != "." && dir != "/" {
		if err = s.MakePathAll(ctx, dir); err != nil {
			return
		}
	}
	return s.openFile(ctx, name, fileModeCreate)
}

// OpenW opens file in the FileSystem for writing.
// An object will be downloaded from S3 storage and opened as a local file for writing.
// To remove the actual local file and write out into S3 object
// it should be properly closed by calling Close() on the caller's side.
// Calls to Open, Create, OpenW and S3OpenedFile.Close are concurrent-safe and mutually locking.
func (s *S3) OpenW(ctx context.Context, name string) (f File, err error) {
	if ctx, err = invokeBeforeOperationCB(ctx); err != nil {
		return
	}
	defer func() {
		if errcb := invokeAfterOperationCB(ctx); err == nil {
			err = errcb
		} // else drop callback error
	}()

	if dir := s.Dir(name); dir != "." && dir != "/" {
		if err = s.MakePathAll(ctx, dir); err != nil {
			return
		}
	}
	return s.openFile(ctx, name, fileModeWrite)
}

// ReadFile by it's name from the client's bucket
func (s *S3) ReadFile(ctx context.Context, name string) (b []byte, err error) {
	if ctx, err = invokeBeforeOperationCB(ctx); err != nil {
		return
	}
	defer func() {
		if errcb := invokeAfterOperationCB(ctx); err == nil {
			err = errcb
		} // else drop callback error
	}()

	name = s.normalizeName(name)
	var o *minio.Object
	if o, err = s.minioClient.GetObject(ctx, s.bucketName, name, minio.GetObjectOptions{}); err != nil {
		return
	}
	defer o.Close()
	return io.ReadAll(o)
}

// WriteFile by it's name to the client's bucket
func (s *S3) WriteFile(ctx context.Context, name string, b []byte) (err error) {
	if ctx, err = invokeBeforeOperationCB(ctx); err != nil {
		return
	}
	defer func() {
		if errcb := invokeAfterOperationCB(ctx); err == nil {
			err = errcb
		} // else drop callback error
	}()

	name = s.normalizeName(name)
	if s.emulateEmptyDirs {
		if dir := s.Dir(name); dir != "." && dir != "/" {
			if err = s.MakePathAll(ctx, dir); err != nil {
				return
			}
		}
	}
	_, err = s.minioClient.PutObject(ctx, s.bucketName, name, bytes.NewReader(b), int64(len(b)),
		minio.PutObjectOptions{ContentType: http.DetectContentType(b)})
	return err
}

// WriteFiles by the data given. An archive will be created by the underlying minio client
func (s *S3) WriteFiles(ctx context.Context, f []FileNameData) (err error) {
	if ctx, err = invokeBeforeOperationCB(ctx); err != nil {
		return
	}
	defer func() {
		if errcb := invokeAfterOperationCB(ctx); err == nil {
			err = errcb
		} // else drop callback error
	}()

	snowBallC := make(chan minio.SnowballObject)
	for i, el := range f {
		f[i].Name = s.normalizeName(el.Name)
		if s.emulateEmptyDirs {
			// todo may be optimized if many files have to be written in same subdir structure via a tree
			if dir := s.Dir(el.Name); dir != "." && dir != "/" {
				if err = s.MakePathAll(ctx, dir); err != nil {
					return
				}
			}
		}
	}
	go func() {
		for i := range f {
			snowBallC <- minio.SnowballObject{
				Key:     f[i].Name,
				Size:    int64(len(f[i].Data)),
				ModTime: s.now(),
				Content: bytes.NewReader(f[i].Data),
			}
		}
		close(snowBallC)
	}()
	return s.minioClient.PutObjectsSnowball(ctx, s.bucketName, minio.SnowballOptions{Compress: true}, snowBallC)
}

// Reader returns reader by it's name
func (s *S3) Reader(ctx context.Context, name string) (r io.ReadCloser, err error) {
	if ctx, err = invokeBeforeOperationCB(ctx); err != nil {
		return
	}
	defer func() {
		if errcb := invokeAfterOperationCB(ctx); err == nil {
			err = errcb
		} // else drop callback error
	}()

	name = s.normalizeName(name)
	return s.minioClient.GetObject(ctx, s.bucketName, name, minio.GetObjectOptions{})
}

// Count returns count of items in a folder. May count in childs also if recursive param set to true.
func (s *S3) Count(ctx context.Context, name string, recursive bool,
	countFunc func(oi minio.ObjectInfo, num int64) (proceed bool, e error)) (c int64, err error) {
	if ctx, err = invokeBeforeOperationCB(ctx); err != nil {
		return
	}
	defer func() {
		if errcb := invokeAfterOperationCB(ctx); err == nil {
			err = errcb
		} // else drop callback error
	}()

	name = s.normalizeName(name)

	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()
	for objectInfo := range s.minioClient.ListObjects(ctx, s.bucketName, minio.ListObjectsOptions{
		Prefix:    strings.TrimPrefix(name, "/"),
		Recursive: recursive,
	}) {
		c++
		if countFunc != nil {
			proceed, e := countFunc(objectInfo, c)
			if !proceed {
				break
			}
			if e != nil {
				return c, e
			}
		}
		if objectInfo.Err != nil {
			return c, objectInfo.Err
		}
	}
	return c, nil
}

// Exists checks whether an object exists
func (s *S3) Exists(ctx context.Context, name string) (e bool, err error) {
	if ctx, err = invokeBeforeOperationCB(ctx); err != nil {
		return
	}
	defer func() {
		if errcb := invokeAfterOperationCB(ctx); err == nil {
			err = errcb
		} // else drop callback error
	}()

	name = s.normalizeName(name)
	if !s.nameIsADirectoryPath(name) { // not a folder
		_, err = s.minioClient.StatObject(ctx, s.bucketName, name, minio.StatObjectOptions{})
		switch {
		case err == nil:
			return true, nil
		case s.IsNotExist(err):
			return false, nil
		default:
			return false, err
		}
	}

	// if name is a folder path
	// we may check for just a stub file. But we do a more thorough check
	// for a case if stub file will not exist.
	var count int64
	count, err = s.Count(ctx, name, true, nil)
	return count > 0, err
}

func (s *S3) putStubObject(ctx context.Context, name string) (err error) {
	if ctx, err = invokeBeforeOperationCB(ctx); err != nil {
		return
	}
	defer func() {
		if errcb := invokeAfterOperationCB(ctx); err == nil {
			err = errcb
		} // else drop callback error
	}()

	name = s.nameToStub(name)
	_, err = s.minioClient.PutObject(ctx, s.bucketName, name, strings.NewReader(DirStubFileContent),
		int64(len(DirStubFileContent)), minio.PutObjectOptions{
			DisableMultipart: true,
			ContentType:      "text/plain",
		})
	return
}

// MakePathAll does nothing in S3 FileSystem. As a workaround to make sure that we have a precreated folder,
// we create a small file in it.
// refer to: https://github.com/minio/minio/issues/3555, https://github.com/minio/minio/issues/2423
func (s *S3) MakePathAll(ctx context.Context, name string) (err error) {
	if !s.emulateEmptyDirs { // if no empty dirs allowed just do nothing
		return
	}

	if ctx, err = invokeBeforeOperationCB(ctx); err != nil {
		return
	}
	defer func() {
		if errcb := invokeAfterOperationCB(ctx); err == nil {
			err = errcb
		} // else drop callback error
	}()

	name = s.normalizeName(name)

	for ; name != "/"; name = s.Dir(name) {
		if err = s.putStubObject(ctx, name); err != nil {
			return
		}
	}
	return
}

// Remove object by the given name. Returns no error even if object does not exists
func (s *S3) Remove(ctx context.Context, name string) (err error) {
	if ctx, err = invokeBeforeOperationCB(ctx); err != nil {
		return
	}
	defer func() {
		if errcb := invokeAfterOperationCB(ctx); err == nil {
			err = errcb
		} // else drop callback error
	}()

	name = s.normalizeName(name)
	name = s.stubToDir(name)           // if stub, convert to dir with trailing '/'
	if !s.nameIsADirectoryPath(name) { // means was not a stub but a normal object name
		return s.minioClient.RemoveObject(ctx, s.bucketName, name, minio.RemoveObjectOptions{})
	}
	// if nameIsADirectoryPath

	var isEmpty bool
	if isEmpty, err = s.IsEmptyPath(ctx, name); err != nil {
		return
	}
	if !isEmpty {
		return ErrDirectoryNotEmpty
	}
	// if nameIsADirectoryPath && isEmpty

	if !s.emulateEmptyDirs {
		return
	}

	return s.minioClient.RemoveObject(ctx, s.bucketName, s.nameToStub(name), minio.RemoveObjectOptions{})
}

// RemoveFiles removes multiple objects in batch by the given names.
// Returns no error even if any object does not exists
func (s *S3) RemoveFiles(ctx context.Context, names []string) (failed []string, err error) {
	if ctx, err = invokeBeforeOperationCB(ctx); err != nil {
		return
	}
	defer func() {
		if errcb := invokeAfterOperationCB(ctx); err == nil {
			err = errcb
		} // else drop callback error
	}()

	objectInfoC := make(chan minio.ObjectInfo)
	idx := make([]int, 0, len(names))
	for i := range names {
		names[i] = s.normalizeName(names[i])
		names[i] = s.stubToDir(names[i]) // if stub, convert to dir with trailing '/'

		if !s.nameIsADirectoryPath(names[i]) || !s.emulateEmptyDirs { // means was not a stub but a normal object name
			idx = append(idx, i)
			continue
		}
		// if nameIsADirectoryPath

		var isEmpty bool
		if isEmpty, err = s.IsEmptyPath(ctx, names[i]); err != nil {
			return nil, err
		}
		if !isEmpty {
			return nil, ErrDirectoryNotEmpty
		}
		// if nameIsADirectoryPath && isEmpty
		idx = append(idx, i)
	}

	go func() {
		defer close(objectInfoC)
		for _, i := range idx {
			objectInfoC <- minio.ObjectInfo{Key: names[i]}
		}
	}()

	objectRemoveErrorC := s.minioClient.RemoveObjects(ctx, s.bucketName, objectInfoC, minio.RemoveObjectsOptions{})
	select {
	case ore, more := <-objectRemoveErrorC: // if no error, more will be false
		if more && ore.Err != nil {
			failed = append(failed, ore.ObjectName)
		}
	}
	return failed, nil
}

// RemoveAll objects by the given filepath
func (s *S3) RemoveAll(ctx context.Context, name string) (err error) {
	if ctx, err = invokeBeforeOperationCB(ctx); err != nil {
		return
	}
	defer func() {
		if errcb := invokeAfterOperationCB(ctx); err == nil {
			err = errcb
		} // else drop callback error
	}()

	name = s.normalizeName(name)
	ctx1, cancel1 := context.WithCancel(ctx)
	defer cancel1()
	objectInfoC := s.minioClient.ListObjects(ctx1, s.bucketName, minio.ListObjectsOptions{
		Prefix:    strings.TrimPrefix(name, "/"),
		Recursive: s.nameIsADirectory(name),
	})

	ctx2, cancel2 := context.WithCancel(ctx)
	defer cancel2()
	for roeC := range s.minioClient.RemoveObjects(ctx2, s.bucketName, objectInfoC, minio.RemoveObjectsOptions{}) {
		if roeC.Err != nil {
			return roeC.Err
		}
	}
	return nil
}

// IsNotExist returns whether err is an 'bucket not exists' error or 'object not exists' error
func (s *S3) IsNotExist(err error) bool {
	if err == nil {
		return false
	}
	// look https://github.com/minio/minio-go/issues/1082#issuecomment-468215014 for more details
	switch minio.ToErrorResponse(err).Code {
	case "NoSuchKey", "NoSuchBucket":
		return true
	default:
		return false
	}
}

// IsEmptyPath works according to the MakePathAll implementation.
// Returns true only if specified path contains only a dir stub file.
func (s *S3) IsEmptyPath(ctx context.Context, name string) (e bool, err error) {
	if ctx, err = invokeBeforeOperationCB(ctx); err != nil {
		return
	}
	defer func() {
		if errcb := invokeAfterOperationCB(ctx); err == nil {
			err = errcb
		} // else drop callback error
	}()

	name = s.normalizeName(name)
	name = s.nameToDir(name)

	if s.emulateEmptyDirs {
		var exists bool
		if exists, err = s.Exists(ctx, name); err != nil {
			return false, err
		}
		if !exists {
			return true, nil
		}
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var i int
	for objectInfo := range s.minioClient.ListObjects(ctx, s.bucketName, minio.ListObjectsOptions{
		Prefix:    strings.TrimPrefix(name, "/"),
		Recursive: true,
	}) {
		if objectInfo.Err != nil {
			return false, objectInfo.Err
		}
		i++
		if (i > 1 || !s.nameIsADirectoryStub("/"+objectInfo.Key)) && s.emulateEmptyDirs ||
			i > 0 && !s.emulateEmptyDirs {
			return false, nil
		}
	}
	return i == 1 && s.emulateEmptyDirs || i == 0 && !s.emulateEmptyDirs, nil
}

// PreparePath works according to the MakePathAll implementation.
func (s *S3) PreparePath(ctx context.Context, name string) (_ string, err error) {
	if ctx, err = invokeBeforeOperationCB(ctx); err != nil {
		return
	}
	defer func() {
		if errcb := invokeAfterOperationCB(ctx); err == nil {
			err = errcb
		} // else drop callback error
	}()

	name = s.normalizeName(name)
	if !s.emulateEmptyDirs { // if no empty dirs allowed just do nothing
		return name, nil
	}

	var exists bool
	if exists, err = s.Exists(ctx, name); !exists && err == nil {
		if err = s.MakePathAll(ctx, name); err != nil {
			return "", err
		}
	}
	return name, nil
}

// Rename object
func (s *S3) Rename(ctx context.Context, from string, to string) (err error) {
	if ctx, err = invokeBeforeOperationCB(ctx); err != nil {
		return
	}
	defer func() {
		if errcb := invokeAfterOperationCB(ctx); err == nil {
			err = errcb
		} // else drop callback error
	}()

	if from, to = s.normalizeName(from), s.normalizeName(to); from == to {
		return
	}

	if s.nameIsADirectoryStub(to) || s.nameIsADirectoryStub(from) {
		return ErrCantUseRenameWithStubObject
	}

	if !s.nameIsADirectory(from) { // normal object
		if dir := s.Dir(to); dir != "." && dir != "/" {
			if err = s.MakePathAll(ctx, dir); err != nil {
				return
			}
		}
		if _, err = s.minioClient.CopyObject(ctx,
			minio.CopyDestOptions{Bucket: s.bucketName, Object: to},
			minio.CopySrcOptions{Bucket: s.bucketName, Object: from}); err != nil {
			return
		}
		return s.minioClient.RemoveObject(ctx, s.bucketName, from, minio.RemoveObjectOptions{})
	}
	// if s.nameIsADirectory(from)

	if !s.nameIsADirectoryPath(to) {
		return ErrDestinationPathIsNotDirectory
	}

	var exists bool
	if exists, err = s.Exists(ctx, from); err != nil {
		return
	}
	if !exists {
		return ErrRenamingNonExistentDirectory
	}

	ctx1, cancel1 := context.WithCancel(ctx)
	defer cancel1()
	for objectInfo := range s.minioClient.ListObjects(ctx1, s.bucketName, minio.ListObjectsOptions{
		Prefix:    strings.TrimPrefix(from, "/"),
		Recursive: true,
	}) {
		objTo := to + strings.TrimPrefix("/"+objectInfo.Key, from)
		if dir := s.Dir(objTo); dir != "." && dir != "/" {
			if err = s.MakePathAll(ctx, dir); err != nil {
				return
			}
		}

		if _, err = s.minioClient.CopyObject(ctx,
			minio.CopyDestOptions{Bucket: s.bucketName, Object: objTo},
			minio.CopySrcOptions{Bucket: s.bucketName, Object: objectInfo.Key}); err != nil {
			return
		}
		if err = s.minioClient.RemoveObject(ctx, s.bucketName, objectInfo.Key, minio.RemoveObjectOptions{}); err != nil {
			s.logger.Errorf("S3.Rename: failed to remove object %q while batch moving", objectInfo.Key)
			return
		}
	}
	return nil
}

// Stat returns S3 object information as FileInfo interface
func (s *S3) Stat(ctx context.Context, name string) (fi FileInfo, err error) {
	if ctx, err = invokeBeforeOperationCB(ctx); err != nil {
		return
	}
	defer func() {
		if errcb := invokeAfterOperationCB(ctx); err == nil {
			err = errcb
		} // else drop callback error
	}()

	name = s.normalizeName(name)
	if s.nameIsADirectoryPath(name) && s.emulateEmptyDirs {
		var objectInfo minio.ObjectInfo
		if objectInfo, err = s.minioClient.StatObject(ctx, s.bucketName, s.nameToStub(name),
			minio.StatObjectOptions{}); err != nil {
			return
		}
		// objectInfo.LastModified may be zero struct
		return NewS3FileInfoStub(s, name, objectInfo.LastModified), nil
	}
	// if !s.nameIsADirectory(name) || !s.emulateEmptyDirs

	if s.nameIsADirectoryPath(name) {
		var c int64
		if c, err = s.Count(ctx, name, true, nil); err != nil {
			return
		}
		if c > 0 { // directory virtually exists
			// modTime is not available when empty dirs are not emulated
			return NewS3FileInfoStub(s, name, time.Time{}), nil
		}
		return nil, ErrDirectoryNotExists
	}
	// if (!s.nameIsADirectory(name) || !s.emulateEmptyDirs) && !s.nameIsADirectoryPath(name)

	var objectInfo minio.ObjectInfo
	if objectInfo, err = s.minioClient.StatObject(ctx, s.bucketName, name, minio.StatObjectOptions{}); err != nil {
		return
	}
	return NewS3FileInfo(s, objectInfo), nil
}

// ReadDir simulates directory reading by the given name
func (s *S3) ReadDir(ctx context.Context, name string) (fi FilesInfo, err error) {
	if ctx, err = invokeBeforeOperationCB(ctx); err != nil {
		return
	}
	defer func() {
		if errcb := invokeAfterOperationCB(ctx); err == nil {
			err = errcb
		} // else drop callback error
	}()

	name = s.normalizeName(name)
	if !s.nameIsADirectory(name) {
		return nil, ErrNotADirectory
	}
	name = s.stubToDir(name)

	dirMap := make(map[string]struct{})

	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()
	fi = make(FilesInfo, 0)

	// reading files
	for objectInfo := range s.minioClient.ListObjects(ctx, s.bucketName, minio.ListObjectsOptions{
		Prefix:    strings.TrimPrefix(name, "/"),
		Recursive: false,
	}) {
		if objectInfo.Err != nil {
			return fi, objectInfo.Err
		}
		if s.nameIsADirectory(objectInfo.Key) {
			continue
		}
		if !strings.HasPrefix(objectInfo.Key, "/") { // add leading '/'
			objectInfo.Key = "/" + objectInfo.Key
		}
		fi = append(fi, NewS3FileInfo(s, objectInfo))
	}

	if !s.listDirectoryEntries {
		return fi, nil
	}

	// reading dir entries

	for objectInfo := range s.minioClient.ListObjects(ctx, s.bucketName, minio.ListObjectsOptions{
		Prefix:    strings.TrimPrefix(name, "/"),
		Recursive: true,
	}) {
		if objectInfo.Err != nil {
			return fi, objectInfo.Err
		}

		// only current level directories
		var key string
		parent := func() string { return s.Dir("/"+objectInfo.Key) + "/" }
		stillNotRoot := func() bool { return len(strings.TrimRight(key, "/")) > 0 }
		upwards := func() string { return s.Dir(strings.TrimSuffix(key, "/")) + "/" }
		for key = parent(); stillNotRoot(); key = upwards() {
			if strings.HasPrefix(key, name) && strings.Count(strings.TrimPrefix(key, name), "/") <= 1 && key != name {
				dirMap[key] = struct{}{}
			}
		}
	}

	for dirName := range dirMap { // adding directory entries to the list
		s3fi := NewS3FileInfoStub(s, dirName, time.Time{})
		if s.emulateEmptyDirs { // may request stat
			var o FileInfo
			if o, err = s.Stat(ctx, s.nameToStub(dirName)); err != nil {
				return fi, err
			}
			s3fi.oi.LastModified = o.ModTime()
		}

		fi = append(fi, s3fi)
	}
	return fi, nil
}

// walkDir recursively descends path, calling walkDirFunc
func (s *S3) walkDir(ctx context.Context, name string, d DirEntry, walkDirFunc WalkDirFunc) (err error) {
	name = s.normalizeName(name)
	if err = walkDirFunc(name, d, nil); err != nil || !d.IsDir() {
		if err == ErrSkipDir && d.IsDir() {
			err = nil
		}
		return // err may be nil if it is not directory
	}

	var fsi FilesInfo
	if fsi, err = s.ReadDir(ctx, name); err != nil {
		if err = walkDirFunc(name, d, err); err != nil { // second call, to report an error from s.ReadDir()
			return
		}
	}

	for _, fi := range fsi {
		if s.nameIsADirectoryStub(fi.FullName()) {
			continue
		}
		if err = s.walkDir(ctx, fi.FullName(), S3DirEntry{oi: fi.(S3FileInfo).oi, fi: fi, s3: fi.Sys().(*S3)},
			walkDirFunc); err != nil {
			if err == ErrSkipDir {
				break
			}
			return
		}
	}
	return nil
}

// WalkDir simulates traversing the filesystem from the given directory
func (s *S3) WalkDir(ctx context.Context, name string, walkDirFunc WalkDirFunc) (err error) {
	if ctx, err = invokeBeforeOperationCB(ctx); err != nil {
		return
	}
	defer func() {
		if errcb := invokeAfterOperationCB(ctx); err == nil {
			err = errcb
		} // else drop callback error
	}()

	name = s.normalizeName(name)
	var fi FileInfo
	if fi, err = s.Stat(ctx, name); err != nil {
		return err
	}
	err = s.walkDir(ctx, name, S3DirEntry{oi: fi.(S3FileInfo).oi, fi: fi, s3: fi.Sys().(*S3)}, walkDirFunc)
	if err == ErrSkipDir {
		return nil
	}
	return
}

// Join the path segments
func (s *S3) Join(paths ...string) string { return path.Join(paths...) }

// Dir returns parent directory path
func (s *S3) Dir(name string) string { return path.Dir(name) }

// Ext returns object name extension
func (s *S3) Ext(name string) string { return path.Ext(name) }

// Base returns the last element of path
func (s *S3) Base(name string) string { return path.Base(name) }
