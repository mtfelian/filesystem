package filesystem

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"math"
	"net/http"
	"path"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/lifecycle"
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
	provider *S3Provider
	closeMu  sync.Mutex
	closed   bool

	endpoint  string
	region    string
	accessKey string
	secretKey string
	logger    logrus.FieldLogger

	useSSL      bool
	bucketName  string
	bucketTTL   time.Duration
	minioClient *minio.Client

	openedFilesLocalFS *Local
	openedFilesList    *S3OpenedFilesList
	openedFilesTTL     time.Duration
	openedFilesTempDir string

	emulateEmptyDirs     bool
	listDirectoryEntries bool
}

func isNoLifecycleConfig(err error) bool {
	if err == nil {
		return false
	}
	resp := minio.ToErrorResponse(err)
	return resp.Code == "NoSuchLifecycleConfiguration" || resp.Code == "NoSuchBucketLifecycle"
}

const (
	s3LifecycleTTLPurgeAllVersionsRuleID = "ttl-purge-all-versions"
	s3LifecycleTTLExpireDeleteMarkerID   = "ttl-expire-delete-marker"
)

func (s *S3) SetBucketTTL(ctx context.Context, ttl time.Duration) error {
	if err := s.ensureOpen(); err != nil {
		return err
	}

	cfg, err := s.minioClient.GetBucketLifecycle(ctx, s.bucketName)
	if err != nil {
		if isNoLifecycleConfig(err) {
			cfg = lifecycle.NewConfiguration()
		} else {
			return err
		}
	}

	if ttl <= 0 {
		return s.clearBucketTTL(ctx, cfg)
	}

	days := int(math.Ceil(ttl.Hours() / 24.0))
	if days < 1 {
		days = 1
	}

	rulePurge := lifecycle.Rule{
		ID:         s3LifecycleTTLPurgeAllVersionsRuleID,
		Status:     "Enabled",
		RuleFilter: lifecycle.Filter{}, // whole bucket; set Prefix if apply to a subtree
		Expiration: lifecycle.Expiration{
			Days:      lifecycle.ExpirationDays(days),
			DeleteAll: lifecycle.ExpirationBoolean(true),
		},
	}

	ruleDeleteMarker := lifecycle.Rule{
		ID:         s3LifecycleTTLExpireDeleteMarkerID,
		Status:     "Enabled",
		RuleFilter: lifecycle.Filter{},
		Expiration: lifecycle.Expiration{
			DeleteMarker: lifecycle.ExpireDeleteMarker(true),
		},
	}

	// find rule by ID
	find := func(id string) (lifecycle.Rule, bool) {
		for _, r := range cfg.Rules {
			if r.ID == id {
				return r, true
			}
		}
		return lifecycle.Rule{}, false
	}

	// compare rules
	eq := func(a, b lifecycle.Rule) bool {
		if a.ID != b.ID || a.Status != b.Status {
			return false
		}

		if !a.RuleFilter.IsNull() || !b.RuleFilter.IsNull() {
			if a.RuleFilter.Prefix != b.RuleFilter.Prefix {
				return false
			}
		}

		if a.Expiration.Days != b.Expiration.Days {
			return false
		}
		if a.Expiration.DeleteAll != b.Expiration.DeleteAll {
			return false
		}
		if a.Expiration.DeleteMarker != b.Expiration.DeleteMarker {
			return false
		}
		return true
	}

	changed := false
	upsert := func(expected lifecycle.Rule) {
		if rule, ok := find(expected.ID); ok && eq(rule, expected) {
			return // already correct
		}
		for i := range cfg.Rules {
			if cfg.Rules[i].ID == expected.ID {
				cfg.Rules[i] = expected
				changed = true
				return
			}
		}
		cfg.Rules = append(cfg.Rules, expected)
		changed = true
	}

	upsert(rulePurge)
	upsert(ruleDeleteMarker)

	if !changed {
		return nil
	}
	return s.minioClient.SetBucketLifecycle(ctx, s.bucketName, cfg)
}

func (s *S3) clearBucketTTL(ctx context.Context, cfg *lifecycle.Configuration) error {
	if cfg == nil || len(cfg.Rules) == 0 {
		return nil
	}

	rules := make([]lifecycle.Rule, 0, len(cfg.Rules))
	for _, rule := range cfg.Rules {
		switch rule.ID {
		case s3LifecycleTTLPurgeAllVersionsRuleID, s3LifecycleTTLExpireDeleteMarkerID:
			continue
		default:
			rules = append(rules, rule)
		}
	}
	if len(rules) == len(cfg.Rules) {
		return nil
	}

	cfg.Rules = rules
	return s.minioClient.SetBucketLifecycle(ctx, s.bucketName, cfg)
}

// Close attempts to forcibly close opened files and release resources.
// The FileSystem should not be used after calling this method.
func (s *S3) Close() error {
	if s == nil {
		return nil
	}
	s.closeMu.Lock()
	if s.closed {
		s.closeMu.Unlock()
		return nil
	}
	s.closed = true
	s.closeMu.Unlock()

	s.cleanupOpenedFiles(true)
	if s.provider != nil {
		s.provider.removeBucket(s.bucketName, s)
	}
	return nil
}

// Logger provides access to a logger
func (s *S3) Logger() logrus.FieldLogger { return s.logger }

// SetListDirectoryEntries or unset it, use mainly for tests
func (s *S3) SetListDirectoryEntries(v bool) { s.listDirectoryEntries = v }

// MinioClient provides access to Minio Client, use mainly for tests
func (s *S3) MinioClient() *minio.Client { return s.minioClient }

// Provider provides access to the owning S3 provider.
func (s *S3) Provider() *S3Provider { return s.provider }

// OpenedFilesList provides access to opened files list, use mainly for tests
func (s *S3) OpenedFilesList() *S3OpenedFilesList { return s.openedFilesList }

// OpenedFilesListLock locks opened files list associated mutex, use mainly for tests
func (s *S3) OpenedFilesListLock() { s.openedFilesList.Lock() }

// OpenedFilesListUnlock unlocks opened files list associated mutex, use mainly for tests
func (s *S3) OpenedFilesListUnlock() { s.openedFilesList.Unlock() }

func (s *S3) now() time.Time { return time.Now() }

func (s *S3) ensureOpen() error {
	if s == nil {
		return ErrFileSystemClosed
	}
	s.closeMu.Lock()
	closed := s.closed
	s.closeMu.Unlock()
	if closed {
		return ErrFileSystemClosed
	}
	if s.provider != nil && s.provider.isClosed() {
		return ErrFileSystemClosed
	}
	return nil
}

func (s *S3) applyBucketOptions(opts S3BucketOptions) {
	s.bucketTTL = opts.BucketTTL
	s.emulateEmptyDirs = opts.EmulateEmptyDirs
	s.listDirectoryEntries = opts.ListDirectoryEntries
}

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

func (s *S3) objectKey(name string) string {
	return strings.TrimPrefix(s.normalizeName(name), "/")
}

func (s *S3) objectKeyFromNormalized(name string) string {
	return strings.TrimPrefix(name, "/")
}

func (s *S3) logicalNameFromObjectKey(key string) string {
	if key == "" {
		return "/"
	}
	if strings.HasPrefix(key, "/") {
		return key
	}
	return "/" + key
}

func (s *S3) logicalObjectInfo(oi minio.ObjectInfo) minio.ObjectInfo {
	oi.Key = s.logicalNameFromObjectKey(oi.Key)
	return oi
}

func (s *S3) cleanupOpenedFiles(all bool) {
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
		s.logger.Infof("S3.cleanupOpenedFiles: closing %d file(s)", len(s3FilesToClose))
	}
	for _, s3File := range s3FilesToClose {
		s.logger.Infof("S3.cleanupOpenedFiles: autoclosing file %q", s3File.localName)
		if err := s3File.Close(); err != nil {
			s.logger.Errorf("S3.cleanupOpenedFiles: failed to s3File.Close(): %v", err)
		}
	}
}

// TempFileName converts file name to a temporary file name
func (s *S3) TempFileName(name string) string {
	name = s.normalizeName(name)
	sum := sha256.Sum256([]byte(name))
	return s.openedFilesLocalFS.Join(
		s.openedFilesTempDir,
		TempDir,
		s.bucketName+"_"+hex.EncodeToString(sum[:]),
	)
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
	if err = s.ensureOpen(); err != nil {
		return nil, err
	}
	if name = s.normalizeName(name); s.nameIsADirectory(name) {
		return nil, ErrCantOpenS3Directory
	}

	if s.provider != nil {
		if err = s.provider.startCleaner(); err != nil {
			return nil, err
		}
	}

	localFileName := s.TempFileName(name)

	s3OpenedFile := &S3OpenedFilesListEntry{
		ready: make(chan struct{}),
		Added: s.now(),
		S3File: &S3OpenedFile{
			ctx:        ctx,
			s3:         s,
			underlying: nil, // to be written below
			localName:  localFileName,
			objectName: name,
		},
	}
	if existing, added := s.openedFilesList.GetOrAddAndLockEntry(localFileName, s3OpenedFile); !added {
		s3OpenedFile = existing
		if s3OpenedFile.ready != nil {
			select {
			case <-s3OpenedFile.ready:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		if s3OpenedFile.openErr != nil {
			return nil, s3OpenedFile.openErr
		}
		return s3OpenedFile.S3File, ErrFileAlreadyOpened
	}

	defer func() {
		s3OpenedFile.MarkReady(err)
		if err == nil {
			return
		}
		// only if error occured later, so we are in defer:
		s.openedFilesList.DeleteAndUnlockEntry(localFileName)
		if f != nil {
			_ = f.Close()
		}
	}()

	if err = s.openedFilesLocalFS.MakePathAll(ctx, s.openedFilesLocalFS.Dir(localFileName)); err != nil {
		return nil, err
	}

	if fileMode != fileModeCreate { // fileModeOpen or fileModeWrite, so we create local file from S3 object
		var object *minio.Object
		if object, err = s.minioClient.GetObject(ctx, s.bucketName, s.objectKeyFromNormalized(name), minio.GetObjectOptions{}); err != nil {
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
	if err != nil {
		return nil, err
	}
	s3OpenedFile.S3File.SetUnderlying(f)

	return s3OpenedFile.S3File, nil
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

	if err = s.ensureOpen(); err != nil {
		return
	}

	name = s.normalizeName(name)
	var o *minio.Object
	if o, err = s.minioClient.GetObject(ctx, s.bucketName, s.objectKeyFromNormalized(name), minio.GetObjectOptions{}); err != nil {
		return
	}
	defer o.Close()
	return io.ReadAll(o)
}

// WriteFile by it's name to the client's bucket
func (s *S3) WriteFile(ctx context.Context, name string, b []byte) (err error) {
	return s.writeFile(ctx, name, b, false)
}

func (s *S3) writeFile(ctx context.Context, name string, b []byte, allowDuringClose bool) (err error) {
	if ctx, err = invokeBeforeOperationCB(ctx); err != nil {
		return
	}
	defer func() {
		if errcb := invokeAfterOperationCB(ctx); err == nil {
			err = errcb
		} // else drop callback error
	}()

	if !allowDuringClose {
		if err = s.ensureOpen(); err != nil {
			return
		}
	}

	name = s.normalizeName(name)
	if s.emulateEmptyDirs {
		if dir := s.Dir(name); dir != "." && dir != "/" {
			if err = s.makePathAll(ctx, dir, allowDuringClose); err != nil {
				return
			}
		}
	}
	_, err = s.minioClient.PutObject(ctx, s.bucketName, s.objectKeyFromNormalized(name), bytes.NewReader(b), int64(len(b)),
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

	if err = s.ensureOpen(); err != nil {
		return
	}

	snowBallC := make(chan minio.SnowballObject)
	for i, el := range f {
		f[i].Name = s.normalizeName(el.Name)
		if s.emulateEmptyDirs {
			// todo may be optimized if many files have to be written in same subdir structure via a tree
			if dir := s.Dir(f[i].Name); dir != "." && dir != "/" {
				if err = s.MakePathAll(ctx, dir); err != nil {
					return
				}
			}
		}
	}
	go func() {
		for i := range f {
			snowBallC <- minio.SnowballObject{
				Key:     s.objectKeyFromNormalized(f[i].Name),
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

	if err = s.ensureOpen(); err != nil {
		return
	}

	name = s.normalizeName(name)
	return s.minioClient.GetObject(ctx, s.bucketName, s.objectKeyFromNormalized(name), minio.GetObjectOptions{})
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

	if err = s.ensureOpen(); err != nil {
		return
	}

	name = s.normalizeName(name)

	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()
	for objectInfo := range s.minioClient.ListObjects(ctx, s.bucketName, minio.ListObjectsOptions{
		Prefix:    s.objectKeyFromNormalized(name),
		Recursive: recursive,
	}) {
		c++
		objectInfo = s.logicalObjectInfo(objectInfo)
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

	if err = s.ensureOpen(); err != nil {
		return
	}

	name = s.normalizeName(name)
	if !s.nameIsADirectoryPath(name) { // not a folder
		_, err = s.minioClient.StatObject(ctx, s.bucketName, s.objectKeyFromNormalized(name), minio.StatObjectOptions{})
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

func (s *S3) putStubObject(ctx context.Context, name string) error {
	return s.putStubObjectInternal(ctx, name, false)
}

func (s *S3) putStubObjectInternal(ctx context.Context, name string, allowDuringClose bool) (err error) {
	if ctx, err = invokeBeforeOperationCB(ctx); err != nil {
		return
	}
	defer func() {
		if errcb := invokeAfterOperationCB(ctx); err == nil {
			err = errcb
		} // else drop callback error
	}()

	if !allowDuringClose {
		if err = s.ensureOpen(); err != nil {
			return
		}
	}

	name = s.nameToStub(name)
	content := []byte(DirStubFileContent)
	_, err = s.minioClient.PutObject(ctx, s.bucketName, s.objectKeyFromNormalized(name), bytes.NewReader(content),
		int64(len(content)), minio.PutObjectOptions{
			DisableMultipart: true,
			ContentType:      "text/plain",
		})
	return
}

// MakePathAll does nothing in S3 FileSystem. As a workaround to make sure that we have a precreated folder,
// we create a small file in it.
// refer to: https://github.com/minio/minio/issues/3555, https://github.com/minio/minio/issues/2423
func (s *S3) MakePathAll(ctx context.Context, name string) (err error) {
	return s.makePathAll(ctx, name, false)
}

func (s *S3) makePathAll(ctx context.Context, name string, allowDuringClose bool) (err error) {
	if !allowDuringClose {
		if err = s.ensureOpen(); err != nil {
			return
		}
	}
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
		if err = s.putStubObjectInternal(ctx, name, allowDuringClose); err != nil {
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

	if err = s.ensureOpen(); err != nil {
		return
	}

	name = s.normalizeName(name)
	name = s.stubToDir(name)           // if stub, convert to dir with trailing '/'
	if !s.nameIsADirectoryPath(name) { // means was not a stub but a normal object name
		return s.minioClient.RemoveObject(ctx, s.bucketName, s.objectKeyFromNormalized(name), minio.RemoveObjectOptions{})
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

	return s.minioClient.RemoveObject(ctx, s.bucketName, s.objectKey(s.nameToStub(name)), minio.RemoveObjectOptions{})
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

	if err = s.ensureOpen(); err != nil {
		return
	}

	objectInfoC := make(chan minio.ObjectInfo)
	keys := make([]string, 0, len(names))
	for i := range names {
		names[i] = s.normalizeName(names[i])
		names[i] = s.stubToDir(names[i]) // if stub, convert to dir with trailing '/'

		if !s.nameIsADirectoryPath(names[i]) || !s.emulateEmptyDirs { // means was not a stub but a normal object name
			keys = append(keys, s.objectKeyFromNormalized(names[i]))
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
		keys = append(keys, s.objectKey(s.nameToStub(names[i])))
	}

	go func() {
		defer close(objectInfoC)
		for _, key := range keys {
			objectInfoC <- minio.ObjectInfo{Key: key}
		}
	}()

	objectRemoveErrorC := s.minioClient.RemoveObjects(ctx, s.bucketName, objectInfoC, minio.RemoveObjectsOptions{})
	for ore := range objectRemoveErrorC {
		if ore.Err != nil {
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

	if err = s.ensureOpen(); err != nil {
		return
	}

	name = s.normalizeName(name)
	name = s.stubToDir(name)
	if !s.nameIsADirectoryPath(name) {
		return s.minioClient.RemoveObject(ctx, s.bucketName, s.objectKeyFromNormalized(name), minio.RemoveObjectOptions{})
	}

	ctx1, cancel1 := context.WithCancel(ctx)
	defer cancel1()
	objectInfoC := s.minioClient.ListObjects(ctx1, s.bucketName, minio.ListObjectsOptions{
		Prefix:    s.objectKeyFromNormalized(name),
		Recursive: true,
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

	if err = s.ensureOpen(); err != nil {
		return
	}

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
		Prefix:    s.objectKeyFromNormalized(name),
		Recursive: true,
	}) {
		if objectInfo.Err != nil {
			return false, objectInfo.Err
		}
		i++
		if (i > 1 || !s.nameIsADirectoryStub(s.logicalNameFromObjectKey(objectInfo.Key))) && s.emulateEmptyDirs ||
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

	if err = s.ensureOpen(); err != nil {
		return
	}

	name = s.normalizeName(name)
	if !s.emulateEmptyDirs { // if no empty dirs allowed just do nothing
		return name, nil
	}

	var exists bool
	if exists, err = s.Exists(ctx, name); err != nil {
		return "", err
	}
	if !exists {
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

	if err = s.ensureOpen(); err != nil {
		return
	}

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
			minio.CopyDestOptions{Bucket: s.bucketName, Object: s.objectKeyFromNormalized(to)},
			minio.CopySrcOptions{Bucket: s.bucketName, Object: s.objectKeyFromNormalized(from)}); err != nil {
			return
		}
		return s.minioClient.RemoveObject(ctx, s.bucketName, s.objectKeyFromNormalized(from), minio.RemoveObjectOptions{})
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
		Prefix:    s.objectKeyFromNormalized(from),
		Recursive: true,
	}) {
		if objectInfo.Err != nil {
			return objectInfo.Err
		}
		objFrom := s.logicalNameFromObjectKey(objectInfo.Key)
		objTo := to + strings.TrimPrefix(objFrom, from)
		if dir := s.Dir(objTo); dir != "." && dir != "/" {
			if err = s.MakePathAll(ctx, dir); err != nil {
				return
			}
		}

		if _, err = s.minioClient.CopyObject(ctx,
			minio.CopyDestOptions{Bucket: s.bucketName, Object: s.objectKeyFromNormalized(objTo)},
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

	if err = s.ensureOpen(); err != nil {
		return
	}

	name = s.normalizeName(name)
	if s.nameIsADirectoryPath(name) && s.emulateEmptyDirs {
		var objectInfo minio.ObjectInfo
		if objectInfo, err = s.minioClient.StatObject(ctx, s.bucketName, s.objectKey(s.nameToStub(name)),
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
	if objectInfo, err = s.minioClient.StatObject(ctx, s.bucketName, s.objectKeyFromNormalized(name), minio.StatObjectOptions{}); err != nil {
		return
	}
	objectInfo.Key = name
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

	if err = s.ensureOpen(); err != nil {
		return
	}

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
		Prefix:    s.objectKeyFromNormalized(name),
		Recursive: false,
	}) {
		if objectInfo.Err != nil {
			return fi, objectInfo.Err
		}
		objectInfo = s.logicalObjectInfo(objectInfo)
		if s.nameIsADirectory(objectInfo.Key) {
			continue
		}
		fi = append(fi, NewS3FileInfo(s, objectInfo))
	}

	if !s.listDirectoryEntries {
		return fi, nil
	}

	// reading dir entries

	for objectInfo := range s.minioClient.ListObjects(ctx, s.bucketName, minio.ListObjectsOptions{
		Prefix:    s.objectKeyFromNormalized(name),
		Recursive: true,
	}) {
		if objectInfo.Err != nil {
			return fi, objectInfo.Err
		}

		// only current level directories
		var key string
		objectName := s.logicalNameFromObjectKey(objectInfo.Key)
		parent := func() string { return s.Dir(objectName) + "/" }
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

	if err = s.ensureOpen(); err != nil {
		return
	}

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
