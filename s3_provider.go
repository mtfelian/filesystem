package filesystem

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/sirupsen/logrus"
)

var (
	ErrConflictingBucketOptions = errors.New("conflicting S3 bucket options")
	ErrS3BucketNotExists        = errors.New("s3 bucket does not exist")
	ErrFileSystemClosed         = errors.New("filesystem is closed")
)

// S3Provider owns shared S3 resources and returns bucket-scoped filesystems.
type S3Provider struct {
	endpoint  string
	region    string
	accessKey string
	secretKey string
	useSSL    bool
	logger    logrus.FieldLogger

	minioClient *minio.Client

	openedFilesTTL     time.Duration
	openedFilesTempDir string

	defaultBucketOptions S3BucketOptions

	mu      sync.Mutex
	buckets map[string]*S3
	ensured map[string]S3BucketOptions

	ensureMu sync.Mutex

	lifecycleMu    sync.Mutex
	cleanerOnce    sync.Once
	closeOnce      sync.Once
	cleanerStarted bool
	closed         bool
	closeC         chan struct{}
	doneC          chan struct{}
}

// NewS3Provider returns a long-lived S3 provider.
func NewS3Provider(p S3ProviderParams) (*S3Provider, error) {
	p.applyDefaults()

	minioClient, err := minio.New(p.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(p.AccessKey, p.SecretKey, ""),
		Secure: p.UseSSL,
		Region: p.Region,
	})
	if err != nil {
		return nil, err
	}

	return &S3Provider{
		endpoint:             p.Endpoint,
		region:               p.Region,
		accessKey:            p.AccessKey,
		secretKey:            p.SecretKey,
		useSSL:               p.UseSSL,
		logger:               p.Logger,
		minioClient:          minioClient,
		openedFilesTTL:       p.OpenedFilesTTL,
		openedFilesTempDir:   p.OpenedFilesTempDir,
		defaultBucketOptions: p.DefaultBucketOptions,
		buckets:              make(map[string]*S3),
		ensured:              make(map[string]S3BucketOptions),
		closeC:               make(chan struct{}),
		doneC:                make(chan struct{}),
	}, nil
}

// FileSystem returns a bucket-scoped filesystem as the generic FileSystem interface.
func (p *S3Provider) FileSystem(ctx context.Context, bucket string) (FileSystem, error) {
	return p.Bucket(ctx, bucket)
}

// Bucket returns a cheap bucket-scoped S3 filesystem.
func (p *S3Provider) Bucket(ctx context.Context, bucket string) (*S3, error) {
	if p == nil {
		return nil, ErrFileSystemClosed
	}
	if p.defaultBucketOptions.EnsureOnFirstUse {
		if err := p.EnsureBucket(ctx, bucket, p.defaultBucketOptions); err != nil {
			return nil, err
		}
	}
	if p.isClosed() {
		return nil, ErrFileSystemClosed
	}
	return p.bucket(bucket), nil
}

// EnsureBucket performs bucket provisioning once per provider lifetime.
func (p *S3Provider) EnsureBucket(ctx context.Context, bucket string, opts S3BucketOptions) error {
	if p == nil {
		return ErrFileSystemClosed
	}

	p.ensureMu.Lock()
	defer p.ensureMu.Unlock()

	if p.isClosed() {
		return ErrFileSystemClosed
	}

	p.mu.Lock()
	if ensured, ok := p.ensured[bucket]; ok {
		p.mu.Unlock()
		if bucketOptionsConflict(ensured, opts) {
			return ErrConflictingBucketOptions
		}
		return nil
	}
	p.mu.Unlock()

	s3 := p.bucketWithOptions(bucket, opts)

	exists, err := p.minioClient.BucketExists(ctx, bucket)
	if err != nil {
		return err
	}
	if !exists {
		if !opts.CreateIfMissing {
			return ErrS3BucketNotExists
		}
		if err = p.minioClient.MakeBucket(ctx, bucket, minio.MakeBucketOptions{
			Region:        p.region,
			ObjectLocking: false,
		}); err != nil {
			return err
		}
	}
	if opts.BucketTTL > 0 {
		if err = s3.SetBucketTTL(ctx, opts.BucketTTL); err != nil {
			return err
		}
	}
	if opts.EmulateEmptyDirs {
		if err = s3.putStubObject(ctx, ""); err != nil {
			return err
		}
	}

	p.mu.Lock()
	p.ensured[bucket] = opts
	if existing := p.buckets[bucket]; existing != nil {
		existing.applyBucketOptions(opts)
	}
	p.mu.Unlock()
	return nil
}

// Close shuts down provider-owned background work and releases local resources.
func (p *S3Provider) Close() error {
	if p == nil {
		return nil
	}
	p.closeOnce.Do(func() {
		p.lifecycleMu.Lock()
		p.closed = true
		started := p.cleanerStarted
		close(p.closeC)
		p.lifecycleMu.Unlock()

		if started {
			<-p.doneC
			return
		}
		p.cleanupOpenedFiles(true)
	})
	return nil
}

func (p *S3Provider) bucket(bucket string) *S3 {
	p.mu.Lock()
	defer p.mu.Unlock()

	opts := p.defaultBucketOptions
	if ensured, ok := p.ensured[bucket]; ok {
		opts = ensured
	}
	return p.bucketLocked(bucket, opts)
}

func (p *S3Provider) bucketWithOptions(bucket string, opts S3BucketOptions) *S3 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.bucketLocked(bucket, opts)
}

func (p *S3Provider) removeBucket(bucket string, s3 *S3) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.buckets[bucket] == s3 {
		delete(p.buckets, bucket)
	}
}

func (p *S3Provider) bucketLocked(bucket string, opts S3BucketOptions) *S3 {
	if s3 := p.buckets[bucket]; s3 != nil {
		s3.applyBucketOptions(opts)
		return s3
	}

	s3 := &S3{
		provider: p,

		endpoint:   p.endpoint,
		region:     p.region,
		accessKey:  p.accessKey,
		secretKey:  p.secretKey,
		useSSL:     p.useSSL,
		bucketName: bucket,
		logger:     p.logger,

		minioClient: p.minioClient,

		openedFilesList:    NewS3OpenedFilesList(),
		openedFilesTTL:     p.openedFilesTTL,
		openedFilesLocalFS: NewLocal().(*Local),
		openedFilesTempDir: p.openedFilesTempDir,
	}
	s3.applyBucketOptions(opts)
	p.buckets[bucket] = s3
	return s3
}

func (p *S3Provider) startCleaner() error {
	p.lifecycleMu.Lock()
	defer p.lifecycleMu.Unlock()

	if p.closed {
		return ErrFileSystemClosed
	}
	p.cleanerOnce.Do(func() {
		p.cleanerStarted = true
		go p.openedFilesListCleaning()
	})
	return nil
}

func (p *S3Provider) openedFilesListCleaning() {
	defer close(p.doneC)

	t := time.NewTicker(p.openedFilesTTL)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			p.cleanupOpenedFiles(false)
		case <-p.closeC:
			p.logger.Info("S3Provider.openedFilesListCleaning doing last cleanup")
			p.cleanupOpenedFiles(true)
			p.logger.Info("S3Provider.openedFilesListCleaning terminated")
			return
		}
	}
}

func (p *S3Provider) cleanupOpenedFiles(all bool) {
	p.mu.Lock()
	buckets := make([]*S3, 0, len(p.buckets))
	for _, s3 := range p.buckets {
		buckets = append(buckets, s3)
	}
	p.mu.Unlock()

	for _, s3 := range buckets {
		s3.cleanupOpenedFiles(all)
	}
}

func (p *S3Provider) isClosed() bool {
	p.lifecycleMu.Lock()
	defer p.lifecycleMu.Unlock()
	return p.closed
}

func bucketOptionsConflict(a, b S3BucketOptions) bool {
	return a.BucketTTL != b.BucketTTL ||
		a.EmulateEmptyDirs != b.EmulateEmptyDirs ||
		a.ListDirectoryEntries != b.ListDirectoryEntries
}
