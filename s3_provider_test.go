package filesystem_test

import (
	"context"
	"fmt"
	"io"
	"runtime"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/lifecycle"
	"github.com/mtfelian/filesystem"
	"github.com/mtfelian/utils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/sirupsen/logrus"
)

var _ = Describe("S3Provider", func() {
	var (
		ctx         context.Context
		endpoint    string
		logger      logrus.FieldLogger
		provider    *filesystem.S3Provider
		minioClient *minio.Client
		buckets     []string
	)

	const (
		region         = ""
		accessKey      = "minioadmin"
		secretKey      = "minioadmin"
		endpointDocker = "minio:9000"
		endpointLocal  = "localhost:9000"
		ttl            = time.Second

		ttlPurgeRuleID        = "ttl-purge-all-versions"
		ttlDeleteMarkerRuleID = "ttl-expire-delete-marker"
	)

	newProvider := func(defaultBucketOptions filesystem.S3BucketOptions) *filesystem.S3Provider {
		p, err := filesystem.NewS3Provider(filesystem.S3ProviderParams{
			Endpoint:             endpoint,
			Region:               region,
			AccessKey:            accessKey,
			SecretKey:            secretKey,
			UseSSL:               false,
			OpenedFilesTTL:       ttl,
			OpenedFilesTempDir:   "",
			Logger:               logger,
			DefaultBucketOptions: defaultBucketOptions,
		})
		Expect(err).NotTo(HaveOccurred())
		return p
	}

	trackBucket := func(bucket string) string {
		buckets = append(buckets, bucket)
		return bucket
	}

	bucketExists := func(bucket string) bool {
		exists, err := minioClient.BucketExists(ctx, bucket)
		Expect(err).NotTo(HaveOccurred())
		return exists
	}

	bucketLifecycle := func(bucket string) (*lifecycle.Configuration, bool) {
		cfg, err := minioClient.GetBucketLifecycle(ctx, bucket)
		if err != nil {
			resp := minio.ToErrorResponse(err)
			if resp.Code == "NoSuchLifecycleConfiguration" || resp.Code == "NoSuchBucketLifecycle" {
				return nil, false
			}
			Expect(err).NotTo(HaveOccurred())
			return nil, false
		}
		return cfg, true
	}

	lifecycleTTLExists := func(bucket string) (int, bool) {
		cfg, ok := bucketLifecycle(bucket)
		if !ok {
			return 0, false
		}
		for _, rule := range cfg.Rules {
			if rule.ID == ttlPurgeRuleID {
				return int(rule.Expiration.Days), true
			}
		}
		return 0, false
	}

	lifecycleTTL := func(bucket string) int {
		days, ok := lifecycleTTLExists(bucket)
		if ok {
			return days
		}
		Fail("ttl lifecycle rule was not found")
		return 0
	}

	expectNoLifecycleTTL := func(bucket string) {
		cfg, ok := bucketLifecycle(bucket)
		if !ok {
			return
		}
		for _, rule := range cfg.Rules {
			Expect(rule.ID).NotTo(Equal(ttlPurgeRuleID))
			Expect(rule.ID).NotTo(Equal(ttlDeleteMarkerRuleID))
		}
	}

	expectGoroutineCountAtMost := func(limit int) {
		Eventually(func() int {
			return runtime.NumGoroutine()
		}, 2*time.Second, 50*time.Millisecond).Should(BeNumerically("<=", limit))
	}

	waitForMinio := func() {
		const attempts = 10
		const delay = time.Second
		var err error
		for i := 1; i <= attempts; i++ {
			_, err = minioClient.BucketExists(ctx, "provider-readiness-check")
			if err == nil {
				return
			}
			fmt.Printf(">>>>> waiting for minio startup... attempt %d/%d\n", i, attempts)
			time.Sleep(delay)
		}
		Expect(err).NotTo(HaveOccurred())
	}

	BeforeEach(func() {
		ctx = context.Background()
		buckets = nil

		l := logrus.New()
		l.SetLevel(logrus.DebugLevel)
		logger = l

		endpoint = endpointLocal
		if utils.IsInDocker() {
			endpoint = endpointDocker
		}

		var err error
		minioClient, err = minio.New(endpoint, &minio.Options{
			Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
			Secure: false,
			Region: region,
		})
		Expect(err).NotTo(HaveOccurred())

		waitForMinio()

		provider = newProvider(filesystem.S3BucketOptions{})
	})

	AfterEach(func() {
		if provider != nil {
			Expect(provider.Close()).To(Succeed())
		}

		for _, bucket := range buckets {
			exists, err := minioClient.BucketExists(ctx, bucket)
			if err != nil || !exists {
				continue
			}
			Expect(minioClient.RemoveBucketWithOptions(ctx, bucket, minio.RemoveBucketOptions{
				ForceDelete: true,
			})).To(Succeed())
		}

		fsLocal := filesystem.NewLocal()
		exists, err := fsLocal.Exists(ctx, filesystem.TempDir)
		Expect(err).NotTo(HaveOccurred())
		if exists {
			Expect(fsLocal.RemoveAll(ctx, filesystem.TempDir)).To(Succeed())
		}
	})

	It("checks cached bucket handles without provisioning", func() {
		bucket := trackBucket("provider-cache-bucket")

		s3a, err := provider.Bucket(ctx, bucket)
		Expect(err).NotTo(HaveOccurred())
		s3b, err := provider.Bucket(ctx, bucket)
		Expect(err).NotTo(HaveOccurred())
		Expect(s3b).To(BeIdenticalTo(s3a))

		fs, err := provider.FileSystem(ctx, bucket)
		Expect(err).NotTo(HaveOccurred())
		Expect(fs).To(BeIdenticalTo(s3a))

		Expect(bucketExists(bucket)).To(BeFalse())
	})

	It("checks cheap bucket lookup does not grow goroutine count", func() {
		baseline := runtime.NumGoroutine()
		const amount = 100

		for i := 0; i < amount; i++ {
			bucket := trackBucket(fmt.Sprintf("provider-goroutine-lookup-%d", i))
			_, err := provider.Bucket(ctx, bucket)
			Expect(err).NotTo(HaveOccurred())
		}

		expectGoroutineCountAtMost(baseline + 10)
		Expect(provider.Close()).To(Succeed())
		expectGoroutineCountAtMost(baseline + 10)
	})

	It("checks concurrent bucket lookup returns one cached handle", func() {
		bucket := trackBucket("provider-concurrent-lookup-bucket")
		const amount = 64

		var wg sync.WaitGroup
		results := make([]*filesystem.S3, amount)
		errs := make([]error, amount)
		wg.Add(amount)
		for i := 0; i < amount; i++ {
			i := i
			go func() {
				defer GinkgoRecover()
				defer wg.Done()
				results[i], errs[i] = provider.Bucket(ctx, bucket)
			}()
		}
		wg.Wait()

		for i := 0; i < amount; i++ {
			Expect(errs[i]).NotTo(HaveOccurred())
			Expect(results[i]).NotTo(BeNil())
			Expect(results[i]).To(BeIdenticalTo(results[0]))
		}
		Expect(bucketExists(bucket)).To(BeFalse())
	})

	It("checks failed ensure state is not cached", func() {
		bucket := trackBucket("provider-retry-bucket")

		err := provider.EnsureBucket(ctx, bucket, filesystem.S3BucketOptions{
			CreateIfMissing: false,
		})
		Expect(err).To(Equal(filesystem.ErrS3BucketNotExists))
		Expect(bucketExists(bucket)).To(BeFalse())

		Expect(provider.EnsureBucket(ctx, bucket, filesystem.S3BucketOptions{
			CreateIfMissing:      true,
			EmulateEmptyDirs:     true,
			ListDirectoryEntries: true,
		})).To(Succeed())
		Expect(bucketExists(bucket)).To(BeTrue())

		s3, err := provider.Bucket(ctx, bucket)
		Expect(err).NotTo(HaveOccurred())
		Expect(s3.WriteFile(ctx, "/retry.txt", []byte("retry content"))).To(Succeed())
	})

	It("checks different TTLs per bucket", func() {
		bucketA := trackBucket("provider-ttl-bucket-a")
		bucketB := trackBucket("provider-ttl-bucket-b")

		Expect(provider.EnsureBucket(ctx, bucketA, filesystem.S3BucketOptions{
			CreateIfMissing: true,
			BucketTTL:       24 * time.Hour,
		})).To(Succeed())
		Expect(provider.EnsureBucket(ctx, bucketB, filesystem.S3BucketOptions{
			CreateIfMissing: true,
			BucketTTL:       48 * time.Hour,
		})).To(Succeed())

		Expect(lifecycleTTL(bucketA)).To(Equal(1))
		Expect(lifecycleTTL(bucketB)).To(Equal(2))
	})

	It("updates bucket TTL and reconciles ensured options", func() {
		bucket := trackBucket("provider-update-ttl-bucket")

		Expect(provider.EnsureBucket(ctx, bucket, filesystem.S3BucketOptions{
			CreateIfMissing:      true,
			BucketTTL:            24 * time.Hour,
			EmulateEmptyDirs:     true,
			ListDirectoryEntries: true,
		})).To(Succeed())

		s3, err := provider.Bucket(ctx, bucket)
		Expect(err).NotTo(HaveOccurred())

		Expect(provider.SetBucketTTL(ctx, bucket, 48*time.Hour)).To(Succeed())
		Expect(lifecycleTTL(bucket)).To(Equal(2))

		reopened, err := provider.Bucket(ctx, bucket)
		Expect(err).NotTo(HaveOccurred())
		Expect(reopened).To(BeIdenticalTo(s3))

		Expect(provider.EnsureBucket(ctx, bucket, filesystem.S3BucketOptions{
			CreateIfMissing:      true,
			BucketTTL:            48 * time.Hour,
			EmulateEmptyDirs:     true,
			ListDirectoryEntries: true,
		})).To(Succeed())

		err = provider.EnsureBucket(ctx, bucket, filesystem.S3BucketOptions{
			CreateIfMissing:      true,
			BucketTTL:            24 * time.Hour,
			EmulateEmptyDirs:     true,
			ListDirectoryEntries: true,
		})
		Expect(err).To(Equal(filesystem.ErrConflictingBucketOptions))
	})

	It("clears bucket TTL and reconciles ensured options", func() {
		bucket := trackBucket("provider-clear-ttl-bucket")

		Expect(provider.EnsureBucket(ctx, bucket, filesystem.S3BucketOptions{
			CreateIfMissing:      true,
			BucketTTL:            24 * time.Hour,
			EmulateEmptyDirs:     true,
			ListDirectoryEntries: true,
		})).To(Succeed())
		Expect(lifecycleTTL(bucket)).To(Equal(1))

		s3, err := provider.Bucket(ctx, bucket)
		Expect(err).NotTo(HaveOccurred())

		Expect(provider.SetBucketTTL(ctx, bucket, 0)).To(Succeed())
		expectNoLifecycleTTL(bucket)

		reopened, err := provider.Bucket(ctx, bucket)
		Expect(err).NotTo(HaveOccurred())
		Expect(reopened).To(BeIdenticalTo(s3))

		Expect(provider.EnsureBucket(ctx, bucket, filesystem.S3BucketOptions{
			CreateIfMissing:      true,
			EmulateEmptyDirs:     true,
			ListDirectoryEntries: true,
		})).To(Succeed())

		err = provider.EnsureBucket(ctx, bucket, filesystem.S3BucketOptions{
			CreateIfMissing:      true,
			BucketTTL:            24 * time.Hour,
			EmulateEmptyDirs:     true,
			ListDirectoryEntries: true,
		})
		Expect(err).To(Equal(filesystem.ErrConflictingBucketOptions))
	})

	It("clears only provider-owned bucket TTL lifecycle rules", func() {
		bucket := trackBucket("provider-clear-ttl-preserve-bucket")

		Expect(provider.EnsureBucket(ctx, bucket, filesystem.S3BucketOptions{
			CreateIfMissing: true,
			BucketTTL:       24 * time.Hour,
		})).To(Succeed())

		cfg, err := minioClient.GetBucketLifecycle(ctx, bucket)
		Expect(err).NotTo(HaveOccurred())
		cfg.Rules = append(cfg.Rules, lifecycle.Rule{
			ID:         "unrelated-lifecycle-rule",
			Status:     "Enabled",
			RuleFilter: lifecycle.Filter{Prefix: "keep/"},
			Expiration: lifecycle.Expiration{
				Days: lifecycle.ExpirationDays(7),
			},
		})
		Expect(minioClient.SetBucketLifecycle(ctx, bucket, cfg)).To(Succeed())

		Expect(provider.SetBucketTTL(ctx, bucket, 0)).To(Succeed())

		cfg, err = minioClient.GetBucketLifecycle(ctx, bucket)
		Expect(err).NotTo(HaveOccurred())
		foundUnrelatedRule := false
		for _, rule := range cfg.Rules {
			Expect(rule.ID).NotTo(Equal(ttlPurgeRuleID))
			Expect(rule.ID).NotTo(Equal(ttlDeleteMarkerRuleID))
			if rule.ID == "unrelated-lifecycle-rule" {
				foundUnrelatedRule = true
				Expect(rule.RuleFilter.Prefix).To(Equal("keep/"))
				Expect(rule.Expiration.Days).To(Equal(lifecycle.ExpirationDays(7)))
			}
		}
		Expect(foundUnrelatedRule).To(BeTrue())
	})

	It("does not cache failed TTL updates for missing buckets", func() {
		bucket := trackBucket("provider-missing-ttl-bucket")

		err := provider.SetBucketTTL(ctx, bucket, 24*time.Hour)
		Expect(err).To(Equal(filesystem.ErrS3BucketNotExists))
		Expect(bucketExists(bucket)).To(BeFalse())

		Expect(provider.EnsureBucket(ctx, bucket, filesystem.S3BucketOptions{
			CreateIfMissing:      true,
			EmulateEmptyDirs:     true,
			ListDirectoryEntries: true,
		})).To(Succeed())
		Expect(bucketExists(bucket)).To(BeTrue())
	})

	It("checks ensuring buckets on first lookup when default options request it", func() {
		Expect(provider.Close()).To(Succeed())
		provider = newProvider(filesystem.S3BucketOptions{
			CreateIfMissing:      true,
			EnsureOnFirstUse:     true,
			EmulateEmptyDirs:     true,
			ListDirectoryEntries: true,
		})

		bucket := trackBucket("provider-default-ensure-bucket")
		s3, err := provider.Bucket(ctx, bucket)
		Expect(err).NotTo(HaveOccurred())
		Expect(bucketExists(bucket)).To(BeTrue())

		Expect(s3.WriteFile(ctx, "/lazy.txt", []byte("lazy content"))).To(Succeed())
		b, err := s3.ReadFile(ctx, "/lazy.txt")
		Expect(err).NotTo(HaveOccurred())
		Expect(b).To(Equal([]byte("lazy content")))
	})

	It("checks concurrent create starts a bounded number of cleanup goroutines", func() {
		bucket := trackBucket("provider-concurrent-create-bucket")
		Expect(provider.EnsureBucket(ctx, bucket, filesystem.S3BucketOptions{
			CreateIfMissing: true,
		})).To(Succeed())

		s3, err := provider.Bucket(ctx, bucket)
		Expect(err).NotTo(HaveOccurred())

		baseline := runtime.NumGoroutine()
		const amount = 32

		var wg sync.WaitGroup
		files := make([]filesystem.File, amount)
		errs := make([]error, amount)
		wg.Add(amount)
		for i := 0; i < amount; i++ {
			i := i
			go func() {
				defer GinkgoRecover()
				defer wg.Done()
				files[i], errs[i] = s3.Create(ctx, fmt.Sprintf("/concurrent-%d.txt", i))
			}()
		}
		wg.Wait()

		for i := 0; i < amount; i++ {
			Expect(errs[i]).NotTo(HaveOccurred())
			Expect(files[i]).NotTo(BeNil())
		}
		Expect(s3.OpenedFilesList().Len()).To(Equal(amount))

		expectGoroutineCountAtMost(baseline + 10)
		Expect(provider.Close()).To(Succeed())
		expectGoroutineCountAtMost(baseline + 10)
	})

	It("checks rejecting provider and bucket operations after close", func() {
		bucket := trackBucket("provider-close-bucket")
		Expect(provider.EnsureBucket(ctx, bucket, filesystem.S3BucketOptions{
			CreateIfMissing: true,
		})).To(Succeed())

		s3, err := provider.Bucket(ctx, bucket)
		Expect(err).NotTo(HaveOccurred())
		Expect(s3.WriteFile(ctx, "/before-close.txt", []byte("content"))).To(Succeed())

		Expect(provider.Close()).To(Succeed())
		Expect(provider.Close()).To(Succeed())

		_, err = provider.Bucket(ctx, bucket)
		Expect(err).To(Equal(filesystem.ErrFileSystemClosed))

		err = s3.WriteFile(ctx, "/after-close.txt", []byte("content"))
		Expect(err).To(Equal(filesystem.ErrFileSystemClosed))

		_, err = s3.ReadFile(ctx, "/before-close.txt")
		Expect(err).To(Equal(filesystem.ErrFileSystemClosed))

		err = provider.SetBucketTTL(ctx, bucket, time.Hour)
		Expect(err).To(Equal(filesystem.ErrFileSystemClosed))

		f, err := s3.Create(ctx, "/after-close.txt")
		Expect(f).To(BeNil())
		Expect(err).To(Equal(filesystem.ErrFileSystemClosed))
	})

	It("checks closing a provider-owned bucket handle", func() {
		bucket := trackBucket("provider-bucket-close-bucket")
		Expect(provider.EnsureBucket(ctx, bucket, filesystem.S3BucketOptions{
			CreateIfMissing: true,
		})).To(Succeed())

		s3, err := provider.Bucket(ctx, bucket)
		Expect(err).NotTo(HaveOccurred())
		Expect(s3.WriteFile(ctx, "/before-close.txt", []byte("content"))).To(Succeed())

		Expect(s3.Close()).To(Succeed())
		Expect(s3.Close()).To(Succeed())

		err = s3.WriteFile(ctx, "/after-close.txt", []byte("content"))
		Expect(err).To(Equal(filesystem.ErrFileSystemClosed))

		reopened, err := provider.Bucket(ctx, bucket)
		Expect(err).NotTo(HaveOccurred())
		Expect(reopened).NotTo(BeIdenticalTo(s3))
		Expect(reopened.WriteFile(ctx, "/after-reopen.txt", []byte("content"))).To(Succeed())
	})

	It("checks flushing opened files during provider close", func() {
		bucket := trackBucket("provider-cleanup-bucket")
		Expect(provider.EnsureBucket(ctx, bucket, filesystem.S3BucketOptions{
			CreateIfMissing:  true,
			EmulateEmptyDirs: true,
		})).To(Succeed())

		s3, err := provider.Bucket(ctx, bucket)
		Expect(err).NotTo(HaveOccurred())

		f, err := s3.Create(ctx, "/opened.txt")
		Expect(err).NotTo(HaveOccurred())
		Expect(f).NotTo(BeNil())
		_, err = f.Write([]byte("flushed on close"))
		Expect(err).NotTo(HaveOccurred())

		Expect(provider.Close()).To(Succeed())

		object, err := minioClient.GetObject(ctx, bucket, "opened.txt", minio.GetObjectOptions{})
		Expect(err).NotTo(HaveOccurred())
		defer object.Close()

		b, err := io.ReadAll(object)
		Expect(err).NotTo(HaveOccurred())
		Expect(b).To(Equal([]byte("flushed on close")))
	})
})
