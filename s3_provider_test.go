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

	lifecycleTTL := func(bucket string) int {
		cfg, err := minioClient.GetBucketLifecycle(ctx, bucket)
		Expect(err).NotTo(HaveOccurred())
		for _, rule := range cfg.Rules {
			if rule.ID == "ttl-purge-all-versions" {
				return int(rule.Expiration.Days)
			}
		}
		Fail("ttl lifecycle rule was not found")
		return 0
	}

	expectGoroutineCountAtMost := func(limit int) {
		Eventually(func() int {
			return runtime.NumGoroutine()
		}, 2*time.Second, 50*time.Millisecond).Should(BeNumerically("<=", limit))
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

		f, err := s3.Create(ctx, "/after-close.txt")
		Expect(f).To(BeNil())
		Expect(err).To(Equal(filesystem.ErrFileSystemClosed))
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

		object, err := minioClient.GetObject(ctx, bucket, "/opened.txt", minio.GetObjectOptions{})
		Expect(err).NotTo(HaveOccurred())
		defer object.Close()

		b, err := io.ReadAll(object)
		Expect(err).NotTo(HaveOccurred())
		Expect(b).To(Equal([]byte("flushed on close")))
	})
})
