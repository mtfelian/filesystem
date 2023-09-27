package filesystem_test

import (
	"context"
	"os"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/mtfelian/filesystem"
	"github.com/mtfelian/utils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/sirupsen/logrus"
)

var _ = Describe("recursive empty subtree cleaner", func() {
	var (
		err          error
		ctx          context.Context
		logger       logrus.FieldLogger
		fs           filesystem.FileSystem
		basePath     string
		shouldRemove bool
	)

	BeforeEach(func() {
		l := logrus.New()
		l.SetLevel(logrus.DebugLevel)
		logger = l
		_ = logger

		ctx = context.Background()
	})

	Context("with Local", func() {
		BeforeEach(func() {
			fs = filesystem.NewLocal()
			basePath, err = os.Getwd()
			Expect(err).NotTo(HaveOccurred())
			basePath = fs.Join(basePath, "testdir")
			shouldRemove = true
		})

		AfterEach(func() {
			if shouldRemove {
				Expect(fs.RemoveAll(ctx, basePath)).To(Succeed())
			}
		})

		It("checks recursive empty directories tree removal", func() {
			var filePath string
			By("creating directory tree", func() {
				Expect(fs.MakePathAll(ctx, fs.Join(basePath, "dir0", "dir1", "dir2", "dir3"))).To(Succeed())
				Expect(fs.MakePathAll(ctx, fs.Join(basePath, "dir0", "dir4", "dir5"))).To(Succeed())
				Expect(fs.MakePathAll(ctx, fs.Join(basePath, "dir0", "dir6"))).To(Succeed())
				filePath = fs.Join(basePath, "dir0", "dir6", "file.txt")
				Expect(fs.WriteFile(ctx, filePath, []byte("test content"))).To(Succeed())
			})

			Expect(filesystem.RemoveEmptyDirs(ctx, fs, basePath)).To(Succeed())

			for i, tc := range []struct {
				path     string
				expected bool
			}{
				{fs.Join(basePath, "dir0"), true},
				{fs.Join(basePath, "dir0", "dir1"), false},
				{fs.Join(basePath, "dir0", "dir1", "dir2"), false},
				{fs.Join(basePath, "dir0", "dir1", "dir2", "dir3"), false},
				{fs.Join(basePath, "dir0", "dir4"), false},
				{fs.Join(basePath, "dir0", "dir4", "dir5"), false},
				{fs.Join(basePath, "dir0", "dir6"), true},
				{fs.Join(basePath, "dir0", "dir6", "file.txt"), true},
			} {
				exists, err := fs.Exists(ctx, tc.path)
				Expect(err).NotTo(HaveOccurred(), "item %d", i)
				Expect(exists).To(Equal(tc.expected))
			}
		})

	})

	Context("with S3", func() {
		var (
			s3fs        filesystem.FileSystem
			fsLocal     filesystem.FileSystem
			minioClient *minio.Client
			s3Params    filesystem.S3Params
		)
		const (
			region         = ""
			bucketName     = "test-bucket" // no underscores here!
			accessKey      = "minioadmin"
			secretKey      = "minioadmin"
			endpointDocker = "minio:9000"
			endpointLocal  = "localhost:9000"
			ttl            = time.Second // increase on slow computers
		)

		BeforeEach(func() {
			endpoint := endpointLocal
			if utils.IsInDocker() {
				endpoint = endpointDocker
			}

			s3Params = filesystem.S3Params{
				Endpoint:             endpoint,
				Region:               region,
				AccessKey:            accessKey,
				SecretKey:            secretKey,
				UseSSL:               false,
				BucketName:           bucketName,
				OpenedFilesTTL:       ttl,
				OpenedFilesTempDir:   "",
				Logger:               logger,
				ListDirectoryEntries: true,
			}

			fsLocal = filesystem.NewLocal()
			ctx = context.Background()
			const (
				ctxKey   = "key"
				ctxValue = "value"
			)
			filesystem.SetBeforeOperationCB(func(ctx context.Context) (context.Context, error) {
				return context.WithValue(ctx, ctxKey, ctxValue), nil
			})
			filesystem.SetAfterOperationCB(func(ctx context.Context) error {
				if ctx == nil {
					return nil
				}
				Expect(ctx.Value(ctxKey).(string)).To(Equal(ctxValue))
				return nil
			})

			s3fs, err = filesystem.NewS3(ctx, s3Params)
			Expect(err).NotTo(HaveOccurred())

			s3, ok := s3fs.(*filesystem.S3)
			Expect(ok).To(BeTrue())
			Expect(s3).NotTo(BeNil())
			minioClient = s3.MinioClient()
			Expect(minioClient).NotTo(BeNil())
		})

		AfterEach(func() {
			exists, err := fsLocal.Exists(ctx, filesystem.TempDir)
			Expect(err).NotTo(HaveOccurred())
			if exists {
				Expect(fsLocal.RemoveAll(ctx, filesystem.TempDir)).To(Succeed())
			}

			Expect(minioClient.RemoveBucketWithOptions(ctx, bucketName, minio.RemoveBucketOptions{
				ForceDelete: true,
			})).To(Succeed())
		})

		It("checks recursive empty directories tree removal", func() {
			var filePath string
			By("creating directory tree", func() {
				Expect(fs.MakePathAll(ctx, fs.Join(basePath, "dir0", "dir1", "dir2", "dir3"))).To(Succeed())
				Expect(fs.MakePathAll(ctx, fs.Join(basePath, "dir0", "dir4", "dir5"))).To(Succeed())
				Expect(fs.MakePathAll(ctx, fs.Join(basePath, "dir0", "dir6"))).To(Succeed())
				filePath = fs.Join(basePath, "dir0", "dir6", "file.txt")
				Expect(fs.WriteFile(ctx, filePath, []byte("test content"))).To(Succeed())
			})

			Expect(filesystem.RemoveEmptyDirs(ctx, fs, basePath)).To(Succeed())

			for i, tc := range []struct {
				path     string
				expected bool
			}{
				{fs.Join(basePath, "dir0"), true},
				{fs.Join(basePath, "dir0", "dir1"), false},
				{fs.Join(basePath, "dir0", "dir1", "dir2"), false},
				{fs.Join(basePath, "dir0", "dir1", "dir2", "dir3"), false},
				{fs.Join(basePath, "dir0", "dir4"), false},
				{fs.Join(basePath, "dir0", "dir4", "dir5"), false},
				{fs.Join(basePath, "dir0", "dir6"), true},
				{fs.Join(basePath, "dir0", "dir6", "file.txt"), true},
			} {
				exists, err := fs.Exists(ctx, tc.path)
				Expect(err).NotTo(HaveOccurred(), "item %d", i)
				Expect(exists).To(Equal(tc.expected))
			}
		})
	})

})
