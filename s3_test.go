package filesystem_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/mtfelian/filesystem"
	"github.com/mtfelian/utils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/sirupsen/logrus"
)

var testBucketSequence atomic.Uint64

func uniqueTestBucketName(prefix string) string {
	return fmt.Sprintf("%s-%d-%d", prefix, time.Now().UnixNano(), testBucketSequence.Add(1))
}

func newS3ForTest(ctx context.Context, p filesystem.S3Params) (*filesystem.S3Provider, *filesystem.S3, error) {
	provider, err := filesystem.NewS3Provider(filesystem.S3ProviderParams{
		Endpoint:           p.Endpoint,
		Region:             p.Region,
		AccessKey:          p.AccessKey,
		SecretKey:          p.SecretKey,
		UseSSL:             p.UseSSL,
		OpenedFilesTTL:     p.OpenedFilesTTL,
		OpenedFilesTempDir: p.OpenedFilesTempDir,
		Logger:             p.Logger,
	})
	if err != nil {
		return nil, nil, err
	}

	if err = provider.EnsureBucket(ctx, p.BucketName, filesystem.S3BucketOptions{
		CreateIfMissing:      true,
		BucketTTL:            p.BucketTTL,
		EmulateEmptyDirs:     p.EmulateEmptyDirs,
		ListDirectoryEntries: p.ListDirectoryEntries,
	}); err != nil {
		_ = provider.Close()
		return nil, nil, err
	}

	s3, err := provider.Bucket(ctx, p.BucketName)
	if err != nil {
		_ = provider.Close()
		return nil, nil, err
	}
	return provider, s3, nil
}

var _ = Describe("S3 FileSystem implementation", func() {
	var (
		s3fs        filesystem.FileSystem
		fsLocal     filesystem.FileSystem
		err         error
		ctx         context.Context
		logger      logrus.FieldLogger
		minioClient *minio.Client
		s3Params    filesystem.S3Params
		s3Provider  *filesystem.S3Provider
		bucketName  string
	)
	const (
		region         = ""
		noSuchBucket   = "wrong-bucket"
		accessKey      = "minioadmin"
		secretKey      = "minioadmin"
		endpointDocker = "minio:9000"
		endpointLocal  = "localhost:9000"
		ttl            = time.Second // increase on slow computers
	)

	BeforeEach(func() {
		l := logrus.New()
		l.SetLevel(logrus.DebugLevel)
		logger = l

		ctx = context.Background()
		bucketName = uniqueTestBucketName("test-bucket")

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
	})

	AfterEach(func() {
		if s3fs != nil {
			Expect(s3fs.Close()).To(Succeed())
		}
		if s3Provider != nil {
			Expect(s3Provider.Close()).To(Succeed())
		}

		exists, err := fsLocal.Exists(ctx, filesystem.TempDir)
		Expect(err).NotTo(HaveOccurred())
		if exists {
			Expect(fsLocal.RemoveAll(ctx, filesystem.TempDir)).To(Succeed())
		}

		Expect(minioClient.RemoveBucketWithOptions(ctx, bucketName, minio.RemoveBucketOptions{
			ForceDelete: true,
		})).To(Succeed())
	})

	const (
		dir0       = "/a/"
		dir1       = dir0 + "b/"
		dir2       = dir1 + "c_d/"
		key1       = dir2 + "1.txt"
		key2       = dir2 + "2.txt"
		key3       = dir0 + "3.txt"
		noSuchKey  = "/b/c/d/nofile.txt"
		invalidKey = `C:\1\2\3.txt`
		content1   = "content 1"
		content2   = "content 2"
		content3   = "content 3"
	)
	keyToContent := map[string][]byte{
		key1: []byte(content1),
		key2: []byte(content2),
		key3: []byte(content3),
	}

	prepareSpec := func(s3Params filesystem.S3Params) {
		const attempts = 10
		const delay = time.Second
		for i := 1; i <= attempts; i++ {
			var s3 *filesystem.S3
			if s3Provider != nil {
				Expect(s3Provider.Close()).To(Succeed())
				s3Provider = nil
			}
			if s3Provider, s3, err = newS3ForTest(ctx, s3Params); err == nil {
				s3fs = s3
				break
			} else {
				fmt.Printf(">>>>> waiting for minio startup... attempt %d/%d\n", i, attempts)
				time.Sleep(delay)
			}
		}
		Expect(err).NotTo(HaveOccurred())

		s3, ok := s3fs.(*filesystem.S3)
		Expect(ok).To(BeTrue())
		Expect(s3).NotTo(BeNil())
		minioClient = s3.MinioClient()
		Expect(minioClient).NotTo(BeNil())

		By("creating objects", func() {
			for key, content := range keyToContent {
				Expect(s3.WriteFile(ctx, key, content)).To(Succeed())
			}
		})
	}

	type walkDirEntry struct {
		name  string
		isDir bool
	}

	When("EmulateEmptyDirs is true", func() {
		JustBeforeEach(func() {
			s3Params.EmulateEmptyDirs = true
			prepareSpec(s3Params)
		})

		It("checks that created bucket exists", func() {
			exists, err := minioClient.BucketExists(ctx, bucketName)
			Expect(err).NotTo(HaveOccurred())
			Expect(exists).To(BeTrue())
		})

		It("checks that not created bucket does not exists", func() {
			exists, err := minioClient.BucketExists(ctx, noSuchBucket)
			Expect(err).NotTo(HaveOccurred())
			Expect(exists).To(BeFalse())
		})

		It("checks exposing a provider", func() {
			provider := s3fs.(*filesystem.S3).Provider()
			Expect(provider).NotTo(BeNil())
		})

		It("checks idempotent close", func() {
			Expect(s3fs.Close()).To(Succeed())
			Expect(s3fs.Close()).To(Succeed())
		})

		It("checks returning another bucket filesystem from the same provider", func() {
			otherBucket := uniqueTestBucketName("test-bucket-other")
			defer func() {
				Expect(minioClient.RemoveBucketWithOptions(ctx, otherBucket, minio.RemoveBucketOptions{
					ForceDelete: true,
				})).To(Succeed())
			}()

			provider := s3fs.(*filesystem.S3).Provider()
			Expect(provider.EnsureBucket(ctx, otherBucket, filesystem.S3BucketOptions{
				CreateIfMissing:      true,
				EmulateEmptyDirs:     true,
				ListDirectoryEntries: true,
			})).To(Succeed())

			otherS3, err := provider.Bucket(ctx, otherBucket)
			Expect(err).NotTo(HaveOccurred())
			Expect(otherS3.MinioClient()).To(Equal(minioClient))

			Expect(otherS3.WriteFile(ctx, "/other.txt", []byte("other content"))).To(Succeed())
			b, err := otherS3.ReadFile(ctx, "/other.txt")
			Expect(err).NotTo(HaveOccurred())
			Expect(b).To(Equal([]byte("other content")))
		})

		It("checks rejecting conflicting options for an already ensured bucket", func() {
			provider := s3fs.(*filesystem.S3).Provider()
			err := provider.EnsureBucket(ctx, bucketName, filesystem.S3BucketOptions{
				CreateIfMissing:      true,
				BucketTTL:            time.Hour,
				EmulateEmptyDirs:     true,
				ListDirectoryEntries: true,
			})
			Expect(err).To(Equal(filesystem.ErrConflictingBucketOptions))
		})

		It("checks ReadFile on existing objects", func() {
			for key, content := range keyToContent {
				actualContent, err := s3fs.ReadFile(ctx, key)
				Expect(err).NotTo(HaveOccurred())
				Expect(actualContent).To(Equal(content))
			}
		})

		It("checks WriteReader streams object content", func() {
			content := strings.Repeat("streamed s3 content ", 1024)
			key := "/streamed/reader.txt"

			Expect(s3fs.WriteReader(ctx, key, strings.NewReader(content), int64(len(content)))).To(Succeed())

			actualContent, err := s3fs.ReadFile(ctx, key)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(actualContent)).To(Equal(content))
		})

		It("checks WriteReader rejects negative size", func() {
			key := "/streamed/negative-size.txt"

			err := s3fs.WriteReader(ctx, key, strings.NewReader("content"), -1)
			Expect(err).To(Equal(filesystem.ErrInvalidSize))

			exists, err := s3fs.Exists(ctx, key)
			Expect(err).NotTo(HaveOccurred())
			Expect(exists).To(BeFalse())
		})

		It("uses distinct temp names for distinct object keys", func() {
			s3 := s3fs.(*filesystem.S3)
			Expect(s3.TempFileName("/a/b")).NotTo(Equal(s3.TempFileName("/a__b")))
		})

		It("creates directory stubs from normalized WriteFiles names", func() {
			Expect(s3fs.WriteFiles(ctx, []filesystem.FileNameData{{
				Name: `C:\1\2\3.txt`,
				Data: []byte("snowball content"),
			}})).To(Succeed())

			fi, err := s3fs.Stat(ctx, "/1/")
			Expect(err).NotTo(HaveOccurred())
			Expect(fi.IsDir()).To(BeTrue())

			fi, err = s3fs.Stat(ctx, "/1/2/")
			Expect(err).NotTo(HaveOccurred())
			Expect(fi.IsDir()).To(BeTrue())
		})

		Describe("Opening files in various modes, closing and autoclosing", func() {
			var (
				openedFilesList *filesystem.S3OpenedFilesList
				f               filesystem.File
				opened          bool
			)

			JustAfterEach(func() {
				if opened {
					Expect(f.Close()).To(Succeed())
					opened = false
				}
			})

			lookUpForSingleEntry := func() *filesystem.S3OpenedFilesListEntry {
				openedFilesList.Lock()
				defer openedFilesList.Unlock()
				for key, value := range openedFilesList.Map() {
					exists, err := fsLocal.Exists(ctx, key)
					ExpectWithOffset(1, err).NotTo(HaveOccurred())
					ExpectWithOffset(1, exists).To(BeTrue())
					return value
				}
				Fail("openedFilesList is empty")
				return nil
			}

			isExists := func(name string) bool {
				exists, err := fsLocal.Exists(ctx, name)
				ExpectWithOffset(1, err).NotTo(HaveOccurred())
				return exists
			}

			It("allows only one first opener for the same object", func() {
				const amount = 16
				const key = "/a/b/c_d/concurrent-create.txt"
				start := make(chan struct{})
				files := make([]filesystem.File, amount)
				errs := make([]error, amount)

				var wg sync.WaitGroup
				wg.Add(amount)
				for i := 0; i < amount; i++ {
					i := i
					go func() {
						defer GinkgoRecover()
						defer wg.Done()
						<-start
						files[i], errs[i] = s3fs.Create(ctx, key)
					}()
				}
				close(start)
				wg.Wait()

				var opened []filesystem.File
				for i := 0; i < amount; i++ {
					switch errs[i] {
					case nil:
						opened = append(opened, files[i])
					case filesystem.ErrFileAlreadyOpened:
						Expect(files[i]).NotTo(BeNil())
					default:
						Fail(fmt.Sprintf("unexpected error at index %d: %v", i, errs[i]))
					}
				}
				Expect(opened).To(HaveLen(1))
				Expect(opened[0].Close()).To(Succeed())
			})

			It("checks opening not-existing file and it not hangs on second attempt (were a bug)", func() {
				By("opening not existing object 1st time", func() {
					f, err = s3fs.Open(ctx, noSuchKey)
					Expect(err).To(HaveOccurred())
					Expect(f).To(BeNil())

					openedFilesList = s3fs.(*filesystem.S3).OpenedFilesList()
					Expect(openedFilesList).NotTo(BeNil())
					Expect(openedFilesList.Len()).To(BeZero())
				})
				By("opening not existing object 2nd time", func() {
					f, err = s3fs.Open(ctx, noSuchKey)
					Expect(err).To(HaveOccurred())
					Expect(f).To(BeNil())

					openedFilesList = s3fs.(*filesystem.S3).OpenedFilesList()
					Expect(openedFilesList).NotTo(BeNil())
					Expect(openedFilesList.Len()).To(BeZero())
				})
			})

			Context("Opening file for reading", func() {
				JustBeforeEach(func() {
					f, err = s3fs.Open(ctx, key1)
					Expect(err).NotTo(HaveOccurred())
					Expect(f).NotTo(BeNil())
					opened = true

					openedFilesList = s3fs.(*filesystem.S3).OpenedFilesList()
					Expect(openedFilesList).NotTo(BeNil())
					Expect(openedFilesList.Len()).To(Equal(1))
				})

				It("checks Close", func() {
					s3FileEntry := lookUpForSingleEntry()
					Expect(s3FileEntry.Added).To(BeTemporally("~", time.Now(), 2*time.Second))

					name := s3FileEntry.S3File.LocalName()
					Expect(name).To(Equal(s3fs.(*filesystem.S3).TempFileName(key1)))
					Expect(s3FileEntry.S3File.Close()).To(Succeed())
					opened = false

					By("checking that file was removed at Close call", func() {
						Expect(isExists(name)).To(BeFalse())
					})
				})

				It("checks autoclosing", func() {
					s3FileEntry := lookUpForSingleEntry()
					Eventually(func() bool {
						return isExists(s3FileEntry.S3File.LocalName())
					}, 3*ttl, ttl/2).Should(BeFalse()) // to give slightly more time to the cleaner goroutine
					opened = false

					err := s3FileEntry.S3File.Close()
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(ContainSubstring(fs.ErrClosed.Error()))
				})

				It("checks reading from opened file", func() {
					size, err := f.Seek(0, io.SeekEnd)
					Expect(err).NotTo(HaveOccurred())
					Expect(size).To(BeEquivalentTo(len(content1)))

					_, err = f.Seek(0, io.SeekStart)
					Expect(err).NotTo(HaveOccurred())

					b := bytes.NewBuffer([]byte{})
					n, err := io.Copy(b, f)
					Expect(err).NotTo(HaveOccurred())
					Expect(n).To(BeEquivalentTo(size))
					Expect(b.Bytes()).To(BeEquivalentTo([]byte(content1)))
				})

				It("checks that can't write to opened file", func() {
					n, err := f.Write([]byte("123"))
					Expect(err).To(HaveOccurred())
					Expect(n).To(BeZero())
				})
			})

			Context("operations with invalid (Windows) file names", func() {
				It("checks that Create works", func() {
					f1, err := s3fs.Create(ctx, invalidKey)
					Expect(err).NotTo(HaveOccurred()) // name will be converted to normal name
					Expect(f1).NotTo(BeNil())
					Expect(f1.Name()).To(Equal("/1/2/3.txt"))
					defer func() {
						Expect(f1.Close()).To(Succeed())
						Expect(s3fs.Remove(ctx, invalidKey)).To(Succeed())
					}()
				})
			})

			Context("Creating file", func() {
				JustBeforeEach(func() {
					f, err = s3fs.Create(ctx, key1)
					Expect(err).NotTo(HaveOccurred())
					Expect(f).NotTo(BeNil())
					opened = true

					openedFilesList = s3fs.(*filesystem.S3).OpenedFilesList()
					Expect(openedFilesList).NotTo(BeNil())
					Expect(openedFilesList.Len()).To(Equal(1))
				})

				It("checks Close", func() {
					s3FileEntry := lookUpForSingleEntry()
					Expect(s3FileEntry.Added).To(BeTemporally("~", time.Now(), 2*time.Second))

					name := s3FileEntry.S3File.LocalName()
					Expect(name).To(Equal(s3fs.(*filesystem.S3).TempFileName(key1)))
					Expect(s3FileEntry.S3File.Close()).To(Succeed())
					opened = false

					By("checking that file was removed at Close call", func() {
						Expect(isExists(name)).To(BeFalse())
					})
				})

				It("checks autoclosing", func() {
					s3FileEntry := lookUpForSingleEntry()
					Eventually(func() bool {
						return isExists(s3FileEntry.S3File.LocalName())
					}, 3*ttl, ttl/2).Should(BeFalse()) // to give slightly more time to the cleaner goroutine

					opened = false
					err := s3FileEntry.S3File.Close()
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(ContainSubstring(fs.ErrClosed.Error()))
				})

				It("checks that created file is truncated", func() {
					size, err := f.Seek(0, io.SeekEnd)
					Expect(err).NotTo(HaveOccurred())
					Expect(size).To(BeZero())
				})

				It("checks that can read and write to created file", func() {
					content := "123 123"
					By("writing into file", func() {
						n, err := f.Write([]byte(content))
						Expect(err).NotTo(HaveOccurred())
						Expect(n).To(Equal(len(content)))
					})

					By("reading file", func() {
						_, err := f.Seek(0, io.SeekStart)
						Expect(err).NotTo(HaveOccurred())

						b := bytes.NewBuffer([]byte{})
						n, err := io.Copy(b, f)
						Expect(err).NotTo(HaveOccurred())
						Expect(n).To(BeEquivalentTo(len(content)))
					})
				})

				It("checks that an object will be created from written file after it will be opened", func() {
					content := "123 123"
					By("writing into file", func() {
						n, err := f.Write([]byte(content))
						Expect(err).NotTo(HaveOccurred())
						Expect(n).To(Equal(len(content)))
					})
					Expect(f.Close()).To(Succeed())
					opened = false

					By("reading from object", func() {
						b, err := s3fs.ReadFile(ctx, key1)
						Expect(err).NotTo(HaveOccurred())
						Expect(b).To(BeEquivalentTo([]byte(content)))
					})
				})
			})

			Context("Opening file for writing", func() {
				JustBeforeEach(func() {
					f, err = s3fs.OpenW(ctx, key1)
					Expect(err).NotTo(HaveOccurred())
					Expect(f).NotTo(BeNil())
					opened = true

					openedFilesList = s3fs.(*filesystem.S3).OpenedFilesList()
					Expect(openedFilesList).NotTo(BeNil())
					Expect(openedFilesList.Len()).To(Equal(1))
				})

				It("checks Close", func() {
					s3FileEntry := lookUpForSingleEntry()
					Expect(s3FileEntry.Added).To(BeTemporally("~", time.Now(), 2*time.Second))

					name := s3FileEntry.S3File.LocalName()
					Expect(name).To(Equal(s3fs.(*filesystem.S3).TempFileName(key1)))
					Expect(s3FileEntry.S3File.Close()).To(Succeed())
					opened = false

					By("checking that file was removed at Close call", func() {
						Expect(isExists(name)).To(BeFalse())
					})
				})

				It("checks autoclosing", func() {
					s3FileEntry := lookUpForSingleEntry()
					Eventually(func() bool {
						return isExists(s3FileEntry.S3File.LocalName())
					}, 3*ttl, ttl/2).Should(BeFalse()) // to give slightly more time to the cleaner goroutine
					opened = false

					err := s3FileEntry.S3File.Close()
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(ContainSubstring(fs.ErrClosed.Error()))
				})

				It("checks writing to opened file, then closing and reading from object", func() {
					content := "123 456"
					By("writing into file", func() {
						n, err := f.Write([]byte(content))
						Expect(err).NotTo(HaveOccurred())
						Expect(n).To(Equal(len(content)))
					})
					Expect(f.Close()).To(Succeed())
					opened = false

					expectedContent := []byte("123 456 1")
					b, err := s3fs.ReadFile(ctx, key1)
					Expect(err).NotTo(HaveOccurred())
					Expect(b).To(BeEquivalentTo(expectedContent), "should be partially overwritten")

					objectInfo, err := minioClient.StatObject(ctx, bucketName, strings.TrimPrefix(path.Clean(key1), "/"),
						minio.StatObjectOptions{})
					Expect(err).NotTo(HaveOccurred())
					Expect(objectInfo.ContentType).To(Equal(http.DetectContentType(expectedContent)))
				})

				It("checks Sync on opened write-only local file", func() {
					content := "sync data"
					n, err := f.Write([]byte(content))
					Expect(err).NotTo(HaveOccurred())
					Expect(n).To(Equal(len(content)))

					Expect(f.Sync()).To(Succeed())

					b, err := s3fs.ReadFile(ctx, key1)
					Expect(err).NotTo(HaveOccurred())
					Expect(b).To(Equal([]byte(content)))

					objectInfo, err := minioClient.StatObject(ctx, bucketName, strings.TrimPrefix(path.Clean(key1), "/"),
						minio.StatObjectOptions{})
					Expect(err).NotTo(HaveOccurred())
					Expect(objectInfo.ContentType).To(Equal(http.DetectContentType([]byte(content))))
				})

				It("checks that Stat.Size and SeekEnd returns same size", func() {
					fi, err := lookUpForSingleEntry().S3File.Stat()
					Expect(err).NotTo(HaveOccurred())
					Expect(fi).NotTo(BeNil())

					size := fi.Size()
					n, err := f.Seek(0, io.SeekEnd)
					Expect(err).NotTo(HaveOccurred())
					Expect(size).To(Equal(n))
				})
			})

			Context("concurrent opening file for reading (writing behavior expected to be same)", func() {
				JustBeforeEach(func() {
					f, err = s3fs.Open(ctx, key1)
					Expect(err).NotTo(HaveOccurred())
					Expect(f).NotTo(BeNil())
					opened = true

					openedFilesList = s3fs.(*filesystem.S3).OpenedFilesList()
					Expect(openedFilesList).NotTo(BeNil())
					Expect(openedFilesList.Len()).To(Equal(1))
				})

				It("checks concurrent Open with Open", func() {
					By("checking that opened file is still on the list", func() {
						openedFilesList = s3fs.(*filesystem.S3).OpenedFilesList()
						Expect(openedFilesList).NotTo(BeNil())
						Expect(openedFilesList.Len()).To(Equal(1))
					})

					var wg sync.WaitGroup
					amount := 5
					wg.Add(amount)
					f := func() {
						defer GinkgoRecover()
						defer wg.Done()
						fw, err := s3fs.Open(ctx, key1)
						Expect(err).To(Equal(filesystem.ErrFileAlreadyOpened))
						Expect(fw).NotTo(BeNil(), "should be internally cached S3OpenedFile")

						By("checking that opened file is again on the list", func() {
							openedFilesList = s3fs.(*filesystem.S3).OpenedFilesList()
							Expect(openedFilesList).NotTo(BeNil())
							Expect(openedFilesList.Len()).To(Equal(1))
						})
					}
					for i := 0; i < amount; i++ {
						go f()
					}
					wg.Wait()

					By("waiting for autoclosing", func() {
						s3FileEntry := lookUpForSingleEntry()
						Eventually(func() bool {
							return isExists(s3FileEntry.S3File.LocalName())
						}, 3*ttl, ttl/2).Should(BeFalse()) // to give slightly more time to the cleaner goroutine
						opened = false
					})

					By("checking that opened file is no more on the list", func() {
						openedFilesList = s3fs.(*filesystem.S3).OpenedFilesList()
						Expect(openedFilesList).NotTo(BeNil())
						Expect(openedFilesList.Len()).To(Equal(0))
					})
				})
			})
		})

		Describe("Reader", func() {
			It("checks streamed reading", func() {
				r, err := s3fs.Reader(ctx, key2)
				Expect(err).NotTo(HaveOccurred())

				b, err := io.ReadAll(r)
				Expect(err).NotTo(HaveOccurred())
				Expect(b).To(BeEquivalentTo(content2))
			})
		})

		Describe("Exists", func() {
			It("checks that Exists returns true for existing object", func() {
				exists, err := s3fs.Exists(ctx, key2)
				Expect(err).NotTo(HaveOccurred())
				Expect(exists).To(BeTrue())
			})

			It("checks that Exists returns false for not existing object", func() {
				exists, err := s3fs.Exists(ctx, noSuchKey)
				Expect(err).NotTo(HaveOccurred())
				Expect(exists).To(BeFalse())
			})

			It("checks that Exists returns non-nil error for object with invalid name", func() {
				_, err := s3fs.Exists(ctx, strings.Repeat("1", 1025)) // name too long
				Expect(err).To(HaveOccurred())
			})

			It("checks that PreparePath returns Exists errors", func() {
				_, err := s3fs.PreparePath(ctx, strings.Repeat("1", 1025)) // name too long
				Expect(err).To(HaveOccurred())
			})
		})

		Describe("MakePathAll, Exists on empty folder", func() {
			folderPath := "/1/2/3/4"
			It("checks that MakePathAll creates a stub file to precreate empty folder", func() {
				Expect(s3fs.MakePathAll(ctx, folderPath)).To(Succeed())

				By("checking Exists on empty folder path without trailing '/'", func() {
					exists, err := s3fs.Exists(ctx, folderPath)
					Expect(err).NotTo(HaveOccurred())
					Expect(exists).To(BeFalse(), "should be recognized as a file")
				})
				By("checking Exists on empty folder path with trailing '/'", func() {
					exists, err := s3fs.Exists(ctx, folderPath+"/")
					Expect(err).NotTo(HaveOccurred())
					Expect(exists).To(BeTrue(), "should be recognized as a directory")
				})
				By("checking Exists on file stub", func() {
					exists, err := s3fs.Exists(ctx, folderPath+"/"+filesystem.DirStubFileName)
					Expect(err).NotTo(HaveOccurred())
					Expect(exists).To(BeTrue(), "should be recognized as a directory stub file")
				})
			})

			It("checks that MakePathAll correctly works on existing path", func() {
				Expect(s3fs.MakePathAll(ctx, folderPath)).To(Succeed())
				Expect(s3fs.MakePathAll(ctx, folderPath)).To(Succeed())

				By("checking Exists on empty folder path with trailing '/'", func() {
					exists, err := s3fs.Exists(ctx, folderPath+"/")
					Expect(err).NotTo(HaveOccurred())
					Expect(exists).To(BeTrue(), "should be recognized as a directory")
				})
			})
		})

		Describe("RemoveFiles", func() {
			It("checks batch removing all existing objects", func() {
				failed, err := s3fs.RemoveFiles(ctx, []string{key2, key3, key1})
				Expect(err).NotTo(HaveOccurred())
				Expect(failed).To(BeEmpty())
				By("checking that removed objects no more exists", func() {
					exists, err := s3fs.Exists(ctx, key1)
					Expect(err).NotTo(HaveOccurred())
					Expect(exists).To(BeFalse())

					exists, err = s3fs.Exists(ctx, key2)
					Expect(err).NotTo(HaveOccurred())
					Expect(exists).To(BeFalse())

					exists, err = s3fs.Exists(ctx, key3)
					Expect(err).NotTo(HaveOccurred())
					Expect(exists).To(BeFalse())
				})
			})

			It("checks batch removing mix of existing and not existing objects", func() {
				_, err := s3fs.RemoveFiles(ctx, []string{key2, noSuchKey})
				Expect(err).NotTo(HaveOccurred())
				By("checking that removed objects no more exists", func() {
					exists, err := s3fs.Exists(ctx, key1)
					Expect(err).NotTo(HaveOccurred())
					Expect(exists).To(BeTrue())

					exists, err = s3fs.Exists(ctx, key2)
					Expect(err).NotTo(HaveOccurred())
					Expect(exists).To(BeFalse())

					exists, err = s3fs.Exists(ctx, key3)
					Expect(err).NotTo(HaveOccurred())
					Expect(exists).To(BeTrue())
				})
			})

			It("checks batch removing an emulated empty directory", func() {
				const emptyDir = "/empty/removefiles/"
				Expect(s3fs.MakePathAll(ctx, emptyDir)).To(Succeed())

				exists, err := s3fs.Exists(ctx, emptyDir)
				Expect(err).NotTo(HaveOccurred())
				Expect(exists).To(BeTrue())

				failed, err := s3fs.RemoveFiles(ctx, []string{emptyDir})
				Expect(err).NotTo(HaveOccurred())
				Expect(failed).To(BeEmpty())

				exists, err = s3fs.Exists(ctx, emptyDir)
				Expect(err).NotTo(HaveOccurred())
				Expect(exists).To(BeFalse())

				exists, err = s3fs.Exists(ctx, emptyDir+filesystem.DirStubFileName)
				Expect(err).NotTo(HaveOccurred())
				Expect(exists).To(BeFalse())
			})

			Context("non-empty dir", func() {
				It("checks batch removing a non-empty directory path with '/', should not succeed, "+
					"nothing should be removed", func() {
					failed, err := s3fs.RemoveFiles(ctx, []string{key3, dir2})
					Expect(err).To(HaveOccurred())
					Expect(failed).To(BeEmpty())
					By("checking folder path existence", func() {
						exists, err := s3fs.Exists(ctx, dir2)
						Expect(err).NotTo(HaveOccurred())
						Expect(exists).To(BeTrue())
					})
					By("checking object existence", func() {
						exists, err := s3fs.Exists(ctx, key3)
						Expect(err).NotTo(HaveOccurred())
						Expect(exists).To(BeTrue())
					})
				})

				It("checks batch removing a non-empty directory path with no '/', "+
					"it should be nothing, like non-existent file removal, "+
					"but other objects should be removed", func() {
					dir := strings.TrimSuffix(dir2, "/")
					failed, err := s3fs.RemoveFiles(ctx, []string{key3, dir})
					Expect(err).NotTo(HaveOccurred())
					Expect(failed).To(BeEmpty())
					By("checking folder path existence", func() {
						exists, err := s3fs.Exists(ctx, dir2) // as a directory path
						Expect(err).NotTo(HaveOccurred())
						Expect(exists).To(BeTrue())
					})
					By("checking objects existence", func() {
						exists, err := s3fs.Exists(ctx, key2)
						Expect(err).NotTo(HaveOccurred())
						Expect(exists).To(BeTrue())

						exists, err = s3fs.Exists(ctx, key3)
						Expect(err).NotTo(HaveOccurred())
						Expect(exists).To(BeFalse())
					})
				})
			})
		})

		Describe("Remove, should be applied to objects or empty folders", func() {
			It("checks removing existing object", func() {
				Expect(s3fs.Remove(ctx, key2)).To(Succeed())
				By("checking that removed object no more exists", func() {
					exists, err := s3fs.Exists(ctx, key2)
					Expect(err).NotTo(HaveOccurred())
					Expect(exists).To(BeFalse())
				})
			})

			It("checks removing not-existing object, should succeed also", func() {
				Expect(s3fs.Remove(ctx, noSuchKey)).To(Succeed())
				By("checking that removed object still not exists", func() {
					exists, err := s3fs.Exists(ctx, noSuchKey)
					Expect(err).NotTo(HaveOccurred())
					Expect(exists).To(BeFalse())
				})
			})

			Context("non-empty dir", func() {
				It("checks removing a non-empty directory path with '/', should not succeed", func() {
					Expect(s3fs.Remove(ctx, dir2)).NotTo(Succeed())
					By("checking folder path existence", func() {
						exists, err := s3fs.Exists(ctx, dir2)
						Expect(err).NotTo(HaveOccurred())
						Expect(exists).To(BeTrue())
					})
					By("checking object existence", func() {
						exists, err := s3fs.Exists(ctx, key2)
						Expect(err).NotTo(HaveOccurred())
						Expect(exists).To(BeTrue())
					})
				})

				It("checks removing a non-empty directory path with no '/', "+
					"it should be nothing, like non-existent file removal", func() {
					dir := strings.TrimSuffix(dir2, "/")
					Expect(s3fs.Remove(ctx, dir)).To(Succeed())
					By("checking folder path existence", func() {
						exists, err := s3fs.Exists(ctx, dir2) // as a directory path
						Expect(err).NotTo(HaveOccurred())
						Expect(exists).To(BeTrue())
					})
					By("checking object existence", func() {
						exists, err := s3fs.Exists(ctx, key2)
						Expect(err).NotTo(HaveOccurred())
						Expect(exists).To(BeTrue())
					})
				})
			})

			Context("empty dir", func() {
				JustBeforeEach(func() {
					By("removing all (2) objects in a directory", func() {
						Expect(s3fs.Remove(ctx, key1)).To(Succeed())
						Expect(s3fs.Remove(ctx, key2)).To(Succeed())
					})
				})

				It("checks removing an empty directory path, it should be removed", func() {
					Expect(s3fs.Remove(ctx, dir2)).To(Succeed())
					By("checking folder path existence", func() {
						exists, err := s3fs.Exists(ctx, dir2)
						Expect(err).NotTo(HaveOccurred())
						Expect(exists).To(BeFalse())
					})
				})

				It("checks removing an empty directory path with no '/', "+
					"it should be nothing, like non-existent file removal", func() {
					dir := strings.TrimSuffix(dir2, "/")
					Expect(s3fs.Remove(ctx, dir)).To(Succeed())
					By("checking folder path existence", func() {
						exists, err := s3fs.Exists(ctx, dir2) // as a directory path
						Expect(err).NotTo(HaveOccurred())
						Expect(exists).To(BeTrue())
					})
				})
			})

			It("checks removing a non-existent directory path with '/', should not succeed", func() {
				dir := s3fs.Dir("/4/5/6/7/")
				Expect(s3fs.Remove(ctx, dir)).To(Succeed())
				By("checking folder path existence", func() {
					exists, err := s3fs.Exists(ctx, dir)
					Expect(err).NotTo(HaveOccurred())
					Expect(exists).To(BeFalse())
				})
			})
		})

		Describe("RemoveAll, should be applied to folders but not objects", func() {
			It("checks removing exact object does not remove same-prefix objects", func() {
				const (
					objectName = "/same-prefix"
					neighbor   = "/same-prefix-neighbor"
				)
				Expect(s3fs.WriteFile(ctx, objectName, []byte("object"))).To(Succeed())
				Expect(s3fs.WriteFile(ctx, neighbor, []byte("neighbor"))).To(Succeed())

				Expect(s3fs.RemoveAll(ctx, objectName)).To(Succeed())

				exists, err := s3fs.Exists(ctx, objectName)
				Expect(err).NotTo(HaveOccurred())
				Expect(exists).To(BeFalse())

				exists, err = s3fs.Exists(ctx, neighbor)
				Expect(err).NotTo(HaveOccurred())
				Expect(exists).To(BeTrue())
			})

			It("checks removing existing object", func() {
				Expect(s3fs.RemoveAll(ctx, key2)).To(Succeed())
				By("checking that removed object no more exists", func() {
					exists, err := s3fs.Exists(ctx, key2)
					Expect(err).NotTo(HaveOccurred())
					Expect(exists).To(BeFalse())
				})
			})

			It("checks removing not-existing object, should succeed also", func() {
				Expect(s3fs.RemoveAll(ctx, noSuchKey)).To(Succeed())
				By("checking that removed object still not exists", func() {
					exists, err := s3fs.Exists(ctx, noSuchKey)
					Expect(err).NotTo(HaveOccurred())
					Expect(exists).To(BeFalse())
				})
			})

			Context("non-empty dir", func() {
				It("checks removing a non-empty directory path with '/', should succeed", func() {
					Expect(s3fs.RemoveAll(ctx, dir2)).To(Succeed())
					By("checking folder path existence", func() {
						exists, err := s3fs.Exists(ctx, dir2)
						Expect(err).NotTo(HaveOccurred())
						Expect(exists).To(BeFalse())
					})
					By("checking object existence", func() {
						exists, err := s3fs.Exists(ctx, key2)
						Expect(err).NotTo(HaveOccurred())
						Expect(exists).To(BeFalse())
					})
				})

				It("checks removing a non-empty directory path with no '/', "+
					"it should be nothing, like non-existent file removal", func() {
					dir := strings.TrimSuffix(dir2, "/")
					Expect(s3fs.Remove(ctx, dir)).To(Succeed())

					By("checking folder path existence", func() {
						exists, err := s3fs.Exists(ctx, dir2) // as a directory path
						Expect(err).NotTo(HaveOccurred())
						Expect(exists).To(BeTrue())
					})
					By("checking object existence", func() {
						exists, err := s3fs.Exists(ctx, key2)
						Expect(err).NotTo(HaveOccurred())
						Expect(exists).To(BeTrue())
					})
				})
			})
		})

		Describe("IsEmptyPath", func() {
			It("checks for existing non-empty path ends in '/'", func() {
				By("checking level 1", func() {
					isEmpty, err := s3fs.IsEmptyPath(ctx, "/a/b/")
					Expect(err).NotTo(HaveOccurred())
					Expect(isEmpty).To(BeFalse())
				})
				By("checking level 2", func() {
					isEmpty, err := s3fs.IsEmptyPath(ctx, "/a/")
					Expect(err).NotTo(HaveOccurred())
					Expect(isEmpty).To(BeFalse())
				})
				By("checking level 3", func() {
					isEmpty, err := s3fs.IsEmptyPath(ctx, "/a/b/c_d")
					Expect(err).NotTo(HaveOccurred())
					Expect(isEmpty).To(BeFalse())
				})
			})

			It("checks for existing non-empty path not ends in '/'", func() {
				isEmpty, err := s3fs.IsEmptyPath(ctx, "/a/b")
				Expect(err).NotTo(HaveOccurred())
				Expect(isEmpty).To(BeFalse())
			})

			It("checks for not-existing path, should be as empty", func() {
				isEmpty, err := s3fs.IsEmptyPath(ctx, "/1/2/3/4/")
				Expect(err).NotTo(HaveOccurred())
				Expect(isEmpty).To(BeTrue())
			})

			It("checks for existing path with stub", func() {
				const dir = "/1/2/3/4/"
				By("creating path and checking existence", func() {
					Expect(s3fs.MakePathAll(ctx, dir)).To(Succeed())
					exists, err := s3fs.Exists(ctx, dir+filesystem.DirStubFileName)
					Expect(err).NotTo(HaveOccurred())
					Expect(exists).To(BeTrue())
				})
				isEmpty, err := s3fs.IsEmptyPath(ctx, dir)
				Expect(err).NotTo(HaveOccurred())
				Expect(isEmpty).To(BeTrue())
			})
		})

		Describe("Rename", func() {
			It("checks renaming object to new name which does not exists", func() {
				Expect(s3fs.Rename(ctx, key1, noSuchKey)).To(Succeed())
				b, err := s3fs.ReadFile(ctx, noSuchKey)
				Expect(err).NotTo(HaveOccurred())
				Expect(b).To(BeEquivalentTo(content1))

				By("check that every subdir have stub file", func() {
					for dirName := s3fs.Dir(noSuchKey); dirName != "/"; dirName = s3fs.Dir(dirName) {
						exists, err := s3fs.Exists(ctx, dirName+"/"+filesystem.DirStubFileName)
						Expect(err).NotTo(HaveOccurred(), "checking stub existence err, dirname=%q", dirName)
						Expect(exists).To(BeTrue(), "checking stub existence, dirname=%q", dirName)
					}
				})
			})

			It("checks renaming object to new name which already exists, should be replaced", func() {
				Expect(s3fs.Rename(ctx, key1, key3)).To(Succeed())
				b, err := s3fs.ReadFile(ctx, key3)
				Expect(err).NotTo(HaveOccurred())
				Expect(b).To(BeEquivalentTo(content1))
			})

			It("checks renaming object to itself, do nothing", func() {
				Expect(s3fs.Rename(ctx, key1, key1)).To(Succeed())
				b, err := s3fs.ReadFile(ctx, key1)
				Expect(err).NotTo(HaveOccurred())
				Expect(b).To(BeEquivalentTo(content1))
			})

			It("checks renaming not existing object, should fail", func() {
				Expect(s3fs.Rename(ctx, noSuchKey, key1)).NotTo(Succeed())
				b, err := s3fs.ReadFile(ctx, key1)
				Expect(err).NotTo(HaveOccurred())
				Expect(b).To(BeEquivalentTo(content1))
			})

			Context("renaming directory", func() {
				const (
					existingDir    = "/a/"
					notExistingDir = "/d/"
				)
				It("checks renaming existing directory into object, should fail", func() {
					Expect(s3fs.Rename(ctx, existingDir, key1)).To(Equal(filesystem.ErrDestinationPathIsNotDirectory))
				})

				It("checks renaming existing directory into another directory", func() {
					Expect(s3fs.Rename(ctx, existingDir, notExistingDir)).To(Succeed())

					By("checking presence of new directory", func() {
						exists, err := s3fs.Exists(ctx, notExistingDir)
						Expect(err).NotTo(HaveOccurred())
						Expect(exists).To(BeTrue())
					})
					By("checking absence of moved directory", func() {
						exists, err := s3fs.Exists(ctx, existingDir)
						Expect(err).NotTo(HaveOccurred())
						Expect(exists).To(BeFalse())
					})

					By("checking presence of moved objects", func() {
						for key, content := range map[string][]byte{
							"/d/b/c_d/1.txt": []byte(content1),
							"/d/b/c_d/2.txt": []byte(content2),
							"/d/3.txt":       []byte(content3),
						} {
							By("checking existence of objects in new directory", func() {
								exists, err := s3fs.Exists(ctx, key)
								Expect(err).NotTo(HaveOccurred())
								Expect(exists).To(BeTrue(), "object %q should exist", key)
							})

							By("checking their content", func() {
								b, err := s3fs.ReadFile(ctx, key)
								Expect(err).NotTo(HaveOccurred())
								Expect(b).To(BeEquivalentTo(content), "object %q should be equivalent", key)
							})
						}

						By("checking absence of moved objects", func() {
							for key := range keyToContent {
								exists, err := s3fs.Exists(ctx, key)
								Expect(err).NotTo(HaveOccurred())
								Expect(exists).To(BeFalse(), "object %q should not exist", key)
							}
						})
					})
				})

				It("checks renaming not existing directory into existing directory", func() {
					Expect(s3fs.Rename(ctx, notExistingDir, existingDir)).NotTo(Succeed())
					By("checking presence of target directory objects", func() {
						for key := range keyToContent {
							exists, err := s3fs.Exists(ctx, key)
							Expect(err).NotTo(HaveOccurred())
							Expect(exists).To(BeTrue(), "object %q should exist", key)
						}
					})
				})

				Describe("renaming of stub object", func() {
					const (
						stub   = "/a/b/c_d/" + filesystem.DirStubFileName
						target = "/a/b/c_d/no-object.txt"
					)

					It("checks renaming from stub object, should fail", func() {
						Expect(s3fs.Rename(ctx, stub, target)).To(Equal(filesystem.ErrCantUseRenameWithStubObject))
						By("checking that stub exists", func() {
							exists, err := s3fs.Exists(ctx, stub)
							Expect(err).NotTo(HaveOccurred())
							Expect(exists).To(BeTrue())
						})
						By("checking that target does not exists", func() {
							exists, err := s3fs.Exists(ctx, target)
							Expect(err).NotTo(HaveOccurred())
							Expect(exists).To(BeFalse())
						})
					})

					It("checks renaming to stub object, should fail", func() {
						By("creating target object", func() {
							Expect(s3fs.WriteFile(ctx, target, []byte("123"))).To(Succeed())
						})
						Expect(s3fs.Rename(ctx, target, stub)).To(Equal(filesystem.ErrCantUseRenameWithStubObject))
						By("checking that stub exists and contains stub content", func() {
							exists, err := s3fs.Exists(ctx, stub)
							Expect(err).NotTo(HaveOccurred())
							Expect(exists).To(BeTrue())

							b, err := s3fs.ReadFile(ctx, stub)
							Expect(err).NotTo(HaveOccurred())
							Expect(b).To(BeEquivalentTo([]byte(filesystem.DirStubFileContent)))
						})
						By("checking that target still exists", func() {
							exists, err := s3fs.Exists(ctx, target)
							Expect(err).NotTo(HaveOccurred())
							Expect(exists).To(BeTrue())
						})
					})
				})
			})
		})

		Describe("Stat", func() {
			It("checks for an existing object", func() {
				fi, err := s3fs.Stat(ctx, key1)
				Expect(err).NotTo(HaveOccurred())
				Expect(fi.ModTime()).To(BeTemporally("~", time.Now(), 2*time.Second))
				Expect(fi.IsDir()).To(BeFalse())
				Expect(fi.FullName()).To(Equal(key1))
				Expect(fi.Name()).To(Equal(path.Base(key1)))
				Expect(fi.Size()).To(BeEquivalentTo(len(content1)))
			})

			It("checks for not existing object", func() {
				_, err := s3fs.Stat(ctx, noSuchKey)
				Expect(err).To(HaveOccurred())
			})

			It("checks for existing directory", func() {
				fi, err := s3fs.Stat(ctx, dir2)
				Expect(err).NotTo(HaveOccurred())
				Expect(fi.ModTime()).To(BeTemporally("~", time.Now(), 2*time.Second))
				Expect(fi.IsDir()).To(BeTrue())
				Expect(fi.FullName()).To(Equal(dir2))
				Expect(fi.Name()).To(Equal(path.Base(dir2)))
				Expect(fi.Size()).To(BeZero())
			})

			It("checks for not existing directory", func() {
				_, err := s3fs.Stat(ctx, "/4/5/6/7/")
				Expect(err).To(HaveOccurred())
			})
		})

		Describe("ReadDir", func() {
			It("checks if object is not a dir", func() {
				_, err := s3fs.ReadDir(ctx, key1)
				Expect(err).To(Equal(filesystem.ErrNotADirectory))
			})

			It("checks reading existing non-empty dir with objects", func() {
				fi, err := s3fs.ReadDir(ctx, dir2)
				Expect(err).NotTo(HaveOccurred())
				Expect(fi).To(HaveLen(2))
				names := fi.FullNames()
				Expect(names).To(ConsistOf([]string{key1, key2}))
			})

			When("ListDirectoryEntries is false", func() {
				JustBeforeEach(func() {
					s3fs.(*filesystem.S3).SetListDirectoryEntries(false)
				})

				It("checks reading existing non-empty dir with subdirs and objects", func() {
					fi, err := s3fs.ReadDir(ctx, dir0)
					Expect(err).NotTo(HaveOccurred())
					Expect(fi).To(HaveLen(1))
					names := fi.FullNames()
					Expect(names).To(ConsistOf([]string{key3}))
				})

				It("checks reading existing empty dir", func() {
					By("removing everything inside dir0", func() {
						Expect(s3fs.Remove(ctx, key3)).To(Succeed())
						Expect(s3fs.RemoveAll(ctx, dir1)).To(Succeed())
					})

					fi, err := s3fs.ReadDir(ctx, dir0)
					Expect(err).NotTo(HaveOccurred())
					Expect(fi).To(BeEmpty())
					names := fi.FullNames()
					Expect(names).To(BeEmpty())
				})

				It("checks for large amount of objects", func() {
					const (
						amount   = 1500
						largeDir = "/manyfiles/"
					)
					nameFunc := func(i int) string { return fmt.Sprintf("%sitem %d", largeDir, i) }
					contentFunc := func(i int) []byte { return []byte(fmt.Sprintf("content %d", i)) }
					logger := s3fs.(*filesystem.S3).Logger()
					logger.Info("creating large S3 directory...")
					By("creating directory and objects", func() {
						for i := 0; i < amount; i++ {
							if (i+1)%100 == 0 || i == amount-1 {
								logger.Infof("creating test S3 objects... %.2f%%", float64(i+1)*100./float64(amount))
							}
							Expect(s3fs.WriteFile(ctx, nameFunc(i), contentFunc(i))).To(Succeed())
						}
					})

					By("reading directory", func() {
						logger.Info("reading large S3 directory...")
						fsi, err := s3fs.ReadDir(ctx, largeDir)
						Expect(err).NotTo(HaveOccurred())
						Expect(fsi).To(HaveLen(amount))
					})
				})

				It("checks for large amount of objects as a snowball", func() {
					const (
						amount   = 1500
						largeDir = "/manyfiles/"
					)
					nameFunc := func(i int) string { return fmt.Sprintf("%sitem %d", largeDir, i) }
					contentFunc := func(i int) []byte { return []byte(fmt.Sprintf("content %d", i)) }
					logger := s3fs.(*filesystem.S3).Logger()
					logger.Info("creating large S3 directory...")
					By("creating directory and objects", func() {
						files := make([]filesystem.FileNameData, amount)
						for i := 0; i < amount; i++ {
							if (i+1)%100 == 0 || i == amount-1 {
								logger.Infof("(snowball) creating test S3 objects... %.2f%%", float64(i+1)*100./float64(amount))
							}
							files[i] = filesystem.FileNameData{Name: nameFunc(i), Data: contentFunc(i)}
						}
						Expect(s3fs.WriteFiles(ctx, files)).To(Succeed())
					})

					By("reading directory", func() {
						logger.Info("reading large S3 directory...")
						fsi, err := s3fs.ReadDir(ctx, largeDir)
						Expect(err).NotTo(HaveOccurred())
						Expect(fsi).To(HaveLen(amount))
					})
				})
			})

			When("ListDirectoryEntries is true", func() {
				JustBeforeEach(func() {
					s3fs.(*filesystem.S3).SetListDirectoryEntries(true)
				})

				It("checks reading existing non-empty dir with subdirs and objects", func() {
					fi, err := s3fs.ReadDir(ctx, dir0)
					Expect(err).NotTo(HaveOccurred())
					Expect(fi).To(HaveLen(2))
					names := fi.FullNames()
					Expect(names).To(ConsistOf([]string{dir1, key3}))
				})

				It("checks reading existing empty dir", func() {
					By("removing everything inside dir0", func() {
						Expect(s3fs.Remove(ctx, key3)).To(Succeed())
						Expect(s3fs.RemoveAll(ctx, dir1)).To(Succeed())
					})

					fi, err := s3fs.ReadDir(ctx, dir0)
					Expect(err).NotTo(HaveOccurred())
					Expect(fi).To(BeEmpty())
					names := fi.FullNames()
					Expect(names).To(BeEmpty())
				})
			})
		})

		Describe("WalkDir", func() {
			It("checks for root directory", func() {
				var entriesWalked []walkDirEntry
				Expect(s3fs.WalkDir(ctx, "/", func(name string, de filesystem.DirEntry, e error) error {
					if de != nil {
						entriesWalked = append(entriesWalked, walkDirEntry{name: de.FullName(), isDir: de.IsDir()})
					}
					return nil
				})).To(Succeed())
				Expect(entriesWalked).To(ConsistOf([]walkDirEntry{
					{name: "/", isDir: true},
					{name: "/a/", isDir: true},
					{name: "/a/3.txt", isDir: false},
					{name: "/a/b/", isDir: true},
					{name: "/a/b/c_d/", isDir: true},
					{name: "/a/b/c_d/1.txt", isDir: false},
					{name: "/a/b/c_d/2.txt", isDir: false},
				}))
			})

			It("checks for non-root directory", func() {
				var entriesWalked []walkDirEntry
				Expect(s3fs.WalkDir(ctx, dir2, func(name string, de filesystem.DirEntry, e error) error {
					if de != nil {
						entriesWalked = append(entriesWalked, walkDirEntry{name: de.FullName(), isDir: de.IsDir()})
					}
					return nil
				})).To(Succeed())
				Expect(entriesWalked).To(ConsistOf([]walkDirEntry{
					{name: "/a/b/c_d/", isDir: true},
					{name: "/a/b/c_d/1.txt", isDir: false},
					{name: "/a/b/c_d/2.txt", isDir: false},
				}))
			})

			It("checks for an object (not a directory)", func() {
				var entriesWalked []walkDirEntry
				Expect(s3fs.WalkDir(ctx, key2, func(name string, de filesystem.DirEntry, e error) error {
					if de != nil {
						entriesWalked = append(entriesWalked, walkDirEntry{name: de.FullName(), isDir: de.IsDir()})
					}
					return nil
				})).To(Succeed())
				Expect(entriesWalked).To(ConsistOf([]walkDirEntry{
					{name: "/a/b/c_d/2.txt", isDir: false},
				}))
			})

			It("checks for not-existing directory", func() {
				var entriesWalked []walkDirEntry
				Expect(s3fs.IsNotExist(s3fs.WalkDir(ctx, "/4/5/6/7/", func(name string, de filesystem.DirEntry, e error) error {
					if de != nil {
						entriesWalked = append(entriesWalked, walkDirEntry{name: de.FullName(), isDir: de.IsDir()})
					}
					return nil
				}))).To(BeTrue())
				Expect(entriesWalked).To(BeEmpty())
			})

			It("checks for not-existing object", func() {
				var entriesWalked []walkDirEntry
				Expect(s3fs.IsNotExist(s3fs.WalkDir(ctx, "/4/5/6/7", func(name string, de filesystem.DirEntry, e error) error {
					if de != nil {
						entriesWalked = append(entriesWalked, walkDirEntry{name: de.FullName(), isDir: de.IsDir()})
					}
					return nil
				}))).To(BeTrue())
				Expect(entriesWalked).To(BeEmpty())
			})
		})
	})

	When("EmulateEmptyDirs is false (only additional cases)", func() {
		JustBeforeEach(func() {
			s3Params.EmulateEmptyDirs = false
			prepareSpec(s3Params)
		})

		Describe("Exists", func() {
			It("checks that Exists returns true for existing object", func() {
				exists, err := s3fs.Exists(ctx, key2)
				Expect(err).NotTo(HaveOccurred())
				Expect(exists).To(BeTrue())
			})

			It("checks that Exists returns false for not existing object", func() {
				exists, err := s3fs.Exists(ctx, noSuchKey)
				Expect(err).NotTo(HaveOccurred())
				Expect(exists).To(BeFalse())
			})

			It("checks that Exists returns non-nil error for object with invalid name", func() {
				_, err := s3fs.Exists(ctx, strings.Repeat("1", 1025)) // name too long
				Expect(err).To(HaveOccurred())
			})
		})

		Describe("MakePathAll, Exists on empty folder", func() {
			folderPath := "/1/2/3/4"
			It("checks that MakePathAll creates a stub file to precreate empty folder", func() {
				Expect(s3fs.MakePathAll(ctx, folderPath)).To(Succeed())

				By("checking Exists on empty folder path without trailing '/'", func() {
					exists, err := s3fs.Exists(ctx, folderPath)
					Expect(err).NotTo(HaveOccurred())
					Expect(exists).To(BeFalse(), "should be recognized as a file")
				})
				By("checking Exists on empty folder path with trailing '/'", func() {
					exists, err := s3fs.Exists(ctx, folderPath+"/")
					Expect(err).NotTo(HaveOccurred())
					Expect(exists).To(BeFalse(), "empty dirs are not allowed")
				})
				By("checking Exists on file stub", func() {
					exists, err := s3fs.Exists(ctx, folderPath+"/"+filesystem.DirStubFileName)
					Expect(err).NotTo(HaveOccurred())
					Expect(exists).To(BeFalse(), "empty dirs not allowed: stub not created")
				})
			})

			It("checks that MakePathAll don't works: empty dirs emulation is disabled", func() {
				Expect(s3fs.MakePathAll(ctx, folderPath)).To(Succeed())
				Expect(s3fs.MakePathAll(ctx, folderPath)).To(Succeed())

				By("checking Exists on empty folder path with trailing '/'", func() {
					exists, err := s3fs.Exists(ctx, folderPath+"/")
					Expect(err).NotTo(HaveOccurred())
					Expect(exists).To(BeFalse(), "no empty dirs allowed")
				})
			})
		})

		Describe("Remove, should be applied to objects or empty folders", func() {
			It("checks removing existing object", func() {
				Expect(s3fs.Remove(ctx, key2)).To(Succeed())
				By("checking that removed object no more exists", func() {
					exists, err := s3fs.Exists(ctx, key2)
					Expect(err).NotTo(HaveOccurred())
					Expect(exists).To(BeFalse())
				})
			})

			It("checks removing not-existing object, should succeed also", func() {
				Expect(s3fs.Remove(ctx, noSuchKey)).To(Succeed())
				By("checking that removed object still not exists", func() {
					exists, err := s3fs.Exists(ctx, noSuchKey)
					Expect(err).NotTo(HaveOccurred())
					Expect(exists).To(BeFalse())
				})
			})

			Context("non-empty dir", func() {
				It("checks removing a non-empty directory path with '/', should not succeed", func() {
					Expect(s3fs.Remove(ctx, dir2)).NotTo(Succeed())
					By("checking folder path existence", func() {
						exists, err := s3fs.Exists(ctx, dir2)
						Expect(err).NotTo(HaveOccurred())
						Expect(exists).To(BeTrue())
					})
					By("checking object existence", func() {
						exists, err := s3fs.Exists(ctx, key2)
						Expect(err).NotTo(HaveOccurred())
						Expect(exists).To(BeTrue())
					})
				})

				It("checks removing a non-empty directory path with no '/', "+
					"it should be nothing, like non-existent file removal", func() {
					dir := strings.TrimSuffix(dir2, "/")
					Expect(s3fs.Remove(ctx, dir)).To(Succeed())
					By("checking folder path existence", func() {
						exists, err := s3fs.Exists(ctx, dir2) // as a directory path
						Expect(err).NotTo(HaveOccurred())
						Expect(exists).To(BeTrue())
					})
					By("checking object existence", func() {
						exists, err := s3fs.Exists(ctx, key2)
						Expect(err).NotTo(HaveOccurred())
						Expect(exists).To(BeTrue())
					})
				})
			})

			Context("empty dir", func() {
				JustBeforeEach(func() {
					By("removing all (2) objects in a directory", func() {
						Expect(s3fs.Remove(ctx, key1)).To(Succeed())
						Expect(s3fs.Remove(ctx, key2)).To(Succeed())
					})
				})

				It("checks removing an empty directory path, it should be removed", func() {
					Expect(s3fs.Remove(ctx, dir2)).To(Succeed())
					By("checking folder path existence", func() {
						exists, err := s3fs.Exists(ctx, dir2)
						Expect(err).NotTo(HaveOccurred())
						Expect(exists).To(BeFalse())
					})
				})

				It("checks removing an empty directory path with no '/', "+
					"it should be nothing, like non-existent file removal", func() {
					dir := strings.TrimSuffix(dir2, "/")
					Expect(s3fs.Remove(ctx, dir)).To(Succeed())
					By("checking folder path existence", func() {
						exists, err := s3fs.Exists(ctx, dir2) // as a directory path
						Expect(err).NotTo(HaveOccurred())
						Expect(exists).To(BeFalse())
					})
				})
			})

			It("checks removing a non-existent directory path with '/', should not succeed", func() {
				dir := s3fs.Dir("/4/5/6/7/")
				Expect(s3fs.Remove(ctx, dir)).To(Succeed())
				By("checking folder path existence", func() {
					exists, err := s3fs.Exists(ctx, dir)
					Expect(err).NotTo(HaveOccurred())
					Expect(exists).To(BeFalse())
				})
			})
		})

		Describe("RemoveAll, should be applied to folders but not objects", func() {
			It("checks removing existing object", func() {
				Expect(s3fs.RemoveAll(ctx, key2)).To(Succeed())
				By("checking that removed object no more exists", func() {
					exists, err := s3fs.Exists(ctx, key2)
					Expect(err).NotTo(HaveOccurred())
					Expect(exists).To(BeFalse())
				})
			})

			It("checks removing not-existing object, should succeed also", func() {
				Expect(s3fs.RemoveAll(ctx, noSuchKey)).To(Succeed())
				By("checking that removed object still not exists", func() {
					exists, err := s3fs.Exists(ctx, noSuchKey)
					Expect(err).NotTo(HaveOccurred())
					Expect(exists).To(BeFalse())
				})
			})

			Context("non-empty dir", func() {
				It("checks removing a non-empty directory path with '/', should succeed", func() {
					Expect(s3fs.RemoveAll(ctx, dir2)).To(Succeed())
					By("checking folder path existence", func() {
						exists, err := s3fs.Exists(ctx, dir2)
						Expect(err).NotTo(HaveOccurred())
						Expect(exists).To(BeFalse())
					})
					By("checking object existence", func() {
						exists, err := s3fs.Exists(ctx, key2)
						Expect(err).NotTo(HaveOccurred())
						Expect(exists).To(BeFalse())
					})
				})

				It("checks removing a non-empty directory path with no '/', "+
					"it should be nothing, like non-existent file removal", func() {
					dir := strings.TrimSuffix(dir2, "/")
					Expect(s3fs.Remove(ctx, dir)).To(Succeed())

					By("checking folder path existence", func() {
						exists, err := s3fs.Exists(ctx, dir2) // as a directory path
						Expect(err).NotTo(HaveOccurred())
						Expect(exists).To(BeTrue())
					})
					By("checking object existence", func() {
						exists, err := s3fs.Exists(ctx, key2)
						Expect(err).NotTo(HaveOccurred())
						Expect(exists).To(BeTrue())
					})
				})
			})
		})

		Describe("IsEmptyPath", func() {
			It("checks for existing non-empty path ends in '/'", func() {
				By("checking level 1", func() {
					isEmpty, err := s3fs.IsEmptyPath(ctx, "/a/b/")
					Expect(err).NotTo(HaveOccurred())
					Expect(isEmpty).To(BeFalse())
				})
				By("checking level 2", func() {
					isEmpty, err := s3fs.IsEmptyPath(ctx, "/a/")
					Expect(err).NotTo(HaveOccurred())
					Expect(isEmpty).To(BeFalse())
				})
				By("checking level 3", func() {
					isEmpty, err := s3fs.IsEmptyPath(ctx, "/a/b/c_d")
					Expect(err).NotTo(HaveOccurred())
					Expect(isEmpty).To(BeFalse())
				})
			})

			It("checks for existing non-empty path not ends in '/'", func() {
				isEmpty, err := s3fs.IsEmptyPath(ctx, "/a/b")
				Expect(err).NotTo(HaveOccurred())
				Expect(isEmpty).To(BeFalse())
			})

			It("checks for not-existing path, should be as empty", func() {
				isEmpty, err := s3fs.IsEmptyPath(ctx, "/1/2/3/4/")
				Expect(err).NotTo(HaveOccurred())
				Expect(isEmpty).To(BeTrue())
			})

			It("checks for existing path with stub", func() {
				const dir = "/1/2/3/4/"
				By("creating path and checking existence", func() {
					Expect(s3fs.MakePathAll(ctx, dir)).To(Succeed())
					exists, err := s3fs.Exists(ctx, dir+filesystem.DirStubFileName)
					Expect(err).NotTo(HaveOccurred())
					Expect(exists).To(BeFalse(), "no empty dirs allowed")
				})
				isEmpty, err := s3fs.IsEmptyPath(ctx, dir)
				Expect(err).NotTo(HaveOccurred())
				Expect(isEmpty).To(BeTrue())
			})
		})

		Describe("Stat", func() {
			It("checks for existing directory", func() {
				fi, err := s3fs.Stat(ctx, dir2)
				Expect(err).NotTo(HaveOccurred())
				Expect(fi.ModTime()).To(BeZero(), "not defined if empty directories are not emulated")
				Expect(fi.IsDir()).To(BeTrue())
				Expect(fi.FullName()).To(Equal(dir2))
				Expect(fi.Name()).To(Equal(path.Base(dir2)))
				Expect(fi.Size()).To(BeZero())
			})

			It("checks for not existing directory", func() {
				_, err := s3fs.Stat(ctx, "/4/5/6/7/")
				Expect(err).To(HaveOccurred())
			})
		})

		Describe("ReadDir", func() {
			It("checks reading existing non-empty dir with objects", func() {
				fi, err := s3fs.ReadDir(ctx, dir2)
				Expect(err).NotTo(HaveOccurred())
				Expect(fi).To(HaveLen(2))
				names := fi.FullNames()
				Expect(names).To(ConsistOf([]string{key1, key2}))
			})

			When("ListDirectoryEntries is false", func() {
				JustBeforeEach(func() {
					s3fs.(*filesystem.S3).SetListDirectoryEntries(false)
				})

				It("checks reading existing non-empty dir with subdirs and objects", func() {
					fi, err := s3fs.ReadDir(ctx, dir0)
					Expect(err).NotTo(HaveOccurred())
					Expect(fi).To(HaveLen(1))
					names := fi.FullNames()
					Expect(names).To(ConsistOf([]string{key3}))
				})

				It("checks reading existing empty dir", func() {
					By("removing everything inside dir0", func() {
						Expect(s3fs.Remove(ctx, key3)).To(Succeed())
						Expect(s3fs.RemoveAll(ctx, dir1)).To(Succeed())
					})

					fi, err := s3fs.ReadDir(ctx, dir0)
					Expect(err).NotTo(HaveOccurred())
					Expect(fi).To(BeEmpty())
					names := fi.FullNames()
					Expect(names).To(BeEmpty())
				})
			})

			When("ListDirectoryEntries is true", func() {
				JustBeforeEach(func() {
					s3fs.(*filesystem.S3).SetListDirectoryEntries(true)
				})

				It("checks reading existing non-empty dir with subdirs and objects", func() {
					fi, err := s3fs.ReadDir(ctx, dir0)
					Expect(err).NotTo(HaveOccurred())
					Expect(fi).To(HaveLen(2))
					names := fi.FullNames()
					Expect(names).To(ConsistOf([]string{dir1, key3}))
				})

				It("checks reading existing empty dir, should be empty, so no empty dirs allowed", func() {
					By("removing everything inside dir0", func() {
						Expect(s3fs.Remove(ctx, key3)).To(Succeed())
						Expect(s3fs.RemoveAll(ctx, dir1)).To(Succeed())
					})

					fi, err := s3fs.ReadDir(ctx, dir0)
					Expect(err).NotTo(HaveOccurred())
					Expect(fi).To(BeEmpty())
					names := fi.FullNames()
					Expect(names).To(BeEmpty())
				})
			})
		})

		Describe("WalkDir", func() {
			It("checks for root directory", func() {
				var entriesWalked []walkDirEntry
				Expect(s3fs.WalkDir(ctx, "/", func(name string, de filesystem.DirEntry, e error) error {
					if de != nil {
						entriesWalked = append(entriesWalked, walkDirEntry{name: de.FullName(), isDir: de.IsDir()})
					}
					return nil
				})).To(Succeed())
				Expect(entriesWalked).To(ConsistOf([]walkDirEntry{
					{name: "/", isDir: true},
					{name: "/a/", isDir: true},
					{name: "/a/3.txt", isDir: false},
					{name: "/a/b/", isDir: true},
					{name: "/a/b/c_d/", isDir: true},
					{name: "/a/b/c_d/1.txt", isDir: false},
					{name: "/a/b/c_d/2.txt", isDir: false},
				}))
			})

			It("checks for non-root directory", func() {
				var entriesWalked []walkDirEntry
				Expect(s3fs.WalkDir(ctx, dir2, func(name string, de filesystem.DirEntry, e error) error {
					if de != nil {
						entriesWalked = append(entriesWalked, walkDirEntry{name: de.FullName(), isDir: de.IsDir()})
					}
					return nil
				})).To(Succeed())
				Expect(entriesWalked).To(ConsistOf([]walkDirEntry{
					{name: "/a/b/c_d/", isDir: true},
					{name: "/a/b/c_d/1.txt", isDir: false},
					{name: "/a/b/c_d/2.txt", isDir: false},
				}))
			})

			It("checks for not-existing directory", func() {
				var entriesWalked []walkDirEntry
				Expect(s3fs.WalkDir(ctx, "/4/5/6/7/", func(name string, de filesystem.DirEntry, e error) error {
					if de != nil {
						entriesWalked = append(entriesWalked, walkDirEntry{name: de.FullName(), isDir: de.IsDir()})
					}
					return nil
				})).To(Equal(filesystem.ErrDirectoryNotExists))
				Expect(entriesWalked).To(BeEmpty())
			})
		})
	})
})
