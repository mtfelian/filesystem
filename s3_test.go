package filesystem_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/mtfelian/filesystem"
	"github.com/mtfelian/utils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/sirupsen/logrus"
)

var _ = Describe("S3 FileSystem implementation", func() {
	var (
		s3fs        filesystem.FileSystem
		fsLocal     filesystem.FileSystem
		err         error
		ctx         context.Context
		logger      logrus.FieldLogger
		minioClient *minio.Client
		s3Params    filesystem.S3Params
	)
	const (
		region         = ""
		bucketName     = "test-bucket" // no underscores here!
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
		s3fs, err = filesystem.NewS3(ctx, s3Params)
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

		It("checks ReadFile on existing objects", func() {
			for key, content := range keyToContent {
				actualContent, err := s3fs.ReadFile(ctx, key)
				Expect(err).NotTo(HaveOccurred())
				Expect(actualContent).To(Equal(content))
			}
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

					b, err := s3fs.ReadFile(ctx, key1)
					Expect(err).NotTo(HaveOccurred())
					Expect(b).To(BeEquivalentTo([]byte("123 456 1")), "should be partially overwritten")
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
					now := time.Now()
					f := func() {
						GinkgoRecover()
						defer wg.Done()
						// opening waits unlock by autoclosing...
						fw, err := s3fs.Open(ctx, key1)
						Expect(err).NotTo(HaveOccurred())
						Expect(fw).NotTo(BeNil())

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
					Expect(time.Now()).To(BeTemporally("~", now.Add(time.Duration(amount)*ttl), 2*ttl),
						"expected time passed should be: n*ttl")

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
				Expect(s3fs.RemoveFiles(ctx, []string{key2, key3, key1})).To(Succeed())
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
				Expect(s3fs.RemoveFiles(ctx, []string{key2, noSuchKey})).To(Succeed())
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

			Context("non-empty dir", func() {
				It("checks batch removing a non-empty directory path with '/', should not succeed, "+
					"nothing should be removed", func() {
					Expect(s3fs.RemoveFiles(ctx, []string{key3, dir2})).NotTo(Succeed())
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
					Expect(s3fs.RemoveFiles(ctx, []string{key3, dir})).To(Succeed())
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
				dir := path.Dir("/4/5/6/7/")
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
					for dirName := path.Dir(noSuchKey); dirName != "/"; dirName = path.Dir(dirName) {
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
				dir := path.Dir("/4/5/6/7/")
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
