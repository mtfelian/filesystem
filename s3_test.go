package filesystem_test

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"path"
	"path/filepath"
	"strings"
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
	})

	AfterEach(func() {
		exists, err := fsLocal.Exists(filesystem.TempDir)
		Expect(err).NotTo(HaveOccurred())
		if exists {
			Expect(fsLocal.RemoveAll(filesystem.TempDir)).To(Succeed())
		}

		Expect(minioClient.RemoveBucketWithOptions(ctx, bucketName, minio.RemoveBucketOptions{
			ForceDelete: true,
		})).To(Succeed())
	})

	const (
		dir0      = "/a/"
		dir1      = dir0 + "b/"
		dir2      = dir1 + "c_d/"
		key1      = dir2 + "1.txt"
		key2      = dir2 + "2.txt"
		key3      = dir0 + "3.txt"
		noSuchKey = "/b/c/d/nofile.txt"
		content1  = "content 1"
		content2  = "content 2"
		content3  = "content 3"
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
				Expect(s3.WriteFile(key, content)).To(Succeed())
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

		inTempDir := func(s string) string {
			return filepath.Join(filesystem.TempDir, strings.ReplaceAll(s, "/", string(filepath.Separator)))
		}

		It("checks ReadFile on existing objects", func() {
			for key, content := range keyToContent {
				actualContent, err := s3fs.ReadFile(key)
				Expect(err).NotTo(HaveOccurred())
				Expect(actualContent).To(Equal(content))
			}
		})

		Describe("Open, Close and opened files cleanup", func() {
			var (
				openedFilesList *filesystem.S3OpenedFilesList
				f               filesystem.File
				closed          bool
			)
			JustBeforeEach(func() {
				f, err = s3fs.Open(key1)
				Expect(err).NotTo(HaveOccurred())
				Expect(f).NotTo(BeNil())
				closed = false

				openedFilesList = s3fs.(*filesystem.S3).OpenedFilesList()
				Expect(openedFilesList).NotTo(BeNil())
				Expect(openedFilesList.Map()).To(HaveLen(1))
			})

			JustAfterEach(func() {
				if !closed {
					Expect(f.Close()).To(Succeed())
				}
			})

			lookUpForSingleEntry := func() filesystem.S3OpenedFilesListEntry {
				s3fs.(*filesystem.S3).OpenedFilesListLock()
				defer s3fs.(*filesystem.S3).OpenedFilesListUnlock()
				for key, value := range openedFilesList.Map() {
					exists, err := fsLocal.Exists(key)
					ExpectWithOffset(1, err).NotTo(HaveOccurred())
					ExpectWithOffset(1, exists).To(BeTrue())
					return value
				}
				Fail("openedFilesList is empty")
				return filesystem.S3OpenedFilesListEntry{}
			}

			isExists := func(name string) bool {
				exists, err := fsLocal.Exists(name)
				ExpectWithOffset(1, err).NotTo(HaveOccurred())
				return exists
			}

			It("checks Close", func() {
				s3FileEntry := lookUpForSingleEntry()
				Expect(s3FileEntry.Added).To(BeTemporally("~", time.Now(), 2*time.Second))

				name := s3FileEntry.S3File.Name()
				Expect(name).To(Equal(inTempDir(key1)))
				Expect(s3FileEntry.S3File.Close()).To(Succeed())

				By("checking that file was removed at Close call", func() {
					Expect(isExists(name)).To(BeFalse())
				})
			})

			It("checks autoclosing", func() {
				s3FileEntry := lookUpForSingleEntry()
				Eventually(func() bool {
					return isExists(s3FileEntry.S3File.Name())
				}, 3*ttl, ttl/2).Should(BeFalse()) // to give slightly more time to the cleaner goroutine

				closed = true
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

				b := make([]byte, size)
				n, err := f.Read(b)
				Expect(err).NotTo(HaveOccurred())
				Expect(n).To(BeEquivalentTo(size))
				Expect(b).To(BeEquivalentTo([]byte(content1)))
			})

			It("checks that can't write to opened file", func() {
				n, err := f.Write([]byte("123"))
				Expect(err).To(HaveOccurred())
				Expect(n).To(BeZero())
			})
		})

		Describe("Reader", func() {
			It("checks streamed reading", func() {
				r, err := s3fs.Reader(key2)
				Expect(err).NotTo(HaveOccurred())

				b, err := io.ReadAll(r)
				Expect(err).NotTo(HaveOccurred())
				Expect(b).To(BeEquivalentTo(content2))
			})
		})

		Describe("Exists", func() {
			It("checks that Exists returns true for existing object", func() {
				exists, err := s3fs.Exists(key2)
				Expect(err).NotTo(HaveOccurred())
				Expect(exists).To(BeTrue())
			})

			It("checks that Exists returns false for not existing object", func() {
				exists, err := s3fs.Exists(noSuchKey)
				Expect(err).NotTo(HaveOccurred())
				Expect(exists).To(BeFalse())
			})

			It("checks that Exists returns non-nil error for object with invalid name", func() {
				_, err := s3fs.Exists(strings.Repeat("1", 1025)) // name too long
				Expect(err).To(HaveOccurred())
			})
		})

		Describe("MakePathAll, Exists on empty folder", func() {
			folderPath := "/1/2/3/4"
			It("checks that MakePathAll creates a stub file to precreate empty folder", func() {
				Expect(s3fs.MakePathAll(folderPath)).To(Succeed())

				By("checking Exists on empty folder path without trailing '/'", func() {
					exists, err := s3fs.Exists(folderPath)
					Expect(err).NotTo(HaveOccurred())
					Expect(exists).To(BeFalse(), "should be recognized as a file")
				})
				By("checking Exists on empty folder path with trailing '/'", func() {
					exists, err := s3fs.Exists(folderPath + "/")
					Expect(err).NotTo(HaveOccurred())
					Expect(exists).To(BeTrue(), "should be recognized as a directory")
				})
				By("checking Exists on file stub", func() {
					exists, err := s3fs.Exists(folderPath + "/" + filesystem.DirStubFileName)
					Expect(err).NotTo(HaveOccurred())
					Expect(exists).To(BeTrue(), "should be recognized as a directory stub file")
				})
			})

			It("checks that MakePathAll correctly works on existing path", func() {
				Expect(s3fs.MakePathAll(folderPath)).To(Succeed())
				Expect(s3fs.MakePathAll(folderPath)).To(Succeed())

				By("checking Exists on empty folder path with trailing '/'", func() {
					exists, err := s3fs.Exists(folderPath + "/")
					Expect(err).NotTo(HaveOccurred())
					Expect(exists).To(BeTrue(), "should be recognized as a directory")
				})
			})
		})

		Describe("Remove, should be applied to objects or empty folders", func() {
			It("checks removing existing object", func() {
				Expect(s3fs.Remove(key2)).To(Succeed())
				By("checking that removed object no more exists", func() {
					exists, err := s3fs.Exists(key2)
					Expect(err).NotTo(HaveOccurred())
					Expect(exists).To(BeFalse())
				})
			})

			It("checks removing not-existing object, should succeed also", func() {
				Expect(s3fs.Remove(noSuchKey)).To(Succeed())
				By("checking that removed object still not exists", func() {
					exists, err := s3fs.Exists(noSuchKey)
					Expect(err).NotTo(HaveOccurred())
					Expect(exists).To(BeFalse())
				})
			})

			Context("non-empty dir", func() {
				It("checks removing a non-empty directory path with '/', should not succeed", func() {
					Expect(s3fs.Remove(dir2)).NotTo(Succeed())
					By("checking folder path existence", func() {
						exists, err := s3fs.Exists(dir2)
						Expect(err).NotTo(HaveOccurred())
						Expect(exists).To(BeTrue())
					})
					By("checking object existence", func() {
						exists, err := s3fs.Exists(key2)
						Expect(err).NotTo(HaveOccurred())
						Expect(exists).To(BeTrue())
					})
				})

				It("checks removing a non-empty directory path with no '/', "+
					"it should be nothing, like non-existent file removal", func() {
					dir := strings.TrimSuffix(dir2, "/")
					Expect(s3fs.Remove(dir)).To(Succeed())
					By("checking folder path existence", func() {
						exists, err := s3fs.Exists(dir2) // as a directory path
						Expect(err).NotTo(HaveOccurred())
						Expect(exists).To(BeTrue())
					})
					By("checking object existence", func() {
						exists, err := s3fs.Exists(key2)
						Expect(err).NotTo(HaveOccurred())
						Expect(exists).To(BeTrue())
					})
				})
			})

			Context("empty dir", func() {
				JustBeforeEach(func() {
					By("removing all (2) objects in a directory", func() {
						Expect(s3fs.Remove(key1)).To(Succeed())
						Expect(s3fs.Remove(key2)).To(Succeed())
					})
				})

				It("checks removing an empty directory path, it should be removed", func() {
					Expect(s3fs.Remove(dir2)).To(Succeed())
					By("checking folder path existence", func() {
						exists, err := s3fs.Exists(dir2)
						Expect(err).NotTo(HaveOccurred())
						Expect(exists).To(BeFalse())
					})
				})

				It("checks removing an empty directory path with no '/', "+
					"it should be nothing, like non-existent file removal", func() {
					dir := strings.TrimSuffix(dir2, "/")
					Expect(s3fs.Remove(dir)).To(Succeed())
					By("checking folder path existence", func() {
						exists, err := s3fs.Exists(dir2) // as a directory path
						Expect(err).NotTo(HaveOccurred())
						Expect(exists).To(BeTrue())
					})
				})
			})

			It("checks removing a non-existent directory path with '/', should not succeed", func() {
				dir := path.Dir("/4/5/6/7/")
				Expect(s3fs.Remove(dir)).To(Succeed())
				By("checking folder path existence", func() {
					exists, err := s3fs.Exists(dir)
					Expect(err).NotTo(HaveOccurred())
					Expect(exists).To(BeFalse())
				})
			})
		})

		Describe("RemoveAll, should be applied to folders but not objects", func() {
			It("checks removing existing object", func() {
				Expect(s3fs.RemoveAll(key2)).To(Succeed())
				By("checking that removed object no more exists", func() {
					exists, err := s3fs.Exists(key2)
					Expect(err).NotTo(HaveOccurred())
					Expect(exists).To(BeFalse())
				})
			})

			It("checks removing not-existing object, should succeed also", func() {
				Expect(s3fs.RemoveAll(noSuchKey)).To(Succeed())
				By("checking that removed object still not exists", func() {
					exists, err := s3fs.Exists(noSuchKey)
					Expect(err).NotTo(HaveOccurred())
					Expect(exists).To(BeFalse())
				})
			})

			Context("non-empty dir", func() {
				It("checks removing a non-empty directory path with '/', should succeed", func() {
					Expect(s3fs.RemoveAll(dir2)).To(Succeed())
					By("checking folder path existence", func() {
						exists, err := s3fs.Exists(dir2)
						Expect(err).NotTo(HaveOccurred())
						Expect(exists).To(BeFalse())
					})
					By("checking object existence", func() {
						exists, err := s3fs.Exists(key2)
						Expect(err).NotTo(HaveOccurred())
						Expect(exists).To(BeFalse())
					})
				})

				It("checks removing a non-empty directory path with no '/', "+
					"it should be nothing, like non-existent file removal", func() {
					dir := strings.TrimSuffix(dir2, "/")
					Expect(s3fs.Remove(dir)).To(Succeed())

					By("checking folder path existence", func() {
						exists, err := s3fs.Exists(dir2) // as a directory path
						Expect(err).NotTo(HaveOccurred())
						Expect(exists).To(BeTrue())
					})
					By("checking object existence", func() {
						exists, err := s3fs.Exists(key2)
						Expect(err).NotTo(HaveOccurred())
						Expect(exists).To(BeTrue())
					})
				})
			})
		})

		Describe("IsEmptyPath", func() {
			It("checks for existing non-empty path ends in '/'", func() {
				By("checking level 1", func() {
					isEmpty, err := s3fs.IsEmptyPath("/a/b/")
					Expect(err).NotTo(HaveOccurred())
					Expect(isEmpty).To(BeFalse())
				})
				By("checking level 2", func() {
					isEmpty, err := s3fs.IsEmptyPath("/a/")
					Expect(err).NotTo(HaveOccurred())
					Expect(isEmpty).To(BeFalse())
				})
				By("checking level 3", func() {
					isEmpty, err := s3fs.IsEmptyPath("/a/b/c_d")
					Expect(err).NotTo(HaveOccurred())
					Expect(isEmpty).To(BeFalse())
				})
			})

			It("checks for existing non-empty path not ends in '/'", func() {
				isEmpty, err := s3fs.IsEmptyPath("/a/b")
				Expect(err).NotTo(HaveOccurred())
				Expect(isEmpty).To(BeFalse())
			})

			It("checks for not-existing path, should be as empty", func() {
				isEmpty, err := s3fs.IsEmptyPath("/1/2/3/4/")
				Expect(err).NotTo(HaveOccurred())
				Expect(isEmpty).To(BeTrue())
			})

			It("checks for existing path with stub", func() {
				const dir = "/1/2/3/4/"
				By("creating path and checking existence", func() {
					Expect(s3fs.MakePathAll(dir)).To(Succeed())
					exists, err := s3fs.Exists(dir + filesystem.DirStubFileName)
					Expect(err).NotTo(HaveOccurred())
					Expect(exists).To(BeTrue())
				})
				isEmpty, err := s3fs.IsEmptyPath(dir)
				Expect(err).NotTo(HaveOccurred())
				Expect(isEmpty).To(BeTrue())
			})
		})

		Describe("Rename", func() {
			It("checks renaming object to new name which does not exists", func() {
				Expect(s3fs.Rename(key1, noSuchKey)).To(Succeed())
				b, err := s3fs.ReadFile(noSuchKey)
				Expect(err).NotTo(HaveOccurred())
				Expect(b).To(BeEquivalentTo(content1))

				By("check that every subdir have stub file", func() {
					for dirName := path.Dir(noSuchKey); dirName != "/"; dirName = path.Dir(dirName) {
						exists, err := s3fs.Exists(dirName + "/" + filesystem.DirStubFileName)
						Expect(err).NotTo(HaveOccurred(), "checking stub existence err, dirname=%q", dirName)
						Expect(exists).To(BeTrue(), "checking stub existence, dirname=%q", dirName)
					}
				})
			})

			It("checks renaming object to new name which already exists, should be replaced", func() {
				Expect(s3fs.Rename(key1, key3)).To(Succeed())
				b, err := s3fs.ReadFile(key3)
				Expect(err).NotTo(HaveOccurred())
				Expect(b).To(BeEquivalentTo(content1))
			})

			It("checks renaming object to itself, do nothing", func() {
				Expect(s3fs.Rename(key1, key1)).To(Succeed())
				b, err := s3fs.ReadFile(key1)
				Expect(err).NotTo(HaveOccurred())
				Expect(b).To(BeEquivalentTo(content1))
			})

			It("checks renaming not existing object, should fail", func() {
				Expect(s3fs.Rename(noSuchKey, key1)).NotTo(Succeed())
				b, err := s3fs.ReadFile(key1)
				Expect(err).NotTo(HaveOccurred())
				Expect(b).To(BeEquivalentTo(content1))
			})

			Context("renaming directory", func() {
				const (
					existingDir    = "/a/"
					notExistingDir = "/d/"
				)
				It("checks renaming existing directory into object, should fail", func() {
					Expect(s3fs.Rename(existingDir, key1)).To(Equal(filesystem.ErrDestinationPathIsNotDirectory))
				})

				It("checks renaming existing directory into another directory", func() {
					Expect(s3fs.Rename(existingDir, notExistingDir)).To(Succeed())

					By("checking presence of new directory", func() {
						exists, err := s3fs.Exists(notExistingDir)
						Expect(err).NotTo(HaveOccurred())
						Expect(exists).To(BeTrue())
					})
					By("checking absence of moved directory", func() {
						exists, err := s3fs.Exists(existingDir)
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
								exists, err := s3fs.Exists(key)
								Expect(err).NotTo(HaveOccurred())
								Expect(exists).To(BeTrue(), "object %q should exist", key)
							})

							By("checking their content", func() {
								b, err := s3fs.ReadFile(key)
								Expect(err).NotTo(HaveOccurred())
								Expect(b).To(BeEquivalentTo(content), "object %q should be equivalent", key)
							})
						}

						By("checking absence of moved objects", func() {
							for key := range keyToContent {
								exists, err := s3fs.Exists(key)
								Expect(err).NotTo(HaveOccurred())
								Expect(exists).To(BeFalse(), "object %q should not exist", key)
							}
						})
					})
				})

				It("checks renaming not existing directory into existing directory", func() {
					Expect(s3fs.Rename(notExistingDir, existingDir)).NotTo(Succeed())
					By("checking presence of target directory objects", func() {
						for key := range keyToContent {
							exists, err := s3fs.Exists(key)
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
						Expect(s3fs.Rename(stub, target)).To(Equal(filesystem.ErrCantUseRenameWithStubObject))
						By("checking that stub exists", func() {
							exists, err := s3fs.Exists(stub)
							Expect(err).NotTo(HaveOccurred())
							Expect(exists).To(BeTrue())
						})
						By("checking that target does not exists", func() {
							exists, err := s3fs.Exists(target)
							Expect(err).NotTo(HaveOccurred())
							Expect(exists).To(BeFalse())
						})
					})

					It("checks renaming to stub object, should fail", func() {
						By("creating target object", func() {
							Expect(s3fs.WriteFile(target, []byte("123"))).To(Succeed())
						})
						Expect(s3fs.Rename(target, stub)).To(Equal(filesystem.ErrCantUseRenameWithStubObject))
						By("checking that stub exists and contains stub content", func() {
							exists, err := s3fs.Exists(stub)
							Expect(err).NotTo(HaveOccurred())
							Expect(exists).To(BeTrue())

							b, err := s3fs.ReadFile(stub)
							Expect(err).NotTo(HaveOccurred())
							Expect(b).To(BeEquivalentTo([]byte(filesystem.DirStubFileContent)))
						})
						By("checking that target still exists", func() {
							exists, err := s3fs.Exists(target)
							Expect(err).NotTo(HaveOccurred())
							Expect(exists).To(BeTrue())
						})
					})
				})
			})
		})

		Describe("Stat", func() {
			It("checks for an existing object", func() {
				fi, err := s3fs.Stat(key1)
				Expect(err).NotTo(HaveOccurred())
				Expect(fi.ModTime()).To(BeTemporally("~", time.Now(), 2*time.Second))
				Expect(fi.IsDir()).To(BeFalse())
				Expect(fi.Name()).To(Equal(key1))
				Expect(fi.Size()).To(BeEquivalentTo(len(content1)))
			})

			It("checks for not existing object", func() {
				_, err := s3fs.Stat(noSuchKey)
				Expect(err).To(HaveOccurred())
			})

			It("checks for existing directory", func() {
				fi, err := s3fs.Stat(dir2)
				Expect(err).NotTo(HaveOccurred())
				Expect(fi.ModTime()).To(BeTemporally("~", time.Now(), 2*time.Second))
				Expect(fi.IsDir()).To(BeTrue())
				Expect(fi.Name()).To(Equal(dir2))
				Expect(fi.Size()).To(BeZero())
			})

			It("checks for not existing directory", func() {
				_, err := s3fs.Stat("/4/5/6/7/")
				Expect(err).To(HaveOccurred())
			})
		})

		Describe("ReadDir", func() {
			It("checks if object is not a dir", func() {
				_, err := s3fs.ReadDir(key1)
				Expect(err).To(Equal(filesystem.ErrNotADirectory))
			})

			It("checks reading existing non-empty dir with objects", func() {
				fi, err := s3fs.ReadDir(dir2)
				Expect(err).NotTo(HaveOccurred())
				Expect(fi).To(HaveLen(2))
				names := fi.Names()
				Expect(names).To(ConsistOf([]string{key1, key2}))
			})

			When("ListDirectoryEntries is false", func() {
				JustBeforeEach(func() {
					s3fs.(*filesystem.S3).SetListDirectoryEntries(false)
				})

				It("checks reading existing non-empty dir with subdirs and objects", func() {
					fi, err := s3fs.ReadDir(dir0)
					Expect(err).NotTo(HaveOccurred())
					Expect(fi).To(HaveLen(1))
					names := fi.Names()
					Expect(names).To(ConsistOf([]string{key3}))
				})

				It("checks reading existing empty dir", func() {
					By("removing everything inside dir0", func() {
						Expect(s3fs.Remove(key3)).To(Succeed())
						Expect(s3fs.RemoveAll(dir1)).To(Succeed())
					})

					fi, err := s3fs.ReadDir(dir0)
					Expect(err).NotTo(HaveOccurred())
					Expect(fi).To(BeEmpty())
					names := fi.Names()
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
							Expect(s3fs.WriteFile(nameFunc(i), contentFunc(i))).To(Succeed())
						}
					})

					By("reading directory", func() {
						logger.Info("reading large S3 directory...")
						fsi, err := s3fs.ReadDir(largeDir)
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
					fi, err := s3fs.ReadDir(dir0)
					Expect(err).NotTo(HaveOccurred())
					Expect(fi).To(HaveLen(2))
					names := fi.Names()
					Expect(names).To(ConsistOf([]string{dir1, key3}))
				})

				It("checks reading existing empty dir", func() {
					By("removing everything inside dir0", func() {
						Expect(s3fs.Remove(key3)).To(Succeed())
						Expect(s3fs.RemoveAll(dir1)).To(Succeed())
					})

					fi, err := s3fs.ReadDir(dir0)
					Expect(err).NotTo(HaveOccurred())
					Expect(fi).To(BeEmpty())
					names := fi.Names()
					Expect(names).To(BeEmpty())
				})
			})
		})

		Describe("WalkDir", func() {
			It("checks for root directory", func() {
				var entriesWalked []walkDirEntry
				Expect(s3fs.WalkDir("/", func(name string, de filesystem.DirEntry, e error) error {
					if de != nil {
						entriesWalked = append(entriesWalked, walkDirEntry{name: de.Name(), isDir: de.IsDir()})
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
				Expect(s3fs.WalkDir(dir2, func(name string, de filesystem.DirEntry, e error) error {
					if de != nil {
						entriesWalked = append(entriesWalked, walkDirEntry{name: de.Name(), isDir: de.IsDir()})
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
				Expect(s3fs.WalkDir(key2, func(name string, de filesystem.DirEntry, e error) error {
					if de != nil {
						entriesWalked = append(entriesWalked, walkDirEntry{name: de.Name(), isDir: de.IsDir()})
					}
					return nil
				})).To(Succeed())
				Expect(entriesWalked).To(ConsistOf([]walkDirEntry{
					{name: "/a/b/c_d/2.txt", isDir: false},
				}))
			})

			It("checks for not-existing directory", func() {
				var entriesWalked []walkDirEntry
				Expect(s3fs.IsNotExist(s3fs.WalkDir("/4/5/6/7/", func(name string, de filesystem.DirEntry, e error) error {
					if de != nil {
						entriesWalked = append(entriesWalked, walkDirEntry{name: de.Name(), isDir: de.IsDir()})
					}
					return nil
				}))).To(BeTrue())
				Expect(entriesWalked).To(BeEmpty())
			})

			It("checks for not-existing object", func() {
				var entriesWalked []walkDirEntry
				Expect(s3fs.IsNotExist(s3fs.WalkDir("/4/5/6/7", func(name string, de filesystem.DirEntry, e error) error {
					if de != nil {
						entriesWalked = append(entriesWalked, walkDirEntry{name: de.Name(), isDir: de.IsDir()})
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
				exists, err := s3fs.Exists(key2)
				Expect(err).NotTo(HaveOccurred())
				Expect(exists).To(BeTrue())
			})

			It("checks that Exists returns false for not existing object", func() {
				exists, err := s3fs.Exists(noSuchKey)
				Expect(err).NotTo(HaveOccurred())
				Expect(exists).To(BeFalse())
			})

			It("checks that Exists returns non-nil error for object with invalid name", func() {
				_, err := s3fs.Exists(strings.Repeat("1", 1025)) // name too long
				Expect(err).To(HaveOccurred())
			})
		})

		Describe("MakePathAll, Exists on empty folder", func() {
			folderPath := "/1/2/3/4"
			It("checks that MakePathAll creates a stub file to precreate empty folder", func() {
				Expect(s3fs.MakePathAll(folderPath)).To(Succeed())

				By("checking Exists on empty folder path without trailing '/'", func() {
					exists, err := s3fs.Exists(folderPath)
					Expect(err).NotTo(HaveOccurred())
					Expect(exists).To(BeFalse(), "should be recognized as a file")
				})
				By("checking Exists on empty folder path with trailing '/'", func() {
					exists, err := s3fs.Exists(folderPath + "/")
					Expect(err).NotTo(HaveOccurred())
					Expect(exists).To(BeFalse(), "empty dirs are not allowed")
				})
				By("checking Exists on file stub", func() {
					exists, err := s3fs.Exists(folderPath + "/" + filesystem.DirStubFileName)
					Expect(err).NotTo(HaveOccurred())
					Expect(exists).To(BeFalse(), "empty dirs not allowed: stub not created")
				})
			})

			It("checks that MakePathAll don't works: empty dirs emulation is disabled", func() {
				Expect(s3fs.MakePathAll(folderPath)).To(Succeed())
				Expect(s3fs.MakePathAll(folderPath)).To(Succeed())

				By("checking Exists on empty folder path with trailing '/'", func() {
					exists, err := s3fs.Exists(folderPath + "/")
					Expect(err).NotTo(HaveOccurred())
					Expect(exists).To(BeFalse(), "no empty dirs allowed")
				})
			})
		})

		Describe("Remove, should be applied to objects or empty folders", func() {
			It("checks removing existing object", func() {
				Expect(s3fs.Remove(key2)).To(Succeed())
				By("checking that removed object no more exists", func() {
					exists, err := s3fs.Exists(key2)
					Expect(err).NotTo(HaveOccurred())
					Expect(exists).To(BeFalse())
				})
			})

			It("checks removing not-existing object, should succeed also", func() {
				Expect(s3fs.Remove(noSuchKey)).To(Succeed())
				By("checking that removed object still not exists", func() {
					exists, err := s3fs.Exists(noSuchKey)
					Expect(err).NotTo(HaveOccurred())
					Expect(exists).To(BeFalse())
				})
			})

			Context("non-empty dir", func() {
				It("checks removing a non-empty directory path with '/', should not succeed", func() {
					Expect(s3fs.Remove(dir2)).NotTo(Succeed())
					By("checking folder path existence", func() {
						exists, err := s3fs.Exists(dir2)
						Expect(err).NotTo(HaveOccurred())
						Expect(exists).To(BeTrue())
					})
					By("checking object existence", func() {
						exists, err := s3fs.Exists(key2)
						Expect(err).NotTo(HaveOccurred())
						Expect(exists).To(BeTrue())
					})
				})

				It("checks removing a non-empty directory path with no '/', "+
					"it should be nothing, like non-existent file removal", func() {
					dir := strings.TrimSuffix(dir2, "/")
					Expect(s3fs.Remove(dir)).To(Succeed())
					By("checking folder path existence", func() {
						exists, err := s3fs.Exists(dir2) // as a directory path
						Expect(err).NotTo(HaveOccurred())
						Expect(exists).To(BeTrue())
					})
					By("checking object existence", func() {
						exists, err := s3fs.Exists(key2)
						Expect(err).NotTo(HaveOccurred())
						Expect(exists).To(BeTrue())
					})
				})
			})

			Context("empty dir", func() {
				JustBeforeEach(func() {
					By("removing all (2) objects in a directory", func() {
						Expect(s3fs.Remove(key1)).To(Succeed())
						Expect(s3fs.Remove(key2)).To(Succeed())
					})
				})

				It("checks removing an empty directory path, it should be removed", func() {
					Expect(s3fs.Remove(dir2)).To(Succeed())
					By("checking folder path existence", func() {
						exists, err := s3fs.Exists(dir2)
						Expect(err).NotTo(HaveOccurred())
						Expect(exists).To(BeFalse())
					})
				})

				It("checks removing an empty directory path with no '/', "+
					"it should be nothing, like non-existent file removal", func() {
					dir := strings.TrimSuffix(dir2, "/")
					Expect(s3fs.Remove(dir)).To(Succeed())
					By("checking folder path existence", func() {
						exists, err := s3fs.Exists(dir2) // as a directory path
						Expect(err).NotTo(HaveOccurred())
						Expect(exists).To(BeFalse())
					})
				})
			})

			It("checks removing a non-existent directory path with '/', should not succeed", func() {
				dir := path.Dir("/4/5/6/7/")
				Expect(s3fs.Remove(dir)).To(Succeed())
				By("checking folder path existence", func() {
					exists, err := s3fs.Exists(dir)
					Expect(err).NotTo(HaveOccurred())
					Expect(exists).To(BeFalse())
				})
			})
		})

		Describe("RemoveAll, should be applied to folders but not objects", func() {
			It("checks removing existing object", func() {
				Expect(s3fs.RemoveAll(key2)).To(Succeed())
				By("checking that removed object no more exists", func() {
					exists, err := s3fs.Exists(key2)
					Expect(err).NotTo(HaveOccurred())
					Expect(exists).To(BeFalse())
				})
			})

			It("checks removing not-existing object, should succeed also", func() {
				Expect(s3fs.RemoveAll(noSuchKey)).To(Succeed())
				By("checking that removed object still not exists", func() {
					exists, err := s3fs.Exists(noSuchKey)
					Expect(err).NotTo(HaveOccurred())
					Expect(exists).To(BeFalse())
				})
			})

			Context("non-empty dir", func() {
				It("checks removing a non-empty directory path with '/', should succeed", func() {
					Expect(s3fs.RemoveAll(dir2)).To(Succeed())
					By("checking folder path existence", func() {
						exists, err := s3fs.Exists(dir2)
						Expect(err).NotTo(HaveOccurred())
						Expect(exists).To(BeFalse())
					})
					By("checking object existence", func() {
						exists, err := s3fs.Exists(key2)
						Expect(err).NotTo(HaveOccurred())
						Expect(exists).To(BeFalse())
					})
				})

				It("checks removing a non-empty directory path with no '/', "+
					"it should be nothing, like non-existent file removal", func() {
					dir := strings.TrimSuffix(dir2, "/")
					Expect(s3fs.Remove(dir)).To(Succeed())

					By("checking folder path existence", func() {
						exists, err := s3fs.Exists(dir2) // as a directory path
						Expect(err).NotTo(HaveOccurred())
						Expect(exists).To(BeTrue())
					})
					By("checking object existence", func() {
						exists, err := s3fs.Exists(key2)
						Expect(err).NotTo(HaveOccurred())
						Expect(exists).To(BeTrue())
					})
				})
			})
		})

		Describe("IsEmptyPath", func() {
			It("checks for existing non-empty path ends in '/'", func() {
				By("checking level 1", func() {
					isEmpty, err := s3fs.IsEmptyPath("/a/b/")
					Expect(err).NotTo(HaveOccurred())
					Expect(isEmpty).To(BeFalse())
				})
				By("checking level 2", func() {
					isEmpty, err := s3fs.IsEmptyPath("/a/")
					Expect(err).NotTo(HaveOccurred())
					Expect(isEmpty).To(BeFalse())
				})
				By("checking level 3", func() {
					isEmpty, err := s3fs.IsEmptyPath("/a/b/c_d")
					Expect(err).NotTo(HaveOccurred())
					Expect(isEmpty).To(BeFalse())
				})
			})

			It("checks for existing non-empty path not ends in '/'", func() {
				isEmpty, err := s3fs.IsEmptyPath("/a/b")
				Expect(err).NotTo(HaveOccurred())
				Expect(isEmpty).To(BeFalse())
			})

			It("checks for not-existing path, should be as empty", func() {
				isEmpty, err := s3fs.IsEmptyPath("/1/2/3/4/")
				Expect(err).NotTo(HaveOccurred())
				Expect(isEmpty).To(BeTrue())
			})

			It("checks for existing path with stub", func() {
				const dir = "/1/2/3/4/"
				By("creating path and checking existence", func() {
					Expect(s3fs.MakePathAll(dir)).To(Succeed())
					exists, err := s3fs.Exists(dir + filesystem.DirStubFileName)
					Expect(err).NotTo(HaveOccurred())
					Expect(exists).To(BeFalse(), "no empty dirs allowed")
				})
				isEmpty, err := s3fs.IsEmptyPath(dir)
				Expect(err).NotTo(HaveOccurred())
				Expect(isEmpty).To(BeTrue())
			})
		})

		Describe("Stat", func() {
			It("checks for existing directory", func() {
				fi, err := s3fs.Stat(dir2)
				Expect(err).NotTo(HaveOccurred())
				Expect(fi.ModTime()).To(BeZero(), "not defined if empty directories are not emulated")
				Expect(fi.IsDir()).To(BeTrue())
				Expect(fi.Name()).To(Equal(dir2))
				Expect(fi.Size()).To(BeZero())
			})

			It("checks for not existing directory", func() {
				_, err := s3fs.Stat("/4/5/6/7/")
				Expect(err).To(HaveOccurred())
			})
		})

		Describe("ReadDir", func() {
			It("checks reading existing non-empty dir with objects", func() {
				fi, err := s3fs.ReadDir(dir2)
				Expect(err).NotTo(HaveOccurred())
				Expect(fi).To(HaveLen(2))
				names := fi.Names()
				Expect(names).To(ConsistOf([]string{key1, key2}))
			})

			When("ListDirectoryEntries is false", func() {
				JustBeforeEach(func() {
					s3fs.(*filesystem.S3).SetListDirectoryEntries(false)
				})

				It("checks reading existing non-empty dir with subdirs and objects", func() {
					fi, err := s3fs.ReadDir(dir0)
					Expect(err).NotTo(HaveOccurred())
					Expect(fi).To(HaveLen(1))
					names := fi.Names()
					Expect(names).To(ConsistOf([]string{key3}))
				})

				It("checks reading existing empty dir", func() {
					By("removing everything inside dir0", func() {
						Expect(s3fs.Remove(key3)).To(Succeed())
						Expect(s3fs.RemoveAll(dir1)).To(Succeed())
					})

					fi, err := s3fs.ReadDir(dir0)
					Expect(err).NotTo(HaveOccurred())
					Expect(fi).To(BeEmpty())
					names := fi.Names()
					Expect(names).To(BeEmpty())
				})
			})

			When("ListDirectoryEntries is true", func() {
				JustBeforeEach(func() {
					s3fs.(*filesystem.S3).SetListDirectoryEntries(true)
				})

				It("checks reading existing non-empty dir with subdirs and objects", func() {
					fi, err := s3fs.ReadDir(dir0)
					Expect(err).NotTo(HaveOccurred())
					Expect(fi).To(HaveLen(2))
					names := fi.Names()
					Expect(names).To(ConsistOf([]string{dir1, key3}))
				})

				It("checks reading existing empty dir, should be empty, so no empty dirs allowed", func() {
					By("removing everything inside dir0", func() {
						Expect(s3fs.Remove(key3)).To(Succeed())
						Expect(s3fs.RemoveAll(dir1)).To(Succeed())
					})

					fi, err := s3fs.ReadDir(dir0)
					Expect(err).NotTo(HaveOccurred())
					Expect(fi).To(BeEmpty())
					names := fi.Names()
					Expect(names).To(BeEmpty())
				})
			})
		})

		Describe("WalkDir", func() {
			It("checks for root directory", func() {
				var entriesWalked []walkDirEntry
				Expect(s3fs.WalkDir("/", func(name string, de filesystem.DirEntry, e error) error {
					if de != nil {
						entriesWalked = append(entriesWalked, walkDirEntry{name: de.Name(), isDir: de.IsDir()})
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
				Expect(s3fs.WalkDir(dir2, func(name string, de filesystem.DirEntry, e error) error {
					if de != nil {
						entriesWalked = append(entriesWalked, walkDirEntry{name: de.Name(), isDir: de.IsDir()})
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
				Expect(s3fs.WalkDir("/4/5/6/7/", func(name string, de filesystem.DirEntry, e error) error {
					if de != nil {
						entriesWalked = append(entriesWalked, walkDirEntry{name: de.Name(), isDir: de.IsDir()})
					}
					return nil
				})).To(Equal(filesystem.ErrDirectoryNotExists))
				Expect(entriesWalked).To(BeEmpty())
			})
		})
	})
})
