package filesystem_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/mtfelian/filesystem"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/sirupsen/logrus"
)

var errFailingReader = errors.New("reader failed")

type failingReader struct {
	read bool
}

func (r *failingReader) Read(p []byte) (int, error) {
	if r.read {
		return 0, errFailingReader
	}
	r.read = true
	return copy(p, "partial content"), nil
}

var _ = Describe("Local FileSystem implementation", func() {
	var (
		fsLocal filesystem.FileSystem
		ctx     context.Context
		l       *logrus.Logger

		dir0, dir1, dir2                string
		fullName1, fullName2, fullName3 string
		fullNameToContent               map[string][]byte
	)
	const (
		content1 = "content 1"
		content2 = "content 2"
		content3 = "content 3"
	)
	BeforeEach(func() {
		l = logrus.New()
		l.SetLevel(logrus.DebugLevel)

		fsLocal = filesystem.NewLocal()
		ctx = context.Background()

		var err error
		dir0, err = os.Getwd()
		Expect(err).NotTo(HaveOccurred())
		dir0 = fsLocal.Join(dir0, filesystem.TempDir)
		dir1 = fsLocal.Join(dir0, "b")
		dir2 = fsLocal.Join(dir1, "c_d")
		fullName1 = fsLocal.Join(dir2, "1.txt")
		fullName2 = fsLocal.Join(dir2, "2.txt")
		fullName3 = fsLocal.Join(dir0, "3.txt")
		fullNameToContent = map[string][]byte{
			fullName1: []byte(content1),
			fullName2: []byte(content2),
			fullName3: []byte(content3),
		}

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
	})

	prepareSpec := func() {
		By("creating objects", func() {
			for fullName, content := range fullNameToContent {
				Expect(fsLocal.WriteFile(ctx, fullName, content)).To(Succeed())
			}
		})
	}

	Describe("Opening files in various modes and closing", func() {
		var (
			f      filesystem.File
			opened bool
		)

		JustBeforeEach(func() {
			prepareSpec()
		})

		JustAfterEach(func() {
			if opened {
				Expect(f.Close()).To(Succeed())
				opened = false
			}
		})

		Describe("ReadDir", func() {
			It("checks if file is not a dir", func() {
				_, err := fsLocal.ReadDir(ctx, fullName1)
				Expect(err).To(Equal(filesystem.ErrNotADirectory))
			})

			It("checks reading existing non-empty dir with files", func() {
				fi, err := fsLocal.ReadDir(ctx, dir2)
				Expect(err).NotTo(HaveOccurred())
				Expect(fi).To(HaveLen(2))
				names := fi.FullNames()
				Expect(names).To(ConsistOf([]string{fullName1, fullName2}))
			})

			It("checks reading existing non-empty dir with subdirs and files", func() {
				fi, err := fsLocal.ReadDir(ctx, dir0)
				Expect(err).NotTo(HaveOccurred())
				Expect(fi).To(HaveLen(2))
				names := fi.FullNames()
				Expect(names).To(ConsistOf([]string{fullName3, dir1}))
			})

			It("checks reading existing empty dir", func() {
				By("removing everything inside dir0", func() {
					Expect(fsLocal.Remove(ctx, fullName3)).To(Succeed())
					Expect(fsLocal.RemoveAll(ctx, dir1)).To(Succeed())
				})

				fi, err := fsLocal.ReadDir(ctx, dir0)
				Expect(err).NotTo(HaveOccurred())
				Expect(fi).To(BeEmpty())
				names := fi.FullNames()
				Expect(names).To(BeEmpty())
			})

			It("checks for large amount of files", func() {
				const amount = 1500
				largeDir := fsLocal.Join(filesystem.TempDir, "manyfiles")
				nameFunc := func(i int) string { return fsLocal.Join(largeDir, fmt.Sprintf("item %d", i)) }
				contentFunc := func(i int) []byte { return []byte(fmt.Sprintf("content %d", i)) }
				l.Info("creating large local directory...")
				By("creating directory and files", func() {
					for i := 0; i < amount; i++ {
						if (i+1)%100 == 0 || i == amount-1 {
							l.Infof("creating test local files... %.2f%%", float64(i+1)*100./float64(amount))
						}
						Expect(fsLocal.WriteFile(ctx, nameFunc(i), contentFunc(i))).To(Succeed())
					}
				})

				By("reading directory", func() {
					l.Info("reading large local directory...")
					fsi, err := fsLocal.ReadDir(ctx, largeDir)
					Expect(err).NotTo(HaveOccurred())
					Expect(fsi).To(HaveLen(amount))
				})
			})

			It("checks reading existing non-empty dir with subdirs and files", func() {
				fi, err := fsLocal.ReadDir(ctx, dir0)
				Expect(err).NotTo(HaveOccurred())
				Expect(fi).To(HaveLen(2))
				names := fi.FullNames()
				Expect(names).To(ConsistOf([]string{dir1, fullName3}))
			})

			It("checks reading existing empty dir", func() {
				By("recreate dir0", func() {
					Expect(fsLocal.RemoveAll(ctx, dir0)).To(Succeed())
					Expect(fsLocal.MakePathAll(ctx, dir0)).To(Succeed())
				})

				fi, err := fsLocal.ReadDir(ctx, dir0)
				Expect(err).NotTo(HaveOccurred())
				Expect(fi).To(BeEmpty())
				names := fi.FullNames()
				Expect(names).To(BeEmpty())
			})
		})

		Describe("Open errors", func() {
			It("returns nil file when Open fails", func() {
				f, err := fsLocal.Open(ctx, fsLocal.Join(dir0, "missing.txt"))
				Expect(err).To(HaveOccurred())
				Expect(f).To(BeNil())
			})

			It("returns nil file when OpenW fails", func() {
				f, err := fsLocal.OpenW(ctx, fsLocal.Join(dir0, "missing.txt"))
				Expect(err).To(HaveOccurred())
				Expect(f).To(BeNil())
			})
		})

		Describe("RemoveFiles", func() {
			It("reports failed paths without returning an order-dependent error", func() {
				failed, err := fsLocal.RemoveFiles(ctx, []string{
					fsLocal.Join(dir0, "missing.txt"),
					fullName1,
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(failed).To(Equal([]string{fsLocal.Join(dir0, "missing.txt")}))

				exists, err := fsLocal.Exists(ctx, fullName1)
				Expect(err).NotTo(HaveOccurred())
				Expect(exists).To(BeFalse())
			})
		})

		Describe("Rename", func() {
			It("creates missing destination parent directories", func() {
				target := fsLocal.Join(dir0, "renamed", "nested", "1.txt")

				Expect(fsLocal.Rename(ctx, fullName1, target)).To(Succeed())

				exists, err := fsLocal.Exists(ctx, fullName1)
				Expect(err).NotTo(HaveOccurred())
				Expect(exists).To(BeFalse())

				content, err := fsLocal.ReadFile(ctx, target)
				Expect(err).NotTo(HaveOccurred())
				Expect(content).To(Equal([]byte(content1)))
			})
		})

		Describe("WriteReader", func() {
			It("writes reader content to a nested file", func() {
				content := strings.Repeat("streamed local content ", 64)
				target := fsLocal.Join(dir0, "streamed", "reader.txt")

				Expect(fsLocal.WriteReader(ctx, target, strings.NewReader(content), int64(len(content)))).To(Succeed())

				actual, err := fsLocal.ReadFile(ctx, target)
				Expect(err).NotTo(HaveOccurred())
				Expect(string(actual)).To(Equal(content))
			})

			It("overwrites an existing file", func() {
				target := fsLocal.Join(dir0, "streamed", "existing.txt")
				Expect(fsLocal.WriteFile(ctx, target, []byte("old content"))).To(Succeed())

				Expect(fsLocal.WriteReader(ctx, target, strings.NewReader("new content"), int64(len("new content")))).
					To(Succeed())

				actual, err := fsLocal.ReadFile(ctx, target)
				Expect(err).NotTo(HaveOccurred())
				Expect(string(actual)).To(Equal("new content"))
			})

			It("rejects mismatched reader size and removes the staged file", func() {
				targetDir := fsLocal.Join(dir0, "streamed-size-mismatch")
				target := fsLocal.Join(targetDir, "reader.txt")

				err := fsLocal.WriteReader(ctx, target, strings.NewReader("short"), 10)
				Expect(errors.Is(err, filesystem.ErrUnexpectedSize)).To(BeTrue())

				exists, err := fsLocal.Exists(ctx, target)
				Expect(err).NotTo(HaveOccurred())
				Expect(exists).To(BeFalse())

				entries, err := fsLocal.ReadDir(ctx, targetDir)
				Expect(err).NotTo(HaveOccurred())
				Expect(entries).To(BeEmpty())
			})

			It("removes the staged file when reader fails", func() {
				targetDir := fsLocal.Join(dir0, "streamed-reader-failure")
				target := fsLocal.Join(targetDir, "reader.txt")

				err := fsLocal.WriteReader(ctx, target, &failingReader{}, 100)
				Expect(errors.Is(err, errFailingReader)).To(BeTrue())

				exists, err := fsLocal.Exists(ctx, target)
				Expect(err).NotTo(HaveOccurred())
				Expect(exists).To(BeFalse())

				entries, err := fsLocal.ReadDir(ctx, targetDir)
				Expect(err).NotTo(HaveOccurred())
				Expect(entries).To(BeEmpty())
			})
		})

		Describe("PreparePath", func() {
			It("returns Exists errors", func() {
				before := filesystem.BeforeOperationCB()
				sentinel := errors.New("exists failed")
				calls := 0
				filesystem.SetBeforeOperationCB(func(ctx context.Context) (context.Context, error) {
					calls++
					if calls == 2 {
						return ctx, sentinel
					}
					return before(ctx)
				})
				defer filesystem.SetBeforeOperationCB(before)

				_, err := fsLocal.PreparePath(ctx, fsLocal.Join(dir0, "prepared"))
				Expect(err).To(Equal(sentinel))
			})
		})
	})
})
