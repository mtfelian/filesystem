package filesystem_test

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/mtfelian/filesystem"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("LocalProvider", func() {
	var (
		ctx      context.Context
		root     string
		provider *filesystem.LocalProvider
	)

	BeforeEach(func() {
		ctx = context.Background()

		var err error
		root, err = os.MkdirTemp("", "filesystem-local-provider-*")
		Expect(err).NotTo(HaveOccurred())

		provider, err = filesystem.NewLocalProvider(filesystem.LocalProviderParams{Root: root})
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		if provider != nil {
			Expect(provider.Close()).To(Succeed())
		}
		Expect(os.RemoveAll(root)).To(Succeed())
	})

	It("checks mapping namespaces to isolated rooted filesystems", func() {
		var fileSystemProvider filesystem.FileSystemProvider = provider

		fsA, err := fileSystemProvider.FileSystem(ctx, "a")
		Expect(err).NotTo(HaveOccurred())
		fsAAgain, err := fileSystemProvider.FileSystem(ctx, "a")
		Expect(err).NotTo(HaveOccurred())
		Expect(fsAAgain).To(BeIdenticalTo(fsA))

		fsB, err := fileSystemProvider.FileSystem(ctx, "b")
		Expect(err).NotTo(HaveOccurred())
		Expect(fsB).NotTo(BeIdenticalTo(fsA))

		name := fsA.Join("dir", "same.txt")
		Expect(fsA.WriteFile(ctx, name, []byte("a"))).To(Succeed())
		Expect(fsB.WriteFile(ctx, name, []byte("b"))).To(Succeed())

		aData, err := os.ReadFile(filepath.Join(root, "a", "dir", "same.txt"))
		Expect(err).NotTo(HaveOccurred())
		Expect(aData).To(Equal([]byte("a")))

		bData, err := os.ReadFile(filepath.Join(root, "b", "dir", "same.txt"))
		Expect(err).NotTo(HaveOccurred())
		Expect(bData).To(Equal([]byte("b")))
	})

	It("checks default namespace rooted at provider root", func() {
		fs, err := provider.FileSystem(ctx, ".")
		Expect(err).NotTo(HaveOccurred())
		fsAgain, err := provider.FileSystem(ctx, "")
		Expect(err).NotTo(HaveOccurred())
		Expect(fsAgain).To(BeIdenticalTo(fs))

		Expect(fs.WriteFile(ctx, "default.txt", []byte("default"))).To(Succeed())

		data, err := os.ReadFile(filepath.Join(root, "default.txt"))
		Expect(err).NotTo(HaveOccurred())
		Expect(data).To(Equal([]byte("default")))
	})

	It("checks rejecting paths that escape a rooted filesystem", func() {
		fs, err := provider.FileSystem(ctx, "safe")
		Expect(err).NotTo(HaveOccurred())

		err = fs.WriteFile(ctx, fs.Join("..", "escape.txt"), []byte("escape"))
		Expect(errors.Is(err, filesystem.ErrPathEscapesRoot)).To(BeTrue())

		_, err = os.Stat(filepath.Join(root, "escape.txt"))
		Expect(os.IsNotExist(err)).To(BeTrue())
	})

	It("checks returning logical names from rooted filesystem metadata", func() {
		fs, err := provider.FileSystem(ctx, "meta")
		Expect(err).NotTo(HaveOccurred())

		name := fs.Join("dir", "file.txt")
		Expect(fs.WriteFile(ctx, name, []byte("content"))).To(Succeed())

		fi, err := fs.Stat(ctx, name)
		Expect(err).NotTo(HaveOccurred())
		Expect(fi.FullName()).To(Equal(name))
		Expect(fi.Name()).To(Equal("file.txt"))

		entries, err := fs.ReadDir(ctx, "dir")
		Expect(err).NotTo(HaveOccurred())
		Expect(entries.FullNames()).To(ConsistOf(name))

		var walked []string
		err = fs.WalkDir(ctx, "dir", func(name string, entry filesystem.DirEntry, err error) error {
			Expect(err).NotTo(HaveOccurred())
			Expect(entry.FullName()).To(Equal(name))
			walked = append(walked, name)
			return nil
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(walked).To(ConsistOf("dir", fs.Join("dir", "file.txt")))
	})

	It("checks close rejects provider and existing filesystem operations", func() {
		fs, err := provider.FileSystem(ctx, "closed")
		Expect(err).NotTo(HaveOccurred())

		Expect(provider.Close()).To(Succeed())

		_, err = provider.FileSystem(ctx, "other")
		Expect(err).To(Equal(filesystem.ErrFileSystemClosed))

		err = fs.WriteFile(ctx, "file.txt", []byte("content"))
		Expect(err).To(Equal(filesystem.ErrFileSystemClosed))
	})

	It("checks rooted filesystem operations stay under namespace root", func() {
		fs, err := provider.FileSystem(ctx, "ops")
		Expect(err).NotTo(HaveOccurred())

		emptyDir := fs.Join("empty", "nested")
		Expect(fs.MakePathAll(ctx, emptyDir)).To(Succeed())
		isEmpty, err := fs.IsEmptyPath(ctx, emptyDir)
		Expect(err).NotTo(HaveOccurred())
		Expect(isEmpty).To(BeTrue())
		Expect(filepath.Join(root, "ops", "empty", "nested")).To(BeADirectory())

		name := fs.Join("dir", "file.txt")
		f, err := fs.Create(ctx, name)
		Expect(err).NotTo(HaveOccurred())
		Expect(f.Name()).To(Equal(name))
		_, err = f.Write([]byte("created"))
		Expect(err).NotTo(HaveOccurred())
		Expect(f.Close()).To(Succeed())

		data, err := fs.ReadFile(ctx, name)
		Expect(err).NotTo(HaveOccurred())
		Expect(data).To(Equal([]byte("created")))

		f, err = fs.Open(ctx, name)
		Expect(err).NotTo(HaveOccurred())
		Expect(f.Name()).To(Equal(name))
		data, err = io.ReadAll(f)
		Expect(err).NotTo(HaveOccurred())
		Expect(data).To(Equal([]byte("created")))
		Expect(f.Close()).To(Succeed())

		f, err = fs.OpenW(ctx, name)
		Expect(err).NotTo(HaveOccurred())
		Expect(f.Name()).To(Equal(name))
		_, err = f.Write([]byte("updated"))
		Expect(err).NotTo(HaveOccurred())
		Expect(f.Close()).To(Succeed())

		r, err := fs.Reader(ctx, name)
		Expect(err).NotTo(HaveOccurred())
		data, err = io.ReadAll(r)
		Expect(err).NotTo(HaveOccurred())
		Expect(data).To(Equal([]byte("updated")))
		Expect(r.Close()).To(Succeed())

		exists, err := fs.Exists(ctx, name)
		Expect(err).NotTo(HaveOccurred())
		Expect(exists).To(BeTrue())

		Expect(fs.WriteFiles(ctx, []filesystem.FileNameData{
			{Name: fs.Join("batch", "one.txt"), Data: []byte("one")},
			{Name: fs.Join("batch", "two.txt"), Data: []byte("two")},
		})).To(Succeed())
		data, err = os.ReadFile(filepath.Join(root, "ops", "batch", "one.txt"))
		Expect(err).NotTo(HaveOccurred())
		Expect(data).To(Equal([]byte("one")))
		data, err = os.ReadFile(filepath.Join(root, "ops", "batch", "two.txt"))
		Expect(err).NotTo(HaveOccurred())
		Expect(data).To(Equal([]byte("two")))

		prepared, err := fs.PreparePath(ctx, fs.Join("prepared", "path"))
		Expect(err).NotTo(HaveOccurred())
		Expect(prepared).To(Equal(filepath.Join(root, "ops", "prepared", "path")))
		Expect(prepared).To(BeADirectory())

		renamed := fs.Join("dir", "renamed.txt")
		Expect(fs.Rename(ctx, name, renamed)).To(Succeed())
		exists, err = fs.Exists(ctx, name)
		Expect(err).NotTo(HaveOccurred())
		Expect(exists).To(BeFalse())
		exists, err = fs.Exists(ctx, renamed)
		Expect(err).NotTo(HaveOccurred())
		Expect(exists).To(BeTrue())

		Expect(fs.Remove(ctx, renamed)).To(Succeed())
		exists, err = fs.Exists(ctx, renamed)
		Expect(err).NotTo(HaveOccurred())
		Expect(exists).To(BeFalse())

		Expect(fs.RemoveAll(ctx, "batch")).To(Succeed())
		exists, err = fs.Exists(ctx, "batch")
		Expect(err).NotTo(HaveOccurred())
		Expect(exists).To(BeFalse())
	})

	It("checks concurrent namespace lookup returns one cached filesystem", func() {
		const goroutines = 64

		var (
			wg      sync.WaitGroup
			results = make([]filesystem.FileSystem, goroutines)
			errs    = make(chan error, goroutines)
		)

		wg.Add(goroutines)
		for i := 0; i < goroutines; i++ {
			i := i
			go func() {
				defer wg.Done()
				fs, err := provider.FileSystem(ctx, "concurrent")
				if err != nil {
					errs <- err
					return
				}
				results[i] = fs
			}()
		}
		wg.Wait()
		close(errs)

		for err := range errs {
			Expect(err).NotTo(HaveOccurred())
		}
		for i := 1; i < goroutines; i++ {
			Expect(results[i]).To(BeIdenticalTo(results[0]))
		}
	})
})
