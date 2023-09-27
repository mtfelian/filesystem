package filesystem

import (
	"context"
	"fmt"
	"path/filepath"
)

// EmptySubtreeCleaner removes empty directories subtrees
type EmptySubtreeCleaner struct {
	FS FileSystem
}

// Node is a tree node
type Node struct {
	Path     string
	Children []*Node
}

// newEmptySubtreeCleaner creates and returns a new EmptySubtreeCleaner
func newEmptySubtreeCleaner(fs FileSystem) EmptySubtreeCleaner { return EmptySubtreeCleaner{FS: fs} }

// IsDir returns true if given name is a directory
func IsDir(ctx context.Context, fs FileSystem, name string) (bool, error) {
	fi, err := fs.Stat(ctx, name)
	if err != nil {
		return false, err
	}
	return fi.IsDir(), nil
}

func (esc *EmptySubtreeCleaner) buildTreeFromDir(ctx context.Context, basePath string) (*Node, error) {
	if _, err := esc.FS.ReadDir(ctx, basePath); err != nil {
		return nil, err
	}
	root := &Node{Path: basePath}
	const maxDepth = 500 // Consider that there can not be any dir with > 500 depth

	queue := make(chan *Node, maxDepth)
	queue <- root
	for len(queue) > 0 {
		data, ok := <-queue
		if !ok || data == nil {
			continue
		}
		// Iterate all the contents in the dir
		isDir, err := IsDir(ctx, esc.FS, data.Path)
		if err != nil {
			return nil, err
		}
		if !isDir {
			continue
		}

		contents, err := esc.FS.ReadDir(ctx, data.Path)
		if err != nil {
			return nil, err
		}

		data.Children = make([]*Node, len(contents))
		for i, content := range contents {
			data.Children[i] = &Node{Path: filepath.Join(data.Path, content.Name())}
			if content.IsDir() {
				queue <- data.Children[i]
			}
		}
	}
	return root, nil
}

func (esc *EmptySubtreeCleaner) printDirTree(root *Node) {
	fmt.Println(root.Path)
	for _, each := range root.Children {
		esc.printDirTree(each)
	}
	if len(root.Children) == 0 {
		fmt.Println("===========")
	}
}

func (esc *EmptySubtreeCleaner) recursiveEmptyDelete(ctx context.Context, root *Node) error {
	if root == nil {
		return nil
	}
	for _, each := range root.Children {
		if err := esc.recursiveEmptyDelete(ctx, each); err != nil {
			return err
		}
	}
	isDir, err := IsDir(ctx, esc.FS, root.Path)
	if err != nil {
		return err
	}
	if !isDir {
		return nil
	}

	isEmpty, err := esc.FS.IsEmptyPath(ctx, root.Path)
	if err != nil {
		return err
	}
	if !isEmpty {
		return nil
	}

	return esc.FS.Remove(ctx, root.Path)
}

// RemoveEmptyDirs removed all subtrees of directories inside basePath
// which contains only empty directories recursively
func RemoveEmptyDirs(ctx context.Context, fs FileSystem, basePath string) error {
	esc := newEmptySubtreeCleaner(fs)
	root, err := esc.buildTreeFromDir(ctx, basePath)
	if err != nil {
		return err
	}
	esc.printDirTree(root)
	return esc.recursiveEmptyDelete(ctx, root)
}
