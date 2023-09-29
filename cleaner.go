package filesystem

import (
	"context"
	"errors"

	"github.com/sirupsen/logrus"
)

// EmptySubtreeCleaner removes empty directories subtrees
type EmptySubtreeCleaner struct {
	FS     FileSystem
	Count  int
	stack  []string
	Logger logrus.FieldLogger
}

// Node is a tree node
type Node struct {
	Path     string
	Children []*Node
}

// newEmptySubtreeCleaner creates and returns a new EmptySubtreeCleaner
func newEmptySubtreeCleaner(fs FileSystem, logger logrus.FieldLogger) EmptySubtreeCleaner {
	return EmptySubtreeCleaner{FS: fs, Logger: logger}
}

// IsDir returns true if given name is a directory
func IsDir(ctx context.Context, fs FileSystem, name string) (bool, error) {
	fi, err := fs.Stat(ctx, name)
	if err != nil {
		return false, err
	}
	return fi.IsDir(), nil
}

func (esc *EmptySubtreeCleaner) bfs(ctx context.Context, basePath string) (*Node, error) {
	if _, err := esc.FS.ReadDir(ctx, basePath); err != nil {
		return nil, err
	}
	root := &Node{Path: basePath}
	const maxDepth = 500 // we assume that there will not be any dir with >500 depth

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
			data.Children[i] = &Node{Path: esc.FS.Join(data.Path, content.Name())}
			if content.IsDir() {
				queue <- data.Children[i]
			}
		}
	}
	return root, nil
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

	esc.Count++
	return esc.FS.Remove(ctx, root.Path)
}

func (esc *EmptySubtreeCleaner) dfs(ctx context.Context, p string) error {
	contents, err := esc.FS.ReadDir(ctx, p)
	if err != nil {
		return err
	}
	for _, item := range contents {
		itemPath := esc.FS.Join(p, item.Name())
		isDir, err := IsDir(ctx, esc.FS, itemPath)
		if err != nil {
			return err
		}
		if !isDir {
			continue
		}
		esc.stack = append(esc.stack, itemPath)
		if err := esc.dfs(ctx, itemPath); err != nil {
			return err
		}
	}
	esc.stack = esc.stack[:len(esc.stack)-1]
	isEmpty, err := esc.FS.IsEmptyPath(ctx, p)
	if err != nil {
		return err
	}
	if isEmpty {
		esc.Count++
		if err := esc.FS.Remove(ctx, p); err != nil {
			return err
		}
	}
	return nil
}

const (
	AlgoDFS = "DFS"
	AlgoBFS = "BFS"
)

var errUnknownAlgorithm = errors.New("unknown algorithm")

// RemoveEmptyDirs removed all subtrees of directories inside basePath
// which contains only empty directories recursively
func RemoveEmptyDirs(ctx context.Context, fs FileSystem, logger logrus.FieldLogger, basePath, algo string) (int, error) {
	esc := newEmptySubtreeCleaner(fs, logger)
	if algo != AlgoDFS && algo != AlgoBFS {
		algo = AlgoDFS
	}
	switch algo {
	case AlgoBFS:
		esc.Logger.Info("cleaner: building directory tree (BFS)...")
		root, err := esc.bfs(ctx, basePath)
		if err != nil {
			return 0, err
		}
		esc.Logger.Info("cleaner: removing directories...")
		return esc.Count, esc.recursiveEmptyDelete(ctx, root)
	case AlgoDFS:
		esc.stack = make([]string, 0, 500)
		esc.stack = append(esc.stack, basePath)
		esc.Logger.Info("cleaner: removing directories (DFS)...")
		return esc.Count, esc.dfs(ctx, basePath)
	default:
		return 0, errUnknownAlgorithm
	}
}
