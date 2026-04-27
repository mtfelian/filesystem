package filesystem

import (
	"context"
	"path/filepath"
	"sync"
)

// LocalProviderParams are parameters for local filesystem provider.
type LocalProviderParams struct {
	Root string
}

// LocalProvider returns local filesystems rooted under provider root namespaces.
type LocalProvider struct {
	root string

	mu          sync.Mutex
	filesystems map[string]*Local

	closeOnce sync.Once
	closed    bool
}

// NewLocalProvider returns a service-scoped local filesystem provider.
func NewLocalProvider(p LocalProviderParams) (*LocalProvider, error) {
	root := p.Root
	if root == "" {
		root = "."
	}

	root, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}

	return &LocalProvider{
		root:        filepath.Clean(root),
		filesystems: make(map[string]*Local),
	}, nil
}

// FileSystem returns a local filesystem rooted at the requested namespace.
func (p *LocalProvider) FileSystem(_ context.Context, namespace string) (FileSystem, error) {
	if p == nil {
		return nil, ErrFileSystemClosed
	}

	cleanNamespace, err := cleanLocalName(namespace)
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil, ErrFileSystemClosed
	}
	if fs := p.filesystems[cleanNamespace]; fs != nil {
		return fs, nil
	}

	root := p.root
	if cleanNamespace != "." {
		root = filepath.Join(root, cleanNamespace)
	}
	fs := &Local{
		root:     root,
		isClosed: p.isClosed,
	}
	p.filesystems[cleanNamespace] = fs
	return fs, nil
}

// Close marks provider-owned local filesystems closed.
func (p *LocalProvider) Close() error {
	if p == nil {
		return nil
	}
	p.closeOnce.Do(func() {
		p.mu.Lock()
		p.closed = true
		p.mu.Unlock()
	})
	return nil
}

func (p *LocalProvider) isClosed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.closed
}
