package filesystem

import (
	"path/filepath"
	"time"

	"github.com/sirupsen/logrus"
)

// S3Params are parameters for S3 filesystem client
type S3Params struct {
	Endpoint   string
	Region     string
	AccessKey  string
	SecretKey  string
	UseSSL     bool
	BucketName string
	BucketTTL  time.Duration // it will be ceiled to days

	OpenedFilesTTL     time.Duration
	OpenedFilesTempDir string

	Logger logrus.FieldLogger

	EmulateEmptyDirs     bool // without this directory modification time will not be available
	ListDirectoryEntries bool // in the ReadDir output
}

// S3ProviderParams are shared parameters for S3 filesystem provider.
type S3ProviderParams struct {
	Endpoint  string
	Region    string
	AccessKey string
	SecretKey string
	UseSSL    bool

	OpenedFilesTTL     time.Duration
	OpenedFilesTempDir string

	Logger logrus.FieldLogger

	DefaultBucketOptions S3BucketOptions
}

// S3BucketOptions are bucket-specific S3 filesystem options.
type S3BucketOptions struct {
	CreateIfMissing      bool
	EnsureOnFirstUse     bool
	BucketTTL            time.Duration // it will be ceiled to days
	EmulateEmptyDirs     bool          // without this directory modification time will not be available
	ListDirectoryEntries bool          // in the ReadDir output
}

func (s3p *S3Params) applyDefaults() {
	const defaultOpenedFilesTTL = 10 * time.Minute
	if s3p.OpenedFilesTTL <= 0 {
		s3p.OpenedFilesTTL = defaultOpenedFilesTTL
	}
	if len(s3p.OpenedFilesTempDir) == 0 {
		s3p.OpenedFilesTempDir = "." + string(filepath.Separator)
	}
	if s3p.Logger == nil {
		s3p.Logger = logrus.StandardLogger()
	}
}

func (s3p *S3ProviderParams) applyDefaults() {
	const defaultOpenedFilesTTL = 10 * time.Minute
	if s3p.OpenedFilesTTL <= 0 {
		s3p.OpenedFilesTTL = defaultOpenedFilesTTL
	}
	if len(s3p.OpenedFilesTempDir) == 0 {
		s3p.OpenedFilesTempDir = "." + string(filepath.Separator)
	}
	if s3p.Logger == nil {
		s3p.Logger = logrus.StandardLogger()
	}
}
