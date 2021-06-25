package backend

import (
	"io"
	"time"
)

type Object struct {
	Key      string
	Size     int64
	Mtime    time.Time
	IsDir    bool
	Metadata map[string]string
}

type MultipartUpload struct {
	MinPartSize int
	MaxCount    int
	UploadID    string
}

type Part struct {
	Num  int
	Size int
	ETag string
}

type ObjectStorage interface {
	String() string
	Create() error
	Head(key string) (Object, error)
	Get(key string, off, limit int64, buf []byte) (int, error)
	Put(key string, metadata map[string]string, in io.Reader) error
	PutDirectory(key string) error
	Delete(key string) error
	DeleteList(key string) error
	List(prefix string) ([]Object, error)
	InitiateMultipartUpload(key string) (*MultipartUpload, error)
	UploadPart(key string, uploadID string, num int, body []byte) (*Part, error)
	AbortUpload(key string, uploadID string) error
	CompleteUpload(key string, uploadID string, parts []*Part) error
}
