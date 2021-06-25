package fuse

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/nevermore/muyifs/pkg/compress"
)

type Compress struct {
	Enable      bool
	CompressBuf []byte
	Compress    compress.Compress
}

type ChunkMeta struct {
	Index        int   `json:"index"`
	Start        int64 `json:"start"`
	End          int64 `json:"end"`
	CompressSize int64 `json:"compress_size,omitempty"`
}

type CommonWriter interface {
	WriteAt(p []byte, off int64) (n int, err error)
	Flush() error
	Release()
}

type CommonReader interface {
	ReadAt(p []byte, offset int64) (n int, err error)
	Release()
}

type ID [sha256.Size]byte

func (id ID) String() string {
	return hex.EncodeToString(id[:])
}
