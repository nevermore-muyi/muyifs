package lz4

import (
	"github.com/hungys/go-lz4"
)

type LZ4 struct{}

func (l *LZ4) Name() string {
	return "lz4"
}

func (l *LZ4) CompressBound(srcSize int) int {
	return lz4.CompressBound(srcSize)
}

func (l *LZ4) Compress(dst, src []byte) (int, error) {
	return lz4.CompressDefault(src, dst)
}

func (l *LZ4) Decompress(dst, src []byte) (int, error) {
	return lz4.DecompressSafe(src, dst)
}
