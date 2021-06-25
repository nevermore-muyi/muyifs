package zstd

import (
	"fmt"

	"github.com/DataDog/zstd"
)

type ZStandard struct{}

func (z *ZStandard) Name() string {
	return "zstd"
}

func (z *ZStandard) CompressBound(srcSize int) int {
	return zstd.CompressBound(srcSize)
}

func (z *ZStandard) Compress(dst, src []byte) (int, error) {
	d, err := zstd.Compress(dst, src)
	if err != nil {
		return 0, err
	}
	if len(d) > 0 && len(dst) > 0 && &d[0] != &dst[0] {
		return 0, fmt.Errorf("buffer too short: %d < %d", cap(dst), cap(d))
	}
	return len(d), err
}

func (z *ZStandard) Decompress(dst, src []byte) (int, error) {
	d, err := zstd.Decompress(dst, src)
	if err != nil {
		return 0, err
	}
	if len(d) > 0 && len(dst) > 0 && &d[0] != &dst[0] {
		return 0, fmt.Errorf("buffer too short: %d < %d", len(dst), len(d))
	}
	return len(d), err
}
