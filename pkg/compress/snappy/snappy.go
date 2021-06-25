package snappy

import "github.com/golang/snappy"

type Snappy struct{}

func (s *Snappy) Name() string {
	return "snappy"
}

func (s *Snappy) CompressBound(srcSize int) int {
	return snappy.MaxEncodedLen(srcSize)
}

func (s *Snappy) Compress(dst, src []byte) (int, error) {
	return len(snappy.Encode(dst, src)), nil
}

func (s *Snappy) Decompress(dst, src []byte) (int, error) {
	data, err := snappy.Decode(dst, src)
	if err != nil {
		return 0, err
	}
	return len(data), nil
}
