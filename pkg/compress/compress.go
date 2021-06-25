package compress

type Compress interface {
	Name() string
	CompressBound(int) int
	Compress(dst, src []byte) (int, error)
	Decompress(dst, src []byte) (int, error)
}
