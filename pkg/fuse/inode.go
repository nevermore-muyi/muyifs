package fuse

var ino uint64

func init() {
	ino = 2
}

func GenerateInode() uint64 {
	ino++
	return ino
}
