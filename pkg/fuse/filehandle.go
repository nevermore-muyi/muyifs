package fuse

import (
	"context"
	"io"
	"sync"
	"syscall"

	"bazil.org/fuse"
	"k8s.io/klog/v2"
)

type FileHandle struct {
	sync.Mutex
	ID     uint64
	Uid    uint32
	Gid    uint32
	f      *File
	reader CommonReader
	writer CommonWriter
}

const (
	CacheSize = 1 << 26
)

func NewFileHandle(f *File, uid, gid uint32) *FileHandle {
	return &FileHandle{
		Uid: uid,
		Gid: gid,
		f:   f,
	}
}

func (fh *FileHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	fh.Lock()
	defer fh.Unlock()

	if req.Size <= 0 {
		return nil
	}

	buff := resp.Data[:cap(resp.Data)]
	if req.Size > cap(resp.Data) {
		// should not happen
		buff = make([]byte, req.Size)
	}

	totalRead, err := fh.reader.ReadAt(buff, req.Offset)
	if err != nil && err != io.EOF {
		klog.Errorf("FileHandle ReadAt error %v", err)
		return fuse.Errno(syscall.EIO)
	}
	resp.Data = buff[:totalRead]
	return nil
}

func max(x, y uint64) uint64 {
	if x > y {
		return x
	}
	return y
}

func (fh *FileHandle) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	fh.Lock()
	defer fh.Unlock()

	n, err := fh.writer.WriteAt(req.Data, req.Offset)
	if err != nil {
		klog.Errorf("Write buffer error %v", err)
		return err
	}
	fh.f.attr.Size = max(fh.f.attr.Size, uint64(req.Offset+int64(n)))
	resp.Size = n
	return nil
}

func (fh *FileHandle) Flush(ctx context.Context, req *fuse.FlushRequest) error {
	fh.Lock()
	defer fh.Unlock()
	return fh.writer.Flush()
}

func (fh *FileHandle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	fh.Lock()
	defer fh.Unlock()
	fh.writer.Release()
	fh.reader.Release()
	return nil
}
