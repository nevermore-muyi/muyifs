package fuse

import (
	"fmt"

	"github.com/nevermore/muyifs/pkg/backend"
	"k8s.io/klog/v2"
)

type WriterAt struct {
	key   string
	fs    *FileSystem
	cache *WriteCache
}

type WriteCache struct {
	uploadID string
	part     []*backend.Part
	num      int
	length   uint64
	offset   int64
	errState bool
	buf      []byte
	chunks   []*Chunk
}

type Chunk struct {
	offset int64
	buf    []byte
}

func NewWriter(key string, fs *FileSystem) CommonWriter {
	return &WriterAt{
		key: key,
		fs:  fs,
		cache: &WriteCache{
			uploadID: "",
			part:     nil,
			num:      1,
			length:   0,
			buf:      make([]byte, CacheSize, CacheSize),
		},
	}
}

func (w *WriterAt) WriteAt(p []byte, off int64) (n int, err error) {

	if w.cache.errState {
		klog.Errorf("WriteAt has cache error, do not continue")
		return 0, fmt.Errorf("write error")
	}

	pLen := len(p)
	// Init
	if off == 0 {
		mu, err := w.fs.Backend.InitiateMultipartUpload(w.key)
		if err != nil {
			klog.Errorf("WriteAt buffer InitiateMultipartUpload error %v", err)
			return 0, err
		}
		w.cache.uploadID = mu.UploadID
		w.cache.length = 0
		w.cache.offset = 0
		w.cache.num = 1
		w.cache.part = make([]*backend.Part, 0)
	}

	if w.cache.length+uint64(pLen) > CacheSize {
		// need upload
		part, err := w.fs.Backend.UploadPart(w.key, w.cache.uploadID, w.cache.num, w.cache.buf[:w.cache.length])
		if err != nil {
			klog.Errorf("WriteAt buffer error %v", err)
			w.cache.errState = true
			return 0, err
		}
		w.cache.part = append(w.cache.part, part)
		w.cache.length = 0
		w.cache.num++
	}

	if off == w.cache.offset {
		copy(w.cache.buf[w.cache.length:w.cache.length+uint64(pLen)], p)
		w.cache.length += uint64(pLen)
		w.cache.offset += int64(pLen)
	} else {
		c := &Chunk{}
		c.offset = off
		c.buf = make([]byte, pLen)
		copy(c.buf, p)
		w.cache.chunks = append(w.cache.chunks, c)
	}

	for i := 0; i < len(w.cache.chunks); {
		if w.cache.chunks[i].offset == w.cache.offset {
			if w.cache.length+uint64(len(w.cache.chunks[i].buf)) > CacheSize {
				part, err := w.fs.Backend.UploadPart(w.key, w.cache.uploadID, w.cache.num, w.cache.buf[:w.cache.length])
				if err != nil {
					klog.Errorf("WriteAt buffer error %v", err)
					w.cache.errState = true
					return 0, err
				}
				w.cache.part = append(w.cache.part, part)
				w.cache.length = 0
				w.cache.num++
			}
			copy(w.cache.buf[w.cache.length:w.cache.length+uint64(len(w.cache.chunks[i].buf))], w.cache.chunks[i].buf)
			w.cache.length += uint64(len(w.cache.chunks[i].buf))
			w.cache.offset += int64(len(w.cache.chunks[i].buf))
			w.cache.chunks = append(w.cache.chunks[:i], w.cache.chunks[i+1:]...)
			i = 0
			continue
		}
		i++
	}

	return pLen, nil
}

func (w *WriterAt) Flush() error {
	if w.cache.errState {
		klog.Errorf("WriteAt has cache error, do not flush")
		w.fs.Backend.AbortUpload(w.key, w.cache.uploadID)
		return fmt.Errorf("flush error")
	}

	if w.cache.length == 0 && len(w.cache.chunks) == 0 {
		return nil
	}

	// do upload
	for i := 0; i < len(w.cache.chunks); {
		if w.cache.chunks[i].offset == w.cache.offset {
			if w.cache.length+uint64(len(w.cache.chunks[i].buf)) > CacheSize {
				part, err := w.fs.Backend.UploadPart(w.key, w.cache.uploadID, w.cache.num, w.cache.buf[:w.cache.length])
				if err != nil {
					klog.Errorf("WriteAt buffer error %v", err)
					w.cache.errState = true
					return err
				}
				w.cache.part = append(w.cache.part, part)
				w.cache.length = 0
				w.cache.num++
			}
			copy(w.cache.buf[w.cache.length:w.cache.length+uint64(len(w.cache.chunks[i].buf))], w.cache.chunks[i].buf)
			w.cache.length += uint64(len(w.cache.chunks[i].buf))
			w.cache.offset += int64(len(w.cache.chunks[i].buf))
			w.cache.chunks = append(w.cache.chunks[:i], w.cache.chunks[i+1:]...)
			i = 0
			continue
		}
		i++
	}

	if w.cache.length > 0 {
		part, err := w.fs.Backend.UploadPart(w.key, w.cache.uploadID, w.cache.num, w.cache.buf[:w.cache.length])
		if err != nil {
			klog.Errorf("WriteAt buffer error %v", err)
			w.cache.errState = true
			return err
		}
		w.cache.part = append(w.cache.part, part)
		w.cache.length = 0
		w.cache.num = 1
	}

	err := w.fs.Backend.CompleteUpload(w.key, w.cache.uploadID, w.cache.part)
	if err != nil {
		klog.Errorf("WriteAt CompleteUpload error %v", err)
		w.cache.errState = true
		return err
	}

	return nil
}

func (w *WriterAt) Release() {
	w.cache.uploadID = ""
	w.cache.part = nil
	w.cache.num = 1
	w.cache.length = 0
	w.cache.chunks = make([]*Chunk, 0)
	w.cache.errState = false
}

type ReaderAt struct {
	key      string
	errState bool
	fs       *FileSystem
	cache    *ReadCache
}

type ReadCache struct {
	start  int64
	offset int64
	size   int64
	buf    []byte
}

func NewReader(key string, fs *FileSystem) CommonReader {
	return &ReaderAt{
		key:      key,
		errState: false,
		fs:       fs,
		cache: &ReadCache{
			start:  0,
			offset: 0,
			size:   0,
			buf:    make([]byte, CacheSize, CacheSize),
		},
	}
}

func (r *ReaderAt) ReadAt(p []byte, offset int64) (n int, err error) {

	if r.errState {
		return 0, fmt.Errorf("read is in error state")
	}

	if offset == 0 {
		o, err := r.fs.Backend.Head(r.key)
		if err != nil {
			r.errState = true
			klog.Errorf("Head object %v error %v", r.key, err)
			return 0, err
		}
		r.cache.size = o.Size
	}

	if offset >= r.cache.size {
		klog.Errorf("buffer has download, over")
		return len(p), nil
	}

	// Reload
	if offset+int64(len(p)) > r.cache.offset {
		if r.cache.offset >= r.cache.size {
			newoff := offset - r.cache.start
			copy(p, r.cache.buf[newoff:newoff+r.cache.offset-offset])
			return len(p), nil
		}
		n, err = r.fs.Backend.Get(r.key, r.cache.offset, CacheSize, r.cache.buf)
		if err != nil {
			r.errState = true
			klog.Errorf("ReadAt error %v", err)
			return 0, err
		}
		r.cache.start = r.cache.offset
		r.cache.offset += int64(n)
	}

	// Cache hit
	if offset >= r.cache.start && offset+int64(len(p)) <= r.cache.offset {
		newoff := offset - r.cache.start
		copy(p, r.cache.buf[newoff:newoff+int64(len(p))])
		return len(p), nil
	}

	// Single load
	n, err = r.fs.Backend.Get(r.key, offset, int64(len(p)), p)
	if err != nil {
		r.errState = true
		klog.Errorf("ReadAt error %v", err)
		return 0, err
	}
	return len(p), nil
}

func (r *ReaderAt) Release() {
	r.cache.start = 0
	r.cache.offset = 0
	r.cache.size = 0
	r.errState = false
}
