package fuse

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/nevermore/muyifs/pkg/compress/lz4"
	"github.com/nevermore/muyifs/pkg/compress/snappy"
	"github.com/restic/chunker"
	"k8s.io/klog/v2"

	"github.com/nevermore/muyifs/pkg/compress/zstd"
)

type ChunkWriter struct {
	key      string
	index    int
	compress Compress
	*ChunkCache
	fs         *FileSystem
	ChunkMetas []ChunkMeta `json:"chunk_metas"`
}

type ChunkCache struct {
	isError  bool
	isFixed  bool
	pol      chunker.Pol
	chnker   *chunker.Chunker
	chunkBuf []byte
	offset   int64
	length   int64
	extras   []*ExtraCache
}

type ExtraCache struct {
	off    int64
	buffer []byte
}

const (
	ChunkCacheFixedSize       = 1 << 23
	ChunkCacheDynamicSize     = 1 << 26
	ChunkCacheDynamicReadSize = 1 << 24

	MetaKey = "chunkid"
)

func NewChunkWriter(key string, fs *FileSystem, compress string, isFixed bool) CommonWriter {
	c := &ChunkWriter{
		key:   key,
		index: 0,
		fs:    fs,
	}
	c.ChunkCache = &ChunkCache{
		isError: false,
		isFixed: isFixed,
		offset:  0,
		length:  0,
	}
	if isFixed {
		c.ChunkCache.chunkBuf = make([]byte, ChunkCacheFixedSize, ChunkCacheFixedSize)
	} else {
		c.ChunkCache.chunkBuf = make([]byte, ChunkCacheDynamicSize, ChunkCacheDynamicSize)
		c.ChunkCache.pol = chunker.Pol(0x3DA3358B4DC173)
		c.ChunkCache.chnker = chunker.New(nil, chunker.Pol(0x3DA3358B4DC173))
	}
	c.ChunkMetas = append(c.ChunkMetas, ChunkMeta{
		Index:        0,
		Start:        0,
		End:          0,
		CompressSize: 0,
	})
	if compress != "" {
		c.compress = Compress{
			Enable:   true,
			Compress: &zstd.ZStandard{},
		}
		switch compress {
		case "zstd":
			c.compress.Compress = &zstd.ZStandard{}
		case "lz4":
			c.compress.Compress = &lz4.LZ4{}
		case "snappy":
			c.compress.Compress = &snappy.Snappy{}
		}
	}
	return c
}

func (c *ChunkWriter) doFixedJob(buf []byte) error {
	pLen := int64(len(buf))
	if pLen+c.length <= ChunkCacheFixedSize {
		copy(c.chunkBuf[c.length:c.length+pLen], buf)
		c.offset += pLen
		c.length += pLen
		if c.length == ChunkCacheFixedSize {
			if err := c.dispatchUpload(); err != nil {
				c.isError = true
				return fmt.Errorf("upload is in error state")
			}
		}
	} else {
		remain := ChunkCacheFixedSize - c.length
		copy(c.chunkBuf[c.length:ChunkCacheFixedSize], buf[:remain])
		c.offset += remain
		c.length += remain
		if err := c.dispatchUpload(); err != nil {
			c.isError = true
			return fmt.Errorf("upload is in error state")
		}
		copy(c.chunkBuf[c.length:pLen-remain], buf[remain:])
		c.length += pLen - remain
		c.offset += pLen - remain
	}
	return nil
}

func (c *ChunkWriter) doDynamicJob(buf []byte) error {
	pLen := int64(len(buf))
	if pLen+c.length > ChunkCacheDynamicSize {
		if err := c.dispatchUpload(); err != nil {
			c.isError = true
			return err
		}
	}
	copy(c.chunkBuf[c.length:c.length+pLen], buf)
	c.offset += pLen
	c.length += pLen
	return nil
}

func (c *ChunkWriter) dispatchJob(buf []byte) error {
	if c.isFixed {
		return c.doFixedJob(buf)
	}
	return c.doDynamicJob(buf)
}

func (c *ChunkWriter) WriteAt(p []byte, off int64) (n int, err error) {

	if c.isError {
		return 0, fmt.Errorf("upload is in error state")
	}

	// off == c.offset
	if off == c.offset {
		err = c.dispatchJob(p)
		return len(p), err
	}

	// off != c.offset
	ec := &ExtraCache{}
	ec.off = off
	ec.buffer = make([]byte, len(p))
	copy(ec.buffer, p)
	c.extras = append(c.extras, ec)

	for i := 0; i < len(c.extras); {
		if c.extras[i].off == c.offset {
			if err = c.dispatchJob(c.extras[i].buffer); err != nil {
				return 0, err
			}
			c.extras = append(c.extras[:i], c.extras[i+1:]...)
			i = 0
			continue
		}
		i++
	}
	return len(p), nil
}

func (c *ChunkWriter) uploadKey() string {
	return c.key + "/" + strconv.Itoa(c.index)
}

func (c *ChunkWriter) hash(data []byte) ID {
	return sha256.Sum256(data)
}

func (c *ChunkWriter) dispatchUpload() error {
	if c.isFixed {
		return c.doFixedUpload()
	}
	return c.doDynamicUpload()
}

func (c *ChunkWriter) doCompress(buf []byte) ([]byte, error) {
	needSize := c.compress.Compress.CompressBound(len(buf))
	tmpBuf := make([]byte, needSize, needSize)
	n, err := c.compress.Compress.Compress(tmpBuf, buf)
	if err != nil {
		klog.Errorf("Do compress error %v", err)
		return []byte{}, err
	}
	return tmpBuf[:n], nil
}

func (c *ChunkWriter) checkDuplicate(id ID) bool {
	o, err := c.fs.Backend.Head(c.uploadKey())
	if err != nil {
		klog.Errorf("Head error %v", err)
		return false
	}

	for k, v := range o.Metadata {
		if strings.ToLower(k) == MetaKey && v == id.String() {
			klog.Errorf("Do not need upload")
			return true
		}
	}
	return false
}

func (c *ChunkWriter) doFixedUpload() error {

	var reader *bytes.Reader
	var id ID
	compressSize := int64(len(c.chunkBuf))

	if c.compress.Enable {
		data, err := c.doCompress(c.chunkBuf)
		if err != nil {
			return err
		}
		compressSize = int64(len(data))
		reader = bytes.NewReader(data)
		id = c.hash(data)
	} else {
		reader = bytes.NewReader(c.chunkBuf)
		id = c.hash(c.chunkBuf)
	}

	if !c.checkDuplicate(id) {
		err := c.fs.Backend.Put(c.uploadKey(), map[string]string{MetaKey: id.String()}, reader)
		if err != nil {
			c.isError = true
			klog.Errorf("Do upload job error %v", err)
			return err
		}
	}

	c.ChunkMetas[c.index].End = c.offset
	c.ChunkMetas[c.index].CompressSize = compressSize
	c.index++
	c.ChunkMetas = append(c.ChunkMetas, ChunkMeta{
		Index:        c.index,
		Start:        c.offset,
		End:          0,
		CompressSize: 0,
	})
	c.length = 0

	return nil
}

func (c *ChunkWriter) doDynamicUpload() error {

	var id ID
	var reader *bytes.Reader

	tmpBuf := make([]byte, 1<<23)
	rd := bytes.NewReader(c.chunkBuf[:c.length])
	c.chnker.ResetWithBoundaries(rd, c.pol, 1<<22, ChunkCacheDynamicReadSize)
	c.chnker.SetAverageBits(23)
	tmpOff := c.offset - c.length

	for {
		chunk, err := c.chnker.Next(tmpBuf)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return err
			}
		}
		tmpOff += int64(chunk.Length)
		compressSize := int64(len(chunk.Data))

		if c.compress.Enable {
			data, err := c.doCompress(chunk.Data)
			if err != nil {
				return err
			}
			compressSize = int64(len(data))
			reader = bytes.NewReader(data)
			id = c.hash(data)
		} else {
			reader = bytes.NewReader(chunk.Data)
			id = c.hash(chunk.Data)
		}

		if !c.checkDuplicate(id) {
			if err := c.fs.Backend.Put(c.uploadKey(), map[string]string{MetaKey: id.String()}, reader); err != nil {
				return err
			}
		}

		c.ChunkMetas[c.index].End = tmpOff
		c.ChunkMetas[c.index].CompressSize = compressSize
		c.index++
		c.ChunkMetas = append(c.ChunkMetas, ChunkMeta{
			Index: c.index,
			Start: tmpOff,
			End:   0,
		})
	}
	c.length = 0
	return nil
}

func (c *ChunkWriter) Flush() error {

	if c.isError {
		return fmt.Errorf("upload is in error state")
	}

	if c.length == 0 && len(c.extras) == 0 {
		return nil
	}

	for i := 0; i < len(c.extras); {
		if c.extras[i].off == c.offset {
			if err := c.dispatchJob(c.extras[i].buffer); err != nil {
				return err
			}
			c.extras = append(c.extras[:i], c.extras[i+1:]...)
			i = 0
			continue
		}
		i++
	}

	if c.length > 0 {
		err := c.dispatchUpload()
		if err != nil {
			return fmt.Errorf("upload is in error state")
		}
	}

	b, err := json.Marshal(c.ChunkMetas)
	if err != nil {
		return fmt.Errorf("json marshal failed %v", err)
	}
	return c.fs.Backend.Put(c.key+"/.meta", map[string]string{"aa": "bb"}, bytes.NewReader(b))
}

func (c *ChunkWriter) Release() {
	c.index = 0
	c.isError = false
	c.length = 0
	c.offset = 0
	c.extras = nil
}

type ChunkReader struct {
	key        string
	fs         *FileSystem
	compress   Compress
	ErrState   bool
	ChunkMetas []ChunkMeta `json:"chunk_metas"`
	c          ChunkReadCache
	ec         ChunkReadCache
}

type ChunkReadCache struct {
	buf    []byte
	start  int64
	offset int64
}

func NewChunkReader(key string, fs *FileSystem, compress string, isFixed bool) CommonReader {
	r := &ChunkReader{
		key:      key,
		fs:       fs,
		ErrState: false,
	}
	r.c.start = 0
	r.c.offset = 0
	r.c.buf = make([]byte, ChunkCacheFixedSize, ChunkCacheFixedSize)
	r.ec.start = 0
	r.ec.offset = 0
	r.ec.buf = make([]byte, ChunkCacheFixedSize, ChunkCacheFixedSize)

	if !isFixed {
		r.c.buf = make([]byte, ChunkCacheDynamicReadSize, ChunkCacheDynamicReadSize)
		r.ec.buf = make([]byte, ChunkCacheDynamicReadSize, ChunkCacheDynamicReadSize)
	}

	if compress != "" {
		r.compress = Compress{
			Enable:      true,
			CompressBuf: make([]byte, ChunkCacheFixedSize, ChunkCacheFixedSize),
		}
		if !isFixed {
			r.compress.CompressBuf = make([]byte, ChunkCacheDynamicReadSize, ChunkCacheDynamicReadSize)
		}
		switch compress {
		case "zstd":
			r.compress.Compress = &zstd.ZStandard{}
		case "lz4":
			r.compress.Compress = &lz4.LZ4{}
		case "snappy":
			r.compress.Compress = &snappy.Snappy{}
		}
	}
	return r
}

func (c *ChunkReader) ReadAt(p []byte, offset int64) (n int, err error) {

	if c.ErrState {
		return 0, fmt.Errorf("read is in error state")
	}

	if offset == 0 {
		if err := c.doInit(); err != nil {
			klog.Errorf("ReadAt Do Init error %v", err)
			c.ErrState = true
			return 0, fmt.Errorf("read is in error state")
		}
	}

	// Cache hit
	if offset >= c.c.start && offset+int64(len(p)) <= c.c.offset {
		st := offset - c.c.start
		copy(p, c.c.buf[st:st+int64(len(p))])
		return len(p), nil
	}
	if offset >= c.ec.start && offset+int64(len(p)) <= c.ec.offset {
		st := offset - c.ec.start
		copy(p, c.ec.buf[st:st+int64(len(p))])
		return len(p), nil
	}

	// Cache Miss
	first, second := c.determinePack(offset, int64(len(p)))
	if first == -1 {
		klog.Errorf("can not find appropriate pack id")
		c.ErrState = true
		return 0, fmt.Errorf("read is in error state")
	}
	// Once
	if err = c.doDownload(first, c.c.buf); err != nil {
		c.ErrState = true
		return 0, err
	}
	c.c.start, c.c.offset = c.doBound(first)
	if second == -1 {
		st := offset - c.c.start
		copy(p, c.c.buf[st:st+int64(len(p))])
		return len(p), nil
	}
	// Twice
	if err = c.doDownload(second, c.ec.buf); err != nil {
		c.ErrState = true
		return 0, err
	}
	c.ec.start, c.ec.offset = c.doBound(second)
	copy(p[:c.c.offset-offset], c.c.buf[offset-c.c.start:])
	copy(p[c.c.offset-offset:], c.ec.buf)
	return len(p), nil
}

func (c *ChunkReader) determinePack(off int64, l int64) (first, second int) {
	first, second = -1, -1
	length := len(c.ChunkMetas)
	for _, v := range c.ChunkMetas {
		if v.Start >= v.End {
			continue
		}
		if off >= v.End || off < v.Start {
			continue
		}
		if off >= v.Start {
			if off+l <= v.End {
				first = v.Index
				break
			}
			first, second = v.Index, v.Index+1
		}
	}

	if first >= 0 && first < length {
		if c.ChunkMetas[first].Start >= c.ChunkMetas[first].End {
			first = -1
		}
	}
	if second >= 0 && second < length {
		if c.ChunkMetas[second].Start >= c.ChunkMetas[second].End {
			second = -1
		}
	}
	return
}

func (c *ChunkReader) doBound(inx int) (start, end int64) {
	for _, v := range c.ChunkMetas {
		if v.Index == inx {
			start = v.Start
			end = v.End
			return
		}
	}
	return
}

func (c *ChunkReader) doInit() error {
	buf := make([]byte, 1<<20)
	n, err := c.fs.Backend.Get(c.key+"/.meta", 0, -1, buf)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(buf[:n], &c.ChunkMetas); err != nil {
		return err
	}
	return nil
}

func (c *ChunkReader) doDownload(index int, buf []byte) error {
	if !c.compress.Enable {
		if _, err := c.fs.Backend.Get(c.key+"/"+strconv.Itoa(index), 0, -1, buf); err != nil {
			return err
		}
		return nil
	}
	// Decompress
	if c.ChunkMetas[index].CompressSize > int64(len(c.compress.CompressBuf)) {
		c.compress.CompressBuf = make([]byte, c.ChunkMetas[index].CompressSize, c.ChunkMetas[index].CompressSize)
	}
	if _, err := c.fs.Backend.Get(c.key+"/"+strconv.Itoa(index), 0, -1, c.compress.CompressBuf[:c.ChunkMetas[index].CompressSize]); err != nil {
		return err
	}
	if _, err := c.compress.Compress.Decompress(buf, c.compress.CompressBuf[:c.ChunkMetas[index].CompressSize]); err != nil {
		return err
	}
	return nil
}

func (c *ChunkReader) Release() {
	c.c.start = 0
	c.c.offset = 0
	c.ec.start = 0
	c.ec.offset = 0
	c.ErrState = false
}
