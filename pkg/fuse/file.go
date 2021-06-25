package fuse

import (
	"context"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
)

type File struct {
	id     uint64
	parent *Dir
	name   string
	typ    fuse.DirentType
	attr   *fuse.Attr
	fs     *FileSystem
}

func (f *File) Attr(ctx context.Context, attr *fuse.Attr) (err error) {
	attr.Inode = f.attr.Inode
	attr.Mode = f.attr.Mode
	attr.Atime = f.attr.Atime
	attr.Mtime = f.attr.Mtime
	attr.Ctime = f.attr.Ctime
	attr.Size = f.attr.Size
	attr.Nlink = f.attr.Nlink
	attr.BlockSize = f.attr.BlockSize
	return nil
}

func (f *File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	f.fs.Lock()
	h := f.fs.handler[f.id]
	f.fs.Unlock()
	resp.Handle = fuse.HandleID(f.id)
	return h, nil
}

func (f *File) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	if req.Valid.Size() {
		f.attr.Size = req.Size
	}
	if req.Valid.Mode() {
		f.attr.Mode = req.Mode
	}
	if req.Valid.Uid() {
		f.attr.Uid = req.Uid
	}
	if req.Valid.Gid() {
		f.attr.Gid = req.Gid
	}
	if req.Valid.Atime() {
		f.attr.Atime = req.Atime
	}
	if req.Valid.Mtime() {
		f.attr.Mtime = req.Mtime
	}
	resp.Attr.Size = f.attr.Size
	resp.Attr.Mode = f.attr.Mode
	resp.Attr.Uid = f.attr.Uid
	resp.Attr.Gid = f.attr.Gid
	resp.Attr.Atime = f.attr.Atime
	resp.Attr.Mtime = f.attr.Mtime
	return nil
}
