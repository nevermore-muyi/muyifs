package fuse

import (
	"bytes"
	"context"
	"os"
	"syscall"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"k8s.io/klog/v2"
)

type Dir struct {
	id        uint64
	name      string
	typ       fuse.DirentType
	attr      *fuse.Attr
	parent    *Dir
	DirChild  []*Dir
	FileChild []*File
	fs        *FileSystem
}

func (d *Dir) String() string {
	s := ""
	t := d
	for t != nil {
		if t.id == 1 {
			break
		}
		s = t.name + "/" + s
		t = t.parent
	}
	return s
}

func (d *Dir) Attr(ctx context.Context, attr *fuse.Attr) error {
	attr.Inode = d.attr.Inode
	attr.Mode = os.ModeDir | d.attr.Mode
	attr.Atime = d.attr.Atime
	attr.Mtime = d.attr.Mtime
	attr.Ctime = d.attr.Ctime
	attr.Size = d.attr.Size
	attr.Nlink = d.attr.Nlink
	attr.BlockSize = d.attr.BlockSize
	return nil
}

func (d *Dir) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	if req.Valid.Size() {
		d.attr.Size = req.Size
	}
	if req.Valid.Mode() {
		d.attr.Mode = req.Mode
	}
	if req.Valid.Uid() {
		d.attr.Uid = req.Uid
	}
	if req.Valid.Gid() {
		d.attr.Gid = req.Gid
	}
	if req.Valid.Atime() {
		d.attr.Atime = req.Atime
	}
	if req.Valid.Mtime() {
		d.attr.Mtime = req.Mtime
	}
	resp.Attr.Size = d.attr.Size
	resp.Attr.Mode = d.attr.Mode
	resp.Attr.Uid = d.attr.Uid
	resp.Attr.Gid = d.attr.Gid
	resp.Attr.Atime = d.attr.Atime
	resp.Attr.Mtime = d.attr.Mtime
	return nil
}

func (d *Dir) Lookup(ctx context.Context, req *fuse.LookupRequest, resp *fuse.LookupResponse) (fs.Node, error) {
	for _, dd := range d.DirChild {
		if dd.name == req.Name {
			resp.Node = fuse.NodeID(dd.id)
			resp.Attr.Inode = dd.id
			resp.Attr.Valid = dd.attr.Valid
			resp.Attr.Size = dd.attr.Size
			resp.Attr.Mtime = dd.attr.Mtime
			resp.Attr.Ctime = dd.attr.Ctime
			resp.Attr.Mode = os.ModeDir | dd.attr.Mode
			resp.Attr.Gid = dd.attr.Gid
			resp.Attr.Uid = dd.attr.Uid
			resp.Attr.Nlink = dd.attr.Nlink
			return dd, nil
		}
	}
	for _, dd := range d.FileChild {
		if dd.name == req.Name {
			resp.Node = fuse.NodeID(dd.id)
			resp.Attr.Inode = dd.id
			resp.Attr.Valid = dd.attr.Valid
			resp.Attr.Size = dd.attr.Size
			resp.Attr.Mtime = dd.attr.Mtime
			resp.Attr.Ctime = dd.attr.Ctime
			resp.Attr.Mode = dd.attr.Mode
			resp.Attr.Gid = dd.attr.Gid
			resp.Attr.Uid = dd.attr.Uid
			resp.Attr.Nlink = dd.attr.Nlink
			return dd, nil
		}
	}
	return nil, fuse.Errno(syscall.ENOENT)
}

func (d *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	var dirent []fuse.Dirent
	for _, dd := range d.DirChild {
		dirent = append(dirent, fuse.Dirent{
			Inode: dd.id,
			Type:  dd.typ,
			Name:  dd.name,
		})
	}
	for _, dd := range d.FileChild {
		dirent = append(dirent, fuse.Dirent{
			Inode: dd.id,
			Type:  dd.typ,
			Name:  dd.name,
		})
	}
	return dirent, nil
}

func (d *Dir) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	newInode := GenerateInode()
	ts := time.Now()
	dir := &Dir{
		id:     newInode,
		parent: d,
		name:   req.Name,
		typ:    fuse.DT_Dir,
		fs:     d.fs,
		attr: &fuse.Attr{
			Valid:     time.Second,
			Inode:     newInode,
			Size:      4 << 10,
			Atime:     ts,
			Mtime:     ts,
			Ctime:     ts,
			Mode:      os.ModeDir | req.Mode,
			Nlink:     2,
			Uid:       0,
			Gid:       0,
			Rdev:      0,
			BlockSize: 512,
		},
	}
	if err := d.fs.Backend.PutDirectory(d.String() + req.Name + "/"); err != nil {
		klog.Error("Mkdir and put directory %v error %v", req.Name, err)
		return nil, err
	}
	d.DirChild = append(d.DirChild, dir)
	return dir, nil
}

func (d *Dir) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	newInode := GenerateInode()
	ts := time.Now()
	f := &File{
		id:     newInode,
		parent: d,
		name:   req.Name,
		typ:    fuse.DT_File,
		fs:     d.fs,
		attr: &fuse.Attr{
			Valid: time.Second,
			Inode: newInode,
			Size:  0,
			Atime: ts,
			Mtime: ts,
			Ctime: ts,
			Mode:  req.Mode,
			Uid:   req.Uid,
			Gid:   req.Gid,
		},
	}

	h := NewFileHandle(f, req.Uid, req.Gid)
	h.ID = newInode

	if d.fs.chunk {
		h.reader = NewChunkReader(d.String()+req.Name, d.fs, d.fs.compress, d.fs.isFixed)
		h.writer = NewChunkWriter(d.String()+req.Name, d.fs, d.fs.compress, d.fs.isFixed)
	} else {
		h.reader = NewReader(d.String()+req.Name, d.fs)
		h.writer = NewWriter(d.String()+req.Name, d.fs)
	}
	if err := d.fs.Backend.Put(d.String()+req.Name, map[string]string{}, bytes.NewReader([]byte{})); err != nil {
		klog.Error("Create and put file %v error %v", req.Name, err)
		return nil, nil, err
	}

	d.FileChild = append(d.FileChild, f)
	d.fs.Lock()
	d.fs.handler[newInode] = h
	d.fs.Unlock()
	return f, h, nil
}

func (d *Dir) Forget() {
	klog.Infof("Forget dir %s", d.name)
}

func (d *Dir) Link(ctx context.Context, req *fuse.LinkRequest, old fs.Node) (fs.Node, error) {
	klog.Infof("Dir Link")
	return nil, nil
}

func (d *Dir) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	if req.Dir {
		return d.removeDir(req)
	}
	return d.removeFile(req)
}

func (d *Dir) removeDir(req *fuse.RemoveRequest) error {
	for i, f := range d.DirChild {
		if f.name == req.Name {
			if err := d.fs.Backend.Delete(d.String() + req.Name + "/"); err != nil {
				return err
			}
			d.DirChild = append(d.DirChild[:i], d.DirChild[i+1:]...)
			return nil
		}
	}
	return nil
}

func (d *Dir) removeFile(req *fuse.RemoveRequest) error {
	for i, f := range d.FileChild {
		if f.name == req.Name {
			if err := d.fs.Backend.DeleteList(d.String() + req.Name + "/"); err != nil {
				return err
			}
			if err := d.fs.Backend.Delete(d.String() + req.Name); err != nil {
				return err
			}
			d.FileChild = append(d.FileChild[:i], d.FileChild[i+1:]...)
			return nil
		}
	}
	return nil
}
