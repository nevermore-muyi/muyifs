package fuse

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"sync"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	_ "bazil.org/fuse/fs/fstestutil"
	"github.com/nevermore/muyifs/pkg/backend"
	"github.com/nevermore/muyifs/pkg/backend/obs"
	"github.com/nevermore/muyifs/pkg/backend/s3"
	"k8s.io/klog/v2"
)

type FileSystem struct {
	sync.Mutex
	root     fs.Node
	option   *Option
	compress string
	chunk    bool
	isFixed  bool
	handler  map[uint64]*FileHandle
	Backend  backend.ObjectStorage
	Server   *fs.Server
}

type Option struct {
	Backend   string
	Bucket    string
	Region    string
	AccessKey string
	SerectKey string
	Endpoint  string
}

func NewFileSystem(mountpoint, datapath, compress string, chunk, isFixed bool, option *Option) *FileSystem {
	// root, err := reloadData(mountpoint, datapath)
	// if err != nil {
	// 	klog.Fatalf("Reload Metadata error %v", err)
	// }
	ts := time.Now()
	root := &Dir{
		id:     1,
		parent: nil,
		name:   mountpoint,
		typ:    fuse.DT_Dir,
		attr: &fuse.Attr{
			Valid:     time.Second,
			Inode:     1,
			Size:      4 << 10,
			Blocks:    0,
			Atime:     ts,
			Mtime:     ts,
			Ctime:     ts,
			Mode:      os.FileMode(0750),
			Nlink:     2,
			Uid:       0,
			Gid:       0,
			Rdev:      0,
			BlockSize: 512,
		},
	}
	f := &FileSystem{
		root:     root,
		compress: compress,
		chunk:    chunk,
		isFixed:  isFixed,
		option:   option,
		handler:  make(map[uint64]*FileHandle),
	}
	root.fs = f
	return f
}

func (fs *FileSystem) Root() (fs.Node, error) {
	klog.Infof("FS Root")
	return fs.root, nil
}

func reloadData(mountpoint, datapath string) (*Dir, error) {
	_, err := os.Stat(datapath)
	if err != nil && os.IsNotExist(err) {
		ts := time.Now()
		d := &Dir{
			id:     1,
			parent: nil,
			name:   mountpoint,
			typ:    fuse.DT_Dir,
			attr: &fuse.Attr{
				Valid:     time.Second,
				Inode:     1,
				Size:      4 << 10,
				Blocks:    0,
				Atime:     ts,
				Mtime:     ts,
				Ctime:     ts,
				Mode:      os.FileMode(0750),
				Nlink:     2,
				Uid:       0,
				Gid:       0,
				Rdev:      0,
				BlockSize: 512,
			},
		}
		b, err := json.Marshal(d)
		if err != nil {
			return nil, err
		}
		if err = os.WriteFile(datapath, b, 0500); err != nil {
			return nil, err
		}
		return d, nil
	}
	f, err := os.Open(datapath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	content, err := ioutil.ReadAll(f)
	var d Dir
	err = json.Unmarshal(content, &d)
	if err != nil {
		return nil, err
	}
	return &d, nil
}

func Mount(mountpoint, datapath, compress string, chunk, isFixed bool, options *Option) {

	muyifs := NewFileSystem(mountpoint, datapath, compress, chunk, isFixed, options)
	switch options.Backend {
	case "obs":
		client, err := obs.NewObsClient(options.Bucket, options.Region, options.AccessKey, options.SerectKey, options.Endpoint)
		if err != nil {
			klog.Fatalf("Init OBS backend client error %v", err)
		}
		if err = client.Create(); err != nil {
			klog.Fatalf("Create OBS backend bucket error %v", err)
		}
		muyifs.Backend = client
	case "s3":
		client, err := s3.NewS3Client(options.Bucket, options.Region, options.AccessKey, options.SerectKey, options.Endpoint)
		if err != nil {
			klog.Fatalf("Init S3 backend client error %v", err)
		}
		if err = client.Create(); err != nil {
			klog.Fatalf("Create S3 backend bucket error %v", err)
		}
		muyifs.Backend = client
	default:
		klog.Fatalf("Unknown Backend %s", options.Backend)
	}

	opt := []fuse.MountOption{
		fuse.FSName("muyifs"),
		fuse.Subtype("muyifs"),
		fuse.DaemonTimeout("3600"),
		fuse.AllowSUID(),
		fuse.DefaultPermissions(),
		fuse.MaxReadahead(1024 * 128),
		fuse.AsyncRead(),
		fuse.WritebackCache(),
		fuse.MaxBackground(128),
		fuse.CongestionThreshold(128),
	}
	c, err := fuse.Mount(mountpoint, opt...)
	if err != nil {
		klog.V(0).Infof("mount: %v", err)
		return
	}
	defer c.Close()

	var conf fs.Config
	conf.Debug = func(msg interface{}) {
		// klog.V(0).Infof("FUSE: %s", msg)
	}
	s := fs.New(c, &conf)
	err = s.Serve(muyifs)
	if err != nil {
		klog.V(0).Infof("mount process: %v", err)
	}
}
