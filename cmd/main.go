package main

import (
	"flag"

	"github.com/nevermore/muyifs/pkg/fuse"
)

func main() {
	var mountpoint, datapath, compress string
	var chunk, fixed bool
	var opt fuse.Option
	flag.StringVar(&mountpoint, "mountpoint", "", "dir for mount use")
	flag.StringVar(&datapath, "datapath", "", "data path to store metadata")
	flag.StringVar(&compress, "compress", "", "compression algorithm for data uploading (snappy/lz4/zstd)")
	flag.BoolVar(&chunk, "chunk", false, "whether to split data into chunks")
	flag.BoolVar(&fixed, "fixed", true, "whether to split data in fixed size")
	flag.StringVar(&opt.Backend, "backend", "s3", "type of object storage (s3 or obs)")
	flag.StringVar(&opt.Bucket, "bucket", "", "bucket of object storage")
	flag.StringVar(&opt.Region, "region", "", "region of object storage")
	flag.StringVar(&opt.AccessKey, "ak", "", "access key of object storage")
	flag.StringVar(&opt.SerectKey, "sk", "", "secret key of object storage")
	flag.StringVar(&opt.Endpoint, "endpoint", "", "endpoint of object storage")
	flag.Parse()
	fuse.Mount(mountpoint, datapath, compress, chunk, fixed, &opt)
}
