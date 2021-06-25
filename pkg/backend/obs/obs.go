package obs

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"strings"

	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
	"github.com/nevermore/muyifs/pkg/backend"
	"k8s.io/klog/v2"
)

type obsClient struct {
	bucket string
	region string
	c      *obs.ObsClient
}

func NewObsClient(bucket, region string, ak, sk, endpoint string) (*obsClient, error) {
	c, err := obs.New(ak, sk, endpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize OBS: %v", err)
	}
	return &obsClient{
		bucket: bucket,
		region: region,
		c:      c,
	}, nil
}

func (s *obsClient) String() string {
	return fmt.Sprintf("obs://%s", s.bucket)
}

func (s *obsClient) Create() error {
	params := &obs.CreateBucketInput{}
	params.Bucket = s.bucket
	params.Location = s.region
	if _, err := s.c.CreateBucket(params); err != nil {
		klog.Errorf("Create OBS Bucket error %v", err)
		return err
	}
	return nil
}

func (s *obsClient) Head(key string) (backend.Object, error) {
	params := &obs.GetObjectMetadataInput{}
	params.Bucket = s.bucket
	params.Key = key
	obj, err := s.c.GetObjectMetadata(params)
	if err != nil {
		klog.Errorf("Head OBS %v Metadata error %v", key, err)
		return backend.Object{}, err
	}
	return backend.Object{
		Key:      key,
		Size:     obj.ContentLength,
		Mtime:    obj.LastModified,
		IsDir:    strings.HasSuffix(key, "/"),
		Metadata: obj.Metadata,
	}, nil
}

func (s *obsClient) Get(key string, off, limit int64, buf []byte) (int, error) {
	params := &obs.GetObjectInput{}
	params.Bucket = s.bucket
	params.Key = key
	params.RangeStart = off
	if limit > 0 {
		params.RangeEnd = off + limit - 1
	}
	output, err := s.c.GetObject(params)
	if err != nil && err != io.EOF {
		klog.Errorf("Get OBS %v Object error %v", key, err)
		return 0, err
	}
	defer output.Body.Close()
	inx := 0
	for {
		count, err := output.Body.Read(buf[inx:])
		if err != nil {
			if err == io.EOF {
				return inx + count, nil
			}
			klog.Errorf("Read OBS %v Object error %v", key, err)
			return 0, err
		}
		inx += count
	}
}

func (s *obsClient) Put(key string, metadata map[string]string, in io.Reader) error {
	var body io.ReadSeeker
	if b, ok := in.(io.ReadSeeker); ok {
		body = b
	} else {
		data, err := ioutil.ReadAll(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(data)
	}
	params := &obs.PutObjectInput{}
	params.Bucket = s.bucket
	params.Key = key
	params.Body = body
	params.Metadata = metadata
	_, err := s.c.PutObject(params)
	if err != nil {
		klog.Errorf("Put OBS %v Object error %v", key, err)
	}
	return err
}

func (s *obsClient) PutDirectory(key string) error {
	params := &obs.PutObjectInput{}
	params.Bucket = s.bucket
	params.Key = key
	_, err := s.c.PutObject(params)
	if err != nil {
		klog.Errorf("PutDirectory OBS %v Object error %v", key, err)
	}
	return err
}

func (s *obsClient) Delete(key string) error {
	params := &obs.DeleteObjectInput{}
	params.Bucket = s.bucket
	params.Key = key
	_, err := s.c.DeleteObject(params)
	return err
}

func (s *obsClient) DeleteList(key string) error {
	objects, err := s.List(key)
	if err != nil {
		return err
	}

	toDelete := make([]obs.ObjectToDelete, len(objects))
	for i := range objects {
		toDelete[i].Key = objects[i].Key
	}
	params := &obs.DeleteObjectsInput{}
	params.Bucket = s.bucket
	params.Objects = toDelete
	_, err = s.c.DeleteObjects(params)
	return err
}

func (s *obsClient) List(prefix string) ([]backend.Object, error) {
	params := &obs.ListObjectsInput{}
	params.Bucket = s.bucket
	params.Prefix = prefix
	params.MaxKeys = 1000

	var obj []backend.Object
	for {
		output, err := s.c.ListObjects(params)
		if err != nil {
			klog.Errorf("List OBS Object error %v", err)
			return obj, err
		}
		for _, o := range output.Contents {
			obj = append(obj, backend.Object{
				Key:   o.Key,
				Size:  o.Size,
				Mtime: o.LastModified,
				IsDir: strings.HasSuffix(o.Key, "/"),
			})
		}
		if !output.IsTruncated {
			break
		}
		params.Marker = output.NextMarker
	}
	return obj, nil
}

func (s *obsClient) InitiateMultipartUpload(key string) (*backend.MultipartUpload, error) {
	params := &obs.InitiateMultipartUploadInput{}
	params.Bucket = s.bucket
	params.Key = key
	output, err := s.c.InitiateMultipartUpload(params)
	if err != nil {
		klog.Errorf("InitiateMultipartUpload OBS %v Object error %v", key, err)
		return nil, err
	}
	return &backend.MultipartUpload{
		MinPartSize: 5 << 20,
		MaxCount:    10000,
		UploadID:    output.UploadId,
	}, nil
}

func (s *obsClient) UploadPart(key string, uploadID string, num int, body []byte) (*backend.Part, error) {
	params := &obs.UploadPartInput{}
	params.Bucket = s.bucket
	params.Key = key
	params.UploadId = uploadID
	params.PartNumber = num
	params.Body = bytes.NewReader(body)
	output, err := s.c.UploadPart(params)
	if err != nil {
		klog.Errorf("UploadPart OBS %v Object error %v", key, err)
		return nil, err
	}
	return &backend.Part{
		Num:  num,
		ETag: output.ETag,
	}, nil
}

func (s *obsClient) AbortUpload(key string, uploadID string) error {
	params := &obs.AbortMultipartUploadInput{}
	params.Bucket = s.bucket
	params.Key = key
	params.UploadId = uploadID
	_, err := s.c.AbortMultipartUpload(params)
	if err != nil {
		klog.Errorf("AbortUpload OBS %v Object error %v", key, err)
		return err
	}
	return nil
}

func (s *obsClient) CompleteUpload(key string, uploadID string, parts []*backend.Part) error {
	params := &obs.CompleteMultipartUploadInput{}
	params.Bucket = s.bucket
	params.Key = key
	params.UploadId = uploadID
	for i := range parts {
		params.Parts = append(params.Parts, obs.Part{ETag: parts[i].ETag, PartNumber: parts[i].Num})
	}
	_, err := s.c.CompleteMultipartUpload(params)
	if err != nil {
		klog.Errorf("CompleteUpload OBS %v Object error %v", key, err)
		return err
	}
	return nil
}
