package s3

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/nevermore/muyifs/pkg/backend"
	"k8s.io/klog/v2"
)

type s3Client struct {
	bucket  string
	s3      *s3.S3
	session *session.Session
}

func NewS3Client(bucket, region string, ak, sk, endpoint string) (*s3Client, error) {
	awsConfig := &aws.Config{}
	awsConfig.Region = aws.String(region)
	awsConfig.DisableSSL = aws.Bool(!strings.Contains(endpoint, "https"))
	awsConfig.Credentials = credentials.NewStaticCredentials(ak, sk, "")
	awsConfig.Endpoint = aws.String(endpoint)
	awsConfig.S3ForcePathStyle = aws.Bool(true)

	ses, err := session.NewSession(awsConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create aws session: %s", err)
	}
	return &s3Client{
		bucket:  bucket,
		s3:      s3.New(ses),
		session: ses,
	}, nil
}

func (s *s3Client) String() string {
	return fmt.Sprintf("s3://%s", s.bucket)
}

func (s *s3Client) Create() error {
	params := &s3.CreateBucketInput{}
	params.Bucket = &s.bucket
	if _, err := s.s3.CreateBucket(params); err != nil {
		klog.Errorf("Create S3 Bucket error %v", err)
		return err
	}
	return nil
}

func (s *s3Client) Head(key string) (backend.Object, error) {
	params := &s3.HeadObjectInput{}
	params.Bucket = &s.bucket
	params.Key = &key
	obj, err := s.s3.HeadObject(params)
	if err != nil {
		klog.Errorf("Head S3 %v Metadata error %v", key, err)
		return backend.Object{}, err
	}
	m := make(map[string]string)
	for k, v := range obj.Metadata {
		m[k] = *v
	}
	return backend.Object{
		Key:      key,
		Size:     *obj.ContentLength,
		Mtime:    *obj.LastModified,
		IsDir:    strings.HasSuffix(key, "/"),
		Metadata: m,
	}, nil
}

func (s *s3Client) Get(key string, off, limit int64, buf []byte) (int, error) {
	params := &s3.GetObjectInput{}
	params.Bucket = &s.bucket
	params.Key = &key
	if off > 0 || limit > 0 {
		var r string
		if limit > 0 {
			r = fmt.Sprintf("bytes=%d-%d", off, off+limit-1)
		} else {
			r = fmt.Sprintf("bytes=%d-", off)
		}
		params.Range = &r
	}
	output, err := s.s3.GetObject(params)
	if err != nil && err != io.EOF {
		klog.Errorf("Get S3 %v Object error %v", key, err)
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

func (s *s3Client) Put(key string, metadata map[string]string, in io.Reader) error {
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
	m := make(map[string]*string)
	for k, v := range metadata {
		m[k] = &v
	}
	params := &s3.PutObjectInput{}
	params.Bucket = &s.bucket
	params.Key = &key
	params.Body = body
	params.Metadata = m
	_, err := s.s3.PutObject(params)
	if err != nil {
		klog.Errorf("Put S3 %v Object error %v", key, err)
	}
	return err
}

func (s *s3Client) PutDirectory(key string) error {
	params := &s3.PutObjectInput{}
	params.Bucket = &s.bucket
	params.Key = &key
	_, err := s.s3.PutObject(params)
	if err != nil {
		klog.Errorf("PutDirectory S3 %v Object error %v", key, err)
	}
	return err
}

func (s *s3Client) Delete(key string) error {
	params := &s3.DeleteObjectInput{}
	params.Bucket = &s.bucket
	params.Key = &key
	_, err := s.s3.DeleteObject(params)
	return err
}

func (s *s3Client) DeleteList(key string) error {
	objects, err := s.List(key)
	if err != nil {
		return err
	}
	o := make([]*s3.ObjectIdentifier, len(objects))
	for i := range objects {
		o[i] = &s3.ObjectIdentifier{
			Key: &objects[i].Key,
		}
	}
	params := &s3.DeleteObjectsInput{}
	params.Bucket = &s.bucket
	params.Delete = &s3.Delete{
		Objects: o,
	}
	_, err = s.s3.DeleteObjects(params)
	return err
}

func (s *s3Client) List(prefix string) ([]backend.Object, error) {
	var maxKeys int64 = 1000
	params := &s3.ListObjectsInput{}
	params.Bucket = &s.bucket
	params.Prefix = &prefix
	params.MaxKeys = &maxKeys

	var obj []backend.Object
	for {
		output, err := s.s3.ListObjects(params)
		if err != nil {
			klog.Errorf("List S3 Object error %v", err)
			return obj, err
		}
		for _, o := range output.Contents {
			obj = append(obj, backend.Object{
				Key:   *o.Key,
				Size:  *o.Size,
				Mtime: *o.LastModified,
				IsDir: strings.HasSuffix(*o.Key, "/"),
			})
		}
		isTruncated := output.IsTruncated
		if isTruncated != nil && !(*isTruncated) {
			break
		}
		params.Marker = output.NextMarker
	}
	return obj, nil
}

func (s *s3Client) InitiateMultipartUpload(key string) (*backend.MultipartUpload, error) {
	params := &s3.CreateMultipartUploadInput{}
	params.Bucket = &s.bucket
	params.Key = &key
	output, err := s.s3.CreateMultipartUpload(params)
	if err != nil {
		klog.Errorf("InitiateMultipartUpload S3 %v Object error %v", key, err)
		return nil, err
	}
	return &backend.MultipartUpload{
		MinPartSize: 5 << 20,
		MaxCount:    10000,
		UploadID:    *output.UploadId,
	}, nil
}

func (s *s3Client) UploadPart(key string, uploadID string, num int, body []byte) (*backend.Part, error) {
	n := int64(num)
	params := &s3.UploadPartInput{}
	params.Bucket = &s.bucket
	params.Key = &key
	params.UploadId = &uploadID
	params.PartNumber = &n
	params.Body = bytes.NewReader(body)
	output, err := s.s3.UploadPart(params)
	if err != nil {
		klog.Errorf("UploadPart S3 %v Object error %v", key, err)
		return nil, err
	}
	return &backend.Part{
		Num:  num,
		ETag: *output.ETag,
	}, nil
}

func (s *s3Client) AbortUpload(key string, uploadID string) error {
	params := &s3.AbortMultipartUploadInput{}
	params.Bucket = &s.bucket
	params.Key = &key
	params.UploadId = &uploadID
	_, err := s.s3.AbortMultipartUpload(params)
	if err != nil {
		klog.Errorf("AbortUpload S3 %v Object error %v", key, err)
		return err
	}
	return nil
}

func (s *s3Client) CompleteUpload(key string, uploadID string, parts []*backend.Part) error {
	params := &s3.CompleteMultipartUploadInput{}
	params.Bucket = &s.bucket
	params.Key = &key
	params.UploadId = &uploadID
	params.MultipartUpload = &s3.CompletedMultipartUpload{}
	for _, p := range parts {
		n := int64(p.Num)
		params.MultipartUpload.Parts = append(params.MultipartUpload.Parts, &s3.CompletedPart{
			PartNumber: &n,
			ETag:       &p.ETag,
		})
	}
	_, err := s.s3.CompleteMultipartUpload(params)
	if err != nil {
		klog.Errorf("CompleteUpload S3 %v Object error %v", key, err)
		return err
	}
	return nil
}
