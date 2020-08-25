package yts3

import (
	"math/big"
	"sync"
	"time"

	"github.com/ryszard/goskiplist/skiplist"
)

var add1 = new(big.Int).SetInt64(1)

type bucketUploads struct {
	uploads map[UploadID]*multipartUpload

	objectIndex *skiplist.SkipList
}
type multipartUpload struct {
	ID        UploadID
	Bucket    string
	Object    string
	Meta      map[string]string
	Initiated time.Time
	parts     []*multipartUploadPart
	mu        sync.Mutex
}

type multipartUploadPart struct {
	PartNumber   int
	ETag         string
	Body         []byte
	LastModified ContentTime
}
type uploader struct {
	// uploadIDs use a big.Int to allow unbounded IDs (not that you'd be
	// expected to ever generate 4.2 billion of these but who are we to judge?)
	uploadID *big.Int

	buckets map[string]*bucketUploads
	mu      sync.Mutex
}

func newUploader() *uploader {
	return &uploader{
		buckets:  make(map[string]*bucketUploads),
		uploadID: new(big.Int),
	}
}
