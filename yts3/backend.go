package yts3

import "io"

const (
	DefaultBucketVersionKeys = 1000
)

type Object struct {
	Name           string
	Metadata       map[string]string
	Size           int64
	Contents       io.ReadCloser
	Hash           []byte
	Range          *ObjectRange
	VersionID      VersionID
	IsDeleteMarker bool
}

type ObjectList struct {
	CommonPrefixes []CommonPrefix
	Contents       []*Content
	IsTruncated    bool
	NextMarker     string
	prefixes       map[string]bool
}

type PutObjectResult struct {
	VersionID VersionID
}

type ListBucketPage struct {
	Marker    string
	HasMarker bool

	MaxKeys int64
}

func (p ListBucketPage) IsEmpty() bool {
	return p == ListBucketPage{}
}

type Backend interface {
	ListBuckets(publicKey string) ([]BucketInfo, error)
	ListBucket(publicKey, name string, prefix *Prefix, page ListBucketPage) (*ObjectList, error)
	CreateBucket(publicKey, name string) error
	BucketExists(name string) (exists bool, err error)
	DeleteMulti(publicKey, bucketName string, objects ...string) (MultiDeleteResult, error)
	PutObject(publicKey, bucketName, key string, meta map[string]string, input io.Reader, size int64) (PutObjectResult, error)
	// PutBigObject(publicKey, bucketName, key, etag string, meta map[string]string, size int64) (PutObjectResult, error)
	GetObject(publicKey, bucketName, objectName string, rangeRequest *ObjectRangeRequest) (*Object, error)
	DeleteBucket(publicKey, name string) error
	HeadObject(publicKey, bucketName, objectName string) (*Object, error)
	DeleteObject(publicKey, bucketName, objectName string) (ObjectDeleteResult, error)
}

type VersionedBackend interface{}

type ObjectDeleteResult struct {
	// Specifies whether the versioned object that was permanently deleted was
	// (true) or was not (false) a delete marker. In a simple DELETE, this
	// header indicates whether (true) or not (false) a delete marker was
	// created.
	IsDeleteMarker bool

	// Returns the version ID of the delete marker created as a result of the
	// DELETE operation. If you delete a specific object version, the value
	// returned by this header is the version ID of the object version deleted.
	VersionID VersionID
}

func NewObjectList() *ObjectList {
	return &ObjectList{}
}

func (b *ObjectList) AddPrefix(prefix string) {
	if b.prefixes == nil {
		b.prefixes = map[string]bool{}
	} else if b.prefixes[prefix] {
		return
	}
	b.prefixes[prefix] = true
	b.CommonPrefixes = append(b.CommonPrefixes, CommonPrefix{Prefix: prefix})
}

func (b *ObjectList) Add(item *Content) {
	b.Contents = append(b.Contents, item)
}
