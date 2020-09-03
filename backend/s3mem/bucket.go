package s3mem

import (
	"bytes"
	"io"
	"time"

	"github.com/ryszard/goskiplist/skiplist"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/api"
	"github.com/yottachain/YTS3/internal/s3io"
	"github.com/yottachain/YTS3/yts3"
)

type versionGenFunc func() yts3.VersionID

type versioningStatus int

type bucket struct {
	name         string
	versioning   yts3.VersioningStatus
	versionGen   versionGenFunc
	creationDate yts3.ContentTime

	objects *skiplist.SkipList
}

func newBucket(publicKey, bucketName string, at time.Time, versionGen versionGenFunc) *bucket {
	c := api.GetClient(publicKey)
	bucketAccessor := c.NewBucketAccessor()
	var header map[string]string
	header = make(map[string]string)

	header["version_status"] = "Enabled"
	meta, err := api.BucketMetaMapToBytes(header)
	if err != nil {
		logrus.Errorf("[CreateBucket meta map to bytes] ERR:%s\n", err)
	}
	err2 := bucketAccessor.CreateBucket(bucketName, meta)
	if err2 != nil {
		logrus.Error(err2)
	}
	return &bucket{
		name:         bucketName,
		creationDate: yts3.NewContentTime(at),
		versionGen:   versionGen,
		objects:      skiplist.NewStringMap(),
	}
}

type bucketObject struct {
	name     string
	data     *bucketData
	versions *skiplist.SkipList
}

func (b *bucketObject) Iterator() *bucketObjectIterator {
	var iter skiplist.Iterator
	if b.versions != nil {
		iter = b.versions.Iterator()
	}

	return &bucketObjectIterator{
		data: b.data,
		iter: iter,
	}
}

type bucketObjectIterator struct {
	data     *bucketData
	iter     skiplist.Iterator
	cur      *bucketData
	seenData bool
	done     bool
}

func (b *bucketObjectIterator) Seek(key yts3.VersionID) bool {
	if b.iter.Seek(key) {
		return true
	}

	b.iter = nil
	if b.data != nil && b.data.versionID == key {
		return true
	}

	b.data = nil
	b.done = true

	return false
}

func (b *bucketObjectIterator) Next() bool {
	if b.done {
		return false
	}

	if b.iter != nil {
		iterAlive := b.iter.Next()
		if iterAlive {
			b.cur = b.iter.Value().(*bucketData)
			return true
		}

		b.iter.Close()
		b.iter = nil
	}

	if b.data != nil {
		b.cur = b.data
		b.data = nil
		return true
	}

	b.done = true
	return false
}

func (b *bucketObjectIterator) Close() {
	if b.iter != nil {
		b.iter.Close()
	}
	b.done = true
}

func (b *bucketObjectIterator) Value() *bucketData {
	return b.cur
}

type bucketData struct {
	name         string
	lastModified time.Time
	versionID    yts3.VersionID
	deleteMarker bool
	body         []byte
	hash         []byte
	etag         string
	metadata     map[string]string
}

func (bi *bucketData) toObject(rangeRequest *yts3.ObjectRangeRequest, withBody bool) (obj *yts3.Object, err error) {
	sz := int64(len(bi.body))
	data := bi.body

	var contents io.ReadCloser
	var rnge *yts3.ObjectRange

	if withBody {
		// In case of a range request the correct part of the slice is extracted:
		rnge, err = rangeRequest.Range(sz)
		if err != nil {
			return nil, err
		}

		if rnge != nil {
			// data =
		}

		// The data slice should be completely replaced if the bucket item is edited, so
		// it should be safe to return the data slice directly.
		contents = s3io.ReaderWithDummyCloser{bytes.NewReader(data)}

	} else {
		contents = s3io.NoOpReadCloser{}
	}

	return &yts3.Object{
		Name:           bi.name,
		Hash:           bi.hash,
		Metadata:       bi.metadata,
		Size:           sz,
		Range:          rnge,
		IsDeleteMarker: bi.deleteMarker,
		VersionID:      bi.versionID,
		Contents:       contents,
	}, nil
}

func (b *bucket) setVersioning(enabled bool) {
	if enabled {
		b.versioning = yts3.VersioningEnabled
	} else if b.versioning == yts3.VersioningEnabled {
		b.versioning = yts3.VersioningSuspended
	}
}

func (b *bucket) object(objectName string) (obj *bucketObject) {
	objIface, _ := b.objects.Get(objectName)
	if objIface == nil {
		return nil
	}
	obj, _ = objIface.(*bucketObject)
	return obj
}

func (b *bucket) objectVersion(objectName string, versionID yts3.VersionID) (*bucketData, error) {
	obj := b.object(objectName)
	if obj == nil {
		return nil, yts3.KeyNotFound(objectName)
	}

	if obj.data != nil && obj.data.versionID == versionID {
		return obj.data, nil
	}
	if obj.versions == nil {
		return nil, yts3.ErrNoSuchVersion
	}
	versionIface, _ := obj.versions.Get(versionID)
	if versionIface == nil {
		return nil, yts3.ErrNoSuchVersion
	}

	return versionIface.(*bucketData), nil
}

func (b *bucket) put(publicKey, name string, item *bucketData) {
	item.versionID = b.versionGen()

	object := b.object(name)
	if object == nil {
		object = &bucketObject{name: name}
		b.objects.Set(name, object)
	}

	if b.versioning == yts3.VersioningEnabled {
		if object.data != nil {
			if object.versions == nil {
				object.versions = skiplist.NewCustomMap(func(l, r interface{}) bool {
					return l.(yts3.VersionID) < r.(yts3.VersionID)
				})
			}
			object.versions.Set(object.data.versionID, object.data)
		}
	}

	object.data = item
}

func (b *bucket) rm(publicKey, name string, at time.Time) (result yts3.ObjectDeleteResult, rerr error) {
	object := b.object(name)
	if object == nil {
		// S3 does not report an error when attemping to delete a key that does not exist
		return result, nil
	}

	if b.versioning == yts3.VersioningEnabled {
		item := &bucketData{lastModified: at, name: name, deleteMarker: true}
		b.put(publicKey, name, item)
		result.IsDeleteMarker = true
		result.VersionID = item.versionID

	} else {
		object.data = nil
		if object.versions == nil || object.versions.Len() == 0 {
			b.objects.Delete(name)
		}
	}

	return result, nil
}
