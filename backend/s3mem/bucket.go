package s3mem

import (
	"io"
	"strconv"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/api"
	"github.com/yottachain/YTS3/yts3"
)

type versionGenFunc func() yts3.VersionID

type versioningStatus int

type bucket struct {
	name         string
	versioning   yts3.VersioningStatus
	versionGen   versionGenFunc
	creationDate yts3.ContentTime
}

func newBucket(publicKey, bucketName string, at time.Time, versionGen versionGenFunc) *bucket {
	c := api.GetClient(publicKey)
	bucketAccessor := c.NewBucketAccessor()
	var header map[string]string
	header = make(map[string]string)
	header["version_status"] = "Enabled"
	meta, err := api.BucketMetaMapToBytes(header)
	if err != nil {
		logrus.Errorf("[CreateBucket]BucketMetaMapToBytes ERR:%s\n", err)
	}
	err2 := bucketAccessor.CreateBucket(bucketName, meta)
	if err2 != nil {
		logrus.Error(err2)
	}
	return &bucket{
		name:         bucketName,
		creationDate: yts3.NewContentTime(at),
		versionGen:   versionGen,
	}
}

type bucketObject struct {
	name string
	data *bucketData
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
	szStr := bi.metadata["contentLength"]
	sz, err := strconv.ParseInt(szStr, 10, 64)
	if err != nil {
		logrus.Errorf("[Bucket]toObject err:%s\n", err)
	}
	var contents io.ReadCloser
	var rnge *yts3.ObjectRange
	if withBody {
		rnge, err = rangeRequest.Range(sz)
		if err != nil {
			return nil, err
		}
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

func (db *Backend) CreateBucket(publicKey, name string) error {
	backmap, err := db.listBuckets(publicKey)
	if err != nil {
		return err
	}
	if _, ok := backmap.Load(name); ok {
		return yts3.ResourceError(yts3.ErrBucketAlreadyExists, name)
	}
	buck := newBucket(publicKey, name, db.timeSource.Now(), db.nextVersion)
	backmap.Store(name, buck)
	return nil
}
