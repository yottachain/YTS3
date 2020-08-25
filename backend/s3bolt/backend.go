package s3bolt

import (
	"crypto/md5"
	"fmt"
	"io"

	"github.com/boltdb/bolt"
	"github.com/yottachain/YTS3/yts3"
	"gopkg.in/mgo.v2/bson"
)

type Backend struct {
	bolt           *bolt.DB
	timeSource     yts3.TimeSource
	metaBucketName []byte
}

// var _ yts3.Backend = &Backend{}

type Option func(b *Backend)

func WithTimeSource(timeSource yts3.TimeSource) Option {
	return func(b *Backend) { b.timeSource = timeSource }
}

func NewFile(file string, opts ...Option) (*Backend, error) {
	if file == "" {
		return nil, fmt.Errorf("gofakes3: invalid bolt file name")
	}
	db, err := bolt.Open(file, 0600, nil)
	if err != nil {
		return nil, err
	}
	return New(db, opts...), nil
}

func New(bolt *bolt.DB, opts ...Option) *Backend {
	b := &Backend{
		bolt:           bolt,
		metaBucketName: []byte("_meta"), // Underscore guarantees no overlap with legal S3 bucket names
	}
	for _, opt := range opts {
		opt(b)
	}
	if b.timeSource == nil {
		b.timeSource = yts3.DefaultTimeSource()
	}
	return b
}

func (db *Backend) metaBucket(tx *bolt.Tx) (*metaBucket, error) {
	var bucket *bolt.Bucket
	var err error

	if tx.Writable() {
		bucket, err = tx.CreateBucketIfNotExists(db.metaBucketName)
		if err != nil {
			return nil, err
		}
	} else {
		bucket = tx.Bucket(db.metaBucketName)
		if bucket == nil {
			// FIXME: support legacy databases; remove when versioning is supported.
			return nil, nil
		}
	}

	return &metaBucket{
		Tx:       tx,
		bucket:   bucket,
		metaName: db.metaBucketName,
	}, nil
}

func (db *Backend) PutObject(
	bucketName, objectName string,
	meta map[string]string,
	input io.Reader, size int64,
) (result yts3.PutObjectResult, err error) {

	bts, err := yts3.ReadAll(input, size)
	if err != nil {
		return result, err
	}

	hash := md5.Sum(bts)

	return result, db.bolt.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			return yts3.BucketNotFound(bucketName)
		}

		data, err := bson.Marshal(&boltObject{
			Name:     objectName,
			Metadata: meta,
			Size:     int64(len(bts)),
			Contents: bts,
			Hash:     hash[:],
		})
		if err != nil {
			return err
		}
		if err := b.Put([]byte(objectName), data); err != nil {
			return err
		}
		return nil
	})
}
