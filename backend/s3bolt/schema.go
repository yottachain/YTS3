package s3bolt

import (
	"bytes"
	"time"

	"github.com/boltdb/bolt"
	"github.com/yottachain/YTS3/internal/s3io"
	"github.com/yottachain/YTS3/yts3"
)

type boltBucket struct {
	CreationDate time.Time
}
type boltObject struct {
	Name         string
	Metadata     map[string]string
	LastModified time.Time
	Size         int64
	Contents     []byte
	Hash         []byte
}

type metaBucket struct {
	*bolt.Tx
	metaName []byte
	bucket   *bolt.Bucket
}

func (b *boltObject) Object(objectName string, rangeRequest *yts3.ObjectRangeRequest) (*yts3.Object, error) {
	data := b.Contents

	rnge, err := rangeRequest.Range(b.Size)
	if err != nil {
		return nil, err
	}

	if rnge != nil {
		data = data[rnge.Start : rnge.Start+rnge.Length]
	}

	return &yts3.Object{
		Name:     objectName,
		Metadata: b.Metadata,
		Size:     b.Size,
		Contents: s3io.ReaderWithDummyCloser{bytes.NewReader(data)},
		Range:    rnge,
		Hash:     b.Hash,
	}, nil
}
