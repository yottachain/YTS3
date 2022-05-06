package s3mem

import (
	"encoding/hex"
	"io"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/api"
	"github.com/yottachain/YTCoreService/pkt"
	"github.com/yottachain/YTS3/yts3"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func (db *Backend) GetObjectV2(publicKey, bucketName, objectName string, rangeRequest *yts3.ObjectRangeRequest, prefix *yts3.Prefix, page yts3.ListBucketPage) (*yts3.Object, error) {
	_, err := db.GetBucket(publicKey, bucketName)
	if err != nil {
		return nil, err
	}
	c := api.GetClient(publicKey)
	if c == nil {
		return nil, yts3.ResourceError(yts3.ErrInvalidAccessKeyID, "YTA"+publicKey)
	}
	var metabs []byte
	var t time.Time
	download, errMsg := c.NewDownloadLastVersion(bucketName, objectName)
	if errMsg != nil {
		logrus.Errorf("[S3Download]NewDownloadLastVersion err:%s\n", errMsg)
		if errMsg.Code == pkt.INVALID_OBJECT_NAME {
			items, err := c.NewObjectAccessor().ListObject(bucketName, "", objectName, false, primitive.NilObjectID, uint32(page.MaxKeys))
			if err != nil {
				return nil, pkt.ToError(errMsg)
			}
			if len(items) > 0 {
				metabs = items[0].Meta
				t = items[0].FileId.Timestamp()
			} else {
				return nil, yts3.ErrNoSuchKey
			}
		} else {
			return nil, pkt.ToError(errMsg)
		}
	} else {
		metabs = download.Meta
		t = download.GetTime()
	}
	meta, err := api.BytesToFileMetaMap(metabs, primitive.NilObjectID)
	if err != nil {
		return nil, err
	}
	meta["x-amz-meta-s3b-last-modified"] = t.Format("20060102T150405Z")
	content := getContentByMeta(meta)
	content.Key = objectName
	content.Owner = &yts3.UserInfo{
		ID:          c.Username,
		DisplayName: c.Username,
	}
	hash, _ := hex.DecodeString(meta["ETag"])
	obj := &bucketObject{name: objectName,
		data: &bucketData{
			name:         objectName,
			hash:         hash,
			metadata:     meta,
			lastModified: content.LastModified.Time,
		}}
	result, err := obj.data.toObject(rangeRequest, true)
	if err != nil {
		logrus.Errorf("[S3Download]toObject err:%s\n", err)
		return nil, err
	}
	content = getContentByMeta(result.Metadata)
	result.Size = content.Size
	if result.Size > 0 {
		if rangeRequest != nil {
			if rangeRequest.End == -1 {
				rangeRequest.End = content.Size
				rangeRequest.FromEnd = true
			}
			result.Contents = &ContentReader{download.LoadRange(rangeRequest.Start, rangeRequest.End).(io.ReadCloser)}
			result.Range = &yts3.ObjectRange{
				Start:  rangeRequest.Start,
				Length: rangeRequest.End - rangeRequest.Start,
			}
		} else {
			result.Contents = &ContentReader{download.Load().(io.ReadCloser)}
		}
	} else if result.Size == 0 {
		result.Contents = &ZeroReader{}
	}
	hash, _ = hex.DecodeString(content.ETag)
	result.Hash = hash
	return result, nil
}

type ZeroReader struct {
	io.ReadCloser
}

func (cr *ZeroReader) Close() error {
	return nil
}

func (cr *ZeroReader) Read(buf []byte) (int, error) {
	return 0, io.EOF
}

func (db *Backend) HeadObject(publicKey, bucketName, objectName string) (*yts3.Object, error) {
	return db.GetObjectV2(publicKey, bucketName, objectName, nil, nil, yts3.ListBucketPage{})
}

func (db *Backend) GetObject(publicKey, bucketName, objectName string, rangeRequest *yts3.ObjectRangeRequest) (*yts3.Object, error) {
	return db.GetObjectV2(publicKey, bucketName, objectName, rangeRequest, nil, yts3.ListBucketPage{})
}
