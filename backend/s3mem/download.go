package s3mem

import (
	"encoding/hex"
	"io"

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
	download, errMsg := c.NewDownloadLastVersion(bucketName, objectName)
	if errMsg != nil {
		logrus.Errorf("[S3Download]NewDownloadLastVersion err:%s\n", errMsg)
		if errMsg.Code == pkt.INVALID_OBJECT_NAME {
			return nil, yts3.ErrNoSuchKey
		}
		return nil, pkt.ToError(errMsg)
	}
	meta, err := api.BytesToFileMetaMap(download.Meta, primitive.NilObjectID)
	if err != nil {
		return nil, err
	}
	t := download.GetTime()
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
	}
	hash, _ = hex.DecodeString(content.ETag)
	result.Hash = hash
	return result, nil
}

func (db *Backend) HeadObject(publicKey, bucketName, objectName string) (*yts3.Object, error) {
	return db.GetObjectV2(publicKey, bucketName, objectName, nil, nil, yts3.ListBucketPage{})
}

func (db *Backend) GetObject(publicKey, bucketName, objectName string, rangeRequest *yts3.ObjectRangeRequest) (*yts3.Object, error) {
	return db.GetObjectV2(publicKey, bucketName, objectName, rangeRequest, nil, yts3.ListBucketPage{})
}
