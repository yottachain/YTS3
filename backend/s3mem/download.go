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
	if len(db.buckets) == 0 {
		if v, has := UserAllBucketsCACHE.Get(publicKey); has {
			if RegDb, ok := v.(*Backend); ok {
				db = RegDb
			}
		}
	}
	bucket := db.buckets[bucketName]
	if bucket == nil {
		return nil, yts3.BucketNotFound(bucketName)
	}
	c := api.GetClient(publicKey)
	if c == nil {
		return nil, yts3.ResourceError(yts3.ErrInvalidAccessKeyID, "YTA"+publicKey)
	}
	download, errMsg := c.NewDownloadLastVersion(bucketName, objectName)
	if errMsg != nil {
		logrus.Errorf("[GetObjectV2]NewDownloadLastVersion err:%s\n", errMsg)
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
		logrus.Errorf("[GetObjectV2]toObject err:%s\n", err)
		return nil, err
	}
	content = getContentByMeta(result.Metadata)
	result.Size = content.Size
	if bucket.versioning != yts3.VersioningEnabled {
		result.VersionID = ""
	}
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

/*
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

func (db *Backend) GetObjectMeta(publicKey, bucketName, objectName string, c *api.Client) (*bucketObject, error) {
	items, err1 := c.NewObjectAccessor().ListObject(bucketName, "", objectName, false, primitive.NilObjectID, 1)
	if err1 != nil || len(items) == 0 {
		return nil, pkt.ToError(err1)
	}
	item := items[0]
	meta, err := api.BytesToFileMetaMap(item.Meta, item.FileId)
	if err != nil {
		return nil, yts3.ErrNoSuchKey
	}
	t := time.Unix(item.FileId.Timestamp().Unix(), 0)
	meta["x-amz-meta-s3b-last-modified"] = t.Format("20060102T150405Z")
	content := getContentByMeta(meta)
	content.Key = item.FileName
	content.Owner = &yts3.UserInfo{
		ID:          c.Username,
		DisplayName: c.Username,
	}
	hash, _ := hex.DecodeString(meta["ETag"])
	return &bucketObject{
		name: item.FileName,
		data: &bucketData{
			name:         item.FileName,
			hash:         hash,
			metadata:     meta,
			lastModified: content.LastModified.Time,
		}}, nil
}

func (db *Backend) GetObject(publicKey, bucketName, objectName string, rangeRequest *yts3.ObjectRangeRequest) (*yts3.Object, error) {
	isExistObject := objectExists(publicKey, bucketName, objectName)
	if isExistObject {
		if len(db.buckets) == 0 {
			if v, has := UserAllBucketsCACHE.Get(publicKey); has {
				if RegDb, ok := v.(*Backend); ok {
					db = RegDb
				}
			}
		}
		bucket := db.buckets[bucketName]
		if bucket == nil {
			return nil, yts3.BucketNotFound(bucketName)
		}
		c := api.GetClient(publicKey)
		if c == nil {
			return nil, yts3.ResourceError(yts3.ErrInvalidAccessKeyID, "YTA"+publicKey)
		}
		download, errMsg := c.NewDownloadLastVersion(bucketName, objectName)
		if errMsg != nil {
			logrus.Errorf("[ListObjects]NewDownloadLastVersion err:%s\n", errMsg)
			return nil, yts3.ErrNoSuchKey
		}
		obj, err := db.GetObjectMeta(publicKey, bucketName, objectName, c)
		if err != nil {
			logrus.Errorf("[ListObjects]GetObjectMeta err:%s\n", err)
			return nil, err
		}
		result, err := obj.data.toObject(rangeRequest, true)
		if err != nil {
			logrus.Errorf("[ListObjects]toObject err:%s\n", err)
			return nil, err
		}
		content := getContentByMeta(result.Metadata)
		result.Size = content.Size
		if bucket.versioning != yts3.VersioningEnabled {
			result.VersionID = ""
		}
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
		hash, _ := hex.DecodeString(content.ETag)
		result.Hash = hash
		return result, nil
	} else {
		return nil, nil
	}
}

func (db *Backend) GetObjectV2(publicKey, bucketName, objectName string, rangeRequest *yts3.ObjectRangeRequest, prefix *yts3.Prefix, page yts3.ListBucketPage) (*yts3.Object, error) {
	isExistObject := objectExists(publicKey, bucketName, objectName)
	if isExistObject {
		if len(db.buckets) == 0 {
			if v, has := UserAllBucketsCACHE.Get(publicKey); has {
				if RegDb, ok := v.(*Backend); ok {
					db = RegDb
				}
			}
		}
		bucket := db.buckets[bucketName]
		if bucket == nil {
			return nil, yts3.BucketNotFound(bucketName)
		}
		c := api.GetClient(publicKey)
		if c == nil {
			return nil, yts3.ResourceError(yts3.ErrInvalidAccessKeyID, "YTA"+publicKey)
		}
		download, errMsg := c.NewDownloadLastVersion(bucketName, objectName)
		if errMsg != nil {
			logrus.Errorf("[ListObjects]NewDownloadLastVersion err:%s\n", errMsg)
			return nil, yts3.ErrNoSuchKey
		}
		obj, err := db.GetObjectMeta(publicKey, bucketName, objectName, c)
		if err != nil {
			logrus.Errorf("[ListObjects]GetObjectMeta err:%s\n", err)
			return nil, err
		}
		result, err := obj.data.toObject(rangeRequest, true)
		if err != nil {
			logrus.Errorf("[ListObjects]toObject err:%s\n", err)
			return nil, err
		}
		content := getContentByMeta(result.Metadata)
		result.Size = content.Size
		if bucket.versioning != yts3.VersioningEnabled {
			result.VersionID = ""
		}
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
		hash, _ := hex.DecodeString(content.ETag)
		result.Hash = hash
		return result, nil
	} else {
		return nil, nil
	}
}

func (db *Backend) HeadObject(publicKey, bucketName, objectName string) (*yts3.Object, error) {
	db.Lock.RLock()
	defer db.Lock.RUnlock()
	if len(db.buckets) == 0 {
		if v, has := UserAllBucketsCACHE.Get(publicKey); has {
			if RegDb, ok := v.(*Backend); ok {
				db = RegDb
			}
		}
	}
	bucket := db.buckets[bucketName]
	if bucket == nil {
		return nil, yts3.BucketNotFound(bucketName)
	}
	c := api.GetClient(publicKey)
	if c == nil {
		return nil, yts3.ResourceError(yts3.ErrInvalidAccessKeyID, "YTA"+publicKey)
	}
	obj, err := db.GetObjectMeta(publicKey, bucketName, objectName, c)
	if err != nil {
		return nil, err
	}
	return obj.data.toObject(nil, false)
}
*/
