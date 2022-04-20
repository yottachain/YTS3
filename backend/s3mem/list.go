package s3mem

import (
	"fmt"
	"sync"
	"time"

	"github.com/patrickmn/go-cache"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/api"
	"github.com/yottachain/YTCoreService/pkt"
	"github.com/yottachain/YTS3/yts3"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

var Bucket_CACHE = cache.New(5*time.Second, 5*time.Second)

func (db *Backend) DelBucket(publicKey, bucketname string) {
	backmap, err := db.listBuckets(publicKey)
	if err != nil {
		return
	}
	backmap.Delete(bucketname)
}

func (db *Backend) GetBucket(publicKey, bucketname string) (*bucket, error) {
	backmap, err := db.listBuckets(publicKey)
	if err != nil {
		return nil, err
	}
	if b, ok := backmap.Load(bucketname); ok {
		bu, _ := b.(*bucket)
		return bu, nil
	} else {
		return nil, yts3.BucketNotFound(bucketname)
	}
}

func (db *Backend) listBuckets(publicKey string) (*sync.Map, error) {
	if bs, has := Bucket_CACHE.Get(publicKey); has {
		bucks, _ := bs.(*sync.Map)
		return bucks, nil
	} else {
		c := api.GetClient(publicKey)
		if c == nil {
			return nil, yts3.ResourceError(yts3.ErrInvalidAccessKeyID, "YTA"+publicKey)
		}
		bucketAccessor := c.NewBucketAccessor()
		names, err1 := bucketAccessor.ListBucket()
		if err1 != nil {
			logrus.Errorf("[ListBucket]AuthSuper ERR:%s\n", err1)
			return nil, pkt.ToError(err1)
		}
		var buckmap sync.Map
		len := len(names)
		for i := 0; i < len; i++ {
			bucket := &bucket{}
			bucket.name = names[i]
			bucket.versioning = "Suspended"
			bucket.creationDate = yts3.NewContentTime(time.Now())
			buckmap.Store(bucket.name, bucket)
		}
		Bucket_CACHE.SetDefault(publicKey, &buckmap)
		return &buckmap, nil
	}
}

func (db *Backend) ListBuckets(publicKey string) ([]yts3.BucketInfo, error) {
	backmap, err := db.listBuckets(publicKey)
	if err != nil {
		return nil, err
	}
	var buckets []yts3.BucketInfo
	backmap.Range(func(key, value interface{}) bool {
		bucketInfo := yts3.BucketInfo{}
		name, _ := key.(string)
		bucketInfo.Name = name
		bucketInfo.CreationDate = yts3.NewContentTime(time.Now())
		buckets = append(buckets, bucketInfo)
		return true
	})
	return buckets, nil
}

func (me *Backend) ListBucket(publicKey, name string, prefix *yts3.Prefix, page yts3.ListBucketPage) (*yts3.ObjectList, error) {
	var response = yts3.NewObjectList()
	c := api.GetClient(publicKey)
	if c == nil {
		return nil, yts3.ResourceError(yts3.ErrInvalidAccessKeyID, "YTA"+publicKey)
	}
	objectAccessor := c.NewObjectAccessor()
	startFile := ""
	if page.HasMarker {
		startFile = page.Marker
	}
	pfix := ""
	if prefix.HasPrefix {
		pfix = prefix.Prefix
	}
	items, err := objectAccessor.ListObject(name, startFile, pfix, false, primitive.NilObjectID, uint32(page.MaxKeys))
	if err != nil {
		return response, fmt.Errorf(err.String())
	}
	logrus.Infof("[ListObjects]Response %d items\n", len(items))
	lastFile := ""
	num := 0
	for _, v := range items {
		num++
		meta, err := api.BytesToFileMetaMap(v.Meta, primitive.ObjectID{})
		if err != nil {
			logrus.Warnf("[ListObjects]ERR meta,filename:%s\n", v.FileName)
			continue
		}
		t := time.Unix(v.FileId.Timestamp().Unix(), 0)
		meta["x-amz-meta-s3b-last-modified"] = t.Format("20060102T150405Z")
		content := getContentByMeta(meta)
		content.Key = v.FileName
		content.Owner = &yts3.UserInfo{
			ID:          c.Username,
			DisplayName: c.Username,
		}
		response.Contents = append(response.Contents, content)
		lastFile = v.FileName
	}
	if int64(num) >= page.MaxKeys {
		response.NextMarker = lastFile
		response.IsTruncated = true
	}
	return response, nil
}
