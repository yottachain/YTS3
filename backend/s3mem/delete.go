package s3mem

import (
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/api"
	"github.com/yottachain/YTCoreService/pkt"
	"github.com/yottachain/YTS3/yts3"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func (db *Backend) rm(publicKey, bucketName, objectName string, c *api.Client) (result yts3.ObjectDeleteResult, rerr error) {
	objectAccessor := c.NewObjectAccessor()
	err := objectAccessor.DeleteObject(bucketName, objectName, primitive.ObjectID{})
	if err != nil {
		logrus.Errorf("[S3Delete]/%s/%s,Err:%s\n", bucketName, objectName, err)
		return
	}
	return result, nil
}

func (db *Backend) DeleteMulti(publicKey, bucketName string, objects ...string) (result yts3.MultiDeleteResult, err error) {
	_, er := db.GetBucket(publicKey, bucketName)
	if er != nil {
		return result, err
	}
	c := api.GetClient(publicKey)
	if c == nil {
		return result, yts3.ResourceError(yts3.ErrInvalidAccessKeyID, "YTA"+publicKey)
	}
	for _, object := range objects {
		dresult, err := db.rm(publicKey, bucketName, object, c)
		_ = dresult
		if err != nil {
			errres := yts3.ErrorResultFromError(err)
			if errres.Code == yts3.ErrInternal {

			}
			result.Error = append(result.Error, errres)
		} else {
			result.Deleted = append(result.Deleted, yts3.ObjectID{
				Key: object,
			})
		}
	}
	return result, nil
}

func (db *Backend) DeleteObject(publicKey, bucketName, objectName string) (result yts3.ObjectDeleteResult, rerr error) {
	_, er := db.GetBucket(publicKey, bucketName)
	if er != nil {
		return result, er
	}
	c := api.GetClient(publicKey)
	if c == nil {
		return result, yts3.ResourceError(yts3.ErrInvalidAccessKeyID, "YTA"+publicKey)
	}
	return db.rm(publicKey, bucketName, objectName, c)
}

func (db *Backend) DeleteBucket(publicKey, bucketName string) error {
	_, er := db.GetBucket(publicKey, bucketName)
	if er != nil {
		return er
	}
	c := api.GetClient(publicKey)
	if c == nil {
		return yts3.ResourceError(yts3.ErrInvalidAccessKeyID, "YTA"+publicKey)
	}
	bucketAccessor := c.NewBucketAccessor()
	err := bucketAccessor.DeleteBucket(bucketName)
	if err != nil {
		if err.Code == pkt.BUCKET_NOT_EMPTY {
			return yts3.ResourceError(yts3.ErrBucketNotEmpty, bucketName)
		} else if err.Code == pkt.INVALID_BUCKET_NAME {
			return yts3.ResourceError(yts3.ErrNoSuchBucket, bucketName)
		}
		logrus.Errorf("[S3Delete]Bucket:%s,Error msg: %s\n", bucketName, err)
	}
	db.DelBucket(publicKey, bucketName)
	return nil
}
