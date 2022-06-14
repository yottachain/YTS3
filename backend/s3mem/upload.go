package s3mem

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/api"
	"github.com/yottachain/YTCoreService/api/cache"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/pkt"
	"github.com/yottachain/YTS3/yts3"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

var Object_UP_CH chan int
var Object_Timeout int = 60
var SyncFileMin int

func InitObjectUpPool() {
	MaxCreateObjNum := env.GetConfig().GetRangeInt("MaxCreateObjNum", 20, 500, 50)
	Object_Timeout = env.GetConfig().GetRangeInt("ObjectTimeout", 10, 300, 60)
	SyncFileMin = env.GetConfig().GetRangeInt("SyncFileMin", 1, 10, 2) * 1024 * 1024
	Object_UP_CH = make(chan int, MaxCreateObjNum)
	for ii := 0; ii < MaxCreateObjNum; ii++ {
		Object_UP_CH <- 1
	}
}

func (db *Backend) PutObject(publicKey, bucketName, objectName string, meta map[string]string, input io.Reader, size int64) (result yts3.PutObjectResult, err error) {
	_, er := db.GetBucket(publicKey, bucketName)
	if er != nil {
		return result, er
	}
	c := api.GetClient(publicKey)
	if c == nil {
		return result, yts3.ResourceError(yts3.ErrInvalidAccessKeyID, "YTA"+publicKey)
	}
	var hash []byte
	var bts []byte
	header := make(map[string]string)
	if size >= int64(SyncFileMin) {
		u1 := primitive.NewObjectID().Hex()
		errw := writeCacheFile(env.GetS3Cache(), u1, input)
		if errw != nil {
			return result, errw
		}
		filePath := env.GetS3Cache() + u1
		md5bytes, erre := c.UploadFile(filePath, bucketName, objectName)
		if erre != nil {
			logrus.Errorf("[S3Upload]/%s/%s,UploadFile ERR: %s\n", bucketName, objectName, erre)
			return result, pkt.ToError(erre)
		}
		hash = md5bytes
		if env.SyncMode == 0 {
			cache.Delete([]string{filePath})
		}
	} else {
		timeout := time.After(time.Second * time.Duration(Object_Timeout))
		select {
		case <-Object_UP_CH:
		case <-timeout:
			return result, errors.New("Upload request too frequently.\n")
		}
		defer func() { Object_UP_CH <- 1 }()
		bts, err = yts3.ReadAll(input, size)
		if err != nil {
			logrus.Errorf("[S3Upload]/%s/%s,Read ERR: %s\n", bucketName, objectName, err)
			return result, err
		}
	}
	if size < int64(SyncFileMin) {
		if size > 0 {
			md5Hash, err1 := c.SyncUploadBytes(bts, bucketName, objectName)
			if err1 != nil {
				logrus.Errorf("[S3Upload]/%s/%s,SyncUploadBytes ERR:%s\n", bucketName, objectName, err1)
				return result, pkt.ToError(err1)
			}
			hash = md5Hash
		}
	}
	if size == 0 {
		hashz := md5.Sum(bts)
		header["ETag"] = hex.EncodeToString(hashz[:])
	} else {
		header["ETag"] = hex.EncodeToString(hash[:])
	}
	header["contentLength"] = strconv.FormatInt(size, 10)
	metadata2, err2 := api.FileMetaMapTobytes(header)
	if err2 != nil {
		logrus.Errorf("[S3Upload]/%s/%s,FileMetaMapTobytes:%s\n", bucketName, objectName, err2)
		return result, err2
	}
	if size == 0 {
		errzero := c.NewObjectAccessor().CreateObject(bucketName, objectName, primitive.NewObjectID(), metadata2)
		if errzero != nil {
			logrus.Errorf("[S3Upload]/%s/%s,Save meta data ERR:%s\n", bucketName, objectName, errzero)
			return result, pkt.ToError(errzero)
		}
	}
	logrus.Infof("[S3Upload]/%s/%sFile upload success,file md5 value : %s\n", bucketName, objectName, hex.EncodeToString(hash[:]))
	return result, nil
}

func writeCacheFile(directory, fileName string, input io.Reader) error {
	s, err := os.Stat(directory)
	if err != nil {
		if !os.IsExist(err) {
			err = os.MkdirAll(directory, os.ModePerm)
			if err != nil {
				return err
			}
		} else {
			return err
		}
	} else {
		if !s.IsDir() {
			return errors.New("The specified path is not a directory.")
		}
		filePath := directory + fileName
		logrus.Infof("[S3Upload]Write cache:%s\n", filePath)
		f, err := os.OpenFile(filePath, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0644)
		if err != nil {
			return err
		}
		defer f.Close()
		readbuf := make([]byte, 8192)
		for {
			num, err := input.Read(readbuf)
			if err != nil && err != io.EOF {
				logrus.Errorf("[S3Upload]Write cache:%s,ERR:%s\n", filePath, err)
				return err
			}
			if num > 0 {
				bs := readbuf[0:num]
				f.Write(bs)
			}
			if err != nil && err == io.EOF {
				break
			}
		}
	}
	return nil
}

func (db *Backend) MultipartUpload(publicKey, bucketName, objectName string, partsPath []string, size int64) (result yts3.PutObjectResult, err error) {
	_, er := db.GetBucket(publicKey, bucketName)
	if er != nil {
		return result, er
	}
	c := api.GetClient(publicKey)
	if c == nil {
		return result, yts3.ResourceError(yts3.ErrInvalidAccessKeyID, "YTA"+publicKey)
	}
	md5Bytes, errB := c.UploadMultiPartFile(partsPath, bucketName, objectName)
	if errB != nil {
		logrus.Errorf("[S3Upload]MultipartUpload /%s/%s,err:%s\n", bucketName, objectName, errB)
		return
	}
	logrus.Infof("[S3Upload]MultipartUpload /%s/%s,File upload success,file md5 value : %s\n", bucketName, objectName, hex.EncodeToString(md5Bytes[:]))
	return result, nil
}
