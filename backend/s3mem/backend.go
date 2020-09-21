package s3mem

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ryszard/goskiplist/skiplist"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/api"
	"github.com/yottachain/YTS3/conf"
	"github.com/yottachain/YTS3/yts3"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

var (
	emptyPrefix = &yts3.Prefix{}
	// emptyVersionsPage = &yts3.ListBucketVersionsPage{}
)

type Backend struct {
	buckets          map[string]*bucket
	timeSource       yts3.TimeSource
	versionGenerator *versionGenerator
	versionSeed      int64
	versionSeedSet   bool
	versionScratch   []byte
	lock             sync.RWMutex
}

var _ yts3.Backend = &Backend{}
var _ yts3.VersionedBackend = &Backend{}

type Option func(b *Backend)

func WithTimeSource(timeSource yts3.TimeSource) Option {
	return func(b *Backend) { b.timeSource = timeSource }
}

func WithVersionSeed(seed int64) Option {
	return func(b *Backend) { b.versionSeed = seed; b.versionSeedSet = true }
}

func New(opts ...Option) *Backend {
	b := &Backend{
		buckets: make(map[string]*bucket),
	}
	for _, opt := range opts {
		opt(b)
	}
	if b.timeSource == nil {
		b.timeSource = yts3.DefaultTimeSource()
	}
	if b.versionGenerator == nil {
		if b.versionSeedSet {
			b.versionGenerator = newVersionGenerator(uint64(b.versionSeed), 0)
		} else {
			b.versionGenerator = newVersionGenerator(uint64(b.timeSource.Now().UnixNano()), 0)
		}
	}
	return b
}

//ListBuckets s3 list all buckets
func (db *Backend) ListBuckets(publicKey string) ([]yts3.BucketInfo, error) {
	db.lock.RLock()
	defer db.lock.RUnlock()
	c := api.GetClient(publicKey)
	bucketAccessor := c.NewBucketAccessor()
	names, err1 := bucketAccessor.ListBucket()
	if err1 != nil {
		logrus.Errorf("[ListBucket ]AuthSuper ERR:%s\n", err1)
	}

	var buckets []yts3.BucketInfo
	len := len(names)
	for i := 0; i < len; i++ {
		bucketInfo := yts3.BucketInfo{}
		bucket := bucket{}
		bucket.name = names[i]
		bucket.versioning = "Suspended"
		bucket.creationDate = yts3.NewContentTime(time.Now())
		bucketInfo.Name = names[i]
		bucketInfo.CreationDate = yts3.NewContentTime(time.Now())
		db.buckets[bucketInfo.Name] = &bucket
		buckets = append(buckets, bucketInfo)
	}

	return buckets, nil
}

func getContentByMeta(meta map[string]string) *yts3.Content {
	var content yts3.Content
	content.ETag = meta["ETag"]
	if contentLengthString, ok := meta["content-length"]; ok {
		size, err := strconv.ParseInt(contentLengthString, 10, 64)
		if err == nil {
			content.Size = size
		}
	}
	if contentLengthString, ok := meta["contentLength"]; ok {
		size, err := strconv.ParseInt(contentLengthString, 10, 64)
		if err == nil {
			content.Size = size
		}
	}
	if lastModifyString, ok := meta["x-amz-meta-s3b-last-modified"]; ok {
		lastModifyTime, err := time.Parse("20190108T135030Z", lastModifyString)
		if err == nil {
			content.LastModified = yts3.ContentTime{lastModifyTime}
		}
	}
	return &content
}

//ListBucket s3 listObjects
func (db *Backend) ListBucket(publicKey, name string, prefix *yts3.Prefix, page yts3.ListBucketPage) (*yts3.ObjectList, error) {
	db.lock.RLock()
	defer db.lock.RUnlock()

	var response = yts3.NewObjectList()

	c := api.GetClient(publicKey)
	sklist := skiplist.NewStringMap()

	filename := ""
	for {
		objectAccessor := c.NewObjectAccessor()
		items, err := objectAccessor.ListObject(name, filename, "", false, primitive.ObjectID{}, 1000)
		if err != nil {
			return response, fmt.Errorf(err.String())
		}
		logrus.Printf("items len %d\n", len(items))
		for _, v := range items {
			meta, err := api.BytesToFileMetaMap(v.Meta, primitive.ObjectID{})

			if err != nil {
				continue
			}
			content := getContentByMeta(meta)
			content.Key = v.FileName
			content.Owner = &yts3.UserInfo{
				ID:          c.Username,
				DisplayName: c.Username,
			}
			response.Contents = append(response.Contents, content)
			filename = v.FileName
			hash, _ := hex.DecodeString(meta["ETag"])
			sklist.Set(v.FileName, &bucketObject{
				name: v.FileName,
				data: &bucketData{
					name:         v.FileName,
					hash:         hash,
					metadata:     meta,
					lastModified: v.FileId.Timestamp(),
				},
			})
		}
		if len(items) < 1000 {
			break
		}
	}
	db.buckets[name].objects = sklist
	return response, nil
}

//BucketExists BucketExists
func (db *Backend) BucketExists(name string) (exists bool, err error) {
	db.lock.RLock()
	defer db.lock.RUnlock()
	return db.buckets[name] != nil, nil
}

//PutObject upload file
func (db *Backend) PutObject(publicKey, bucketName, objectName string, meta map[string]string, input io.Reader, size int64) (result yts3.PutObjectResult, err error) {
	c := api.GetClient(publicKey)
	upload := c.NewUploadObject()
	iniPath := "conf/yotta_config.ini"
	cfg, err := conf.CreateConfig(iniPath)
	cache := cfg.GetCacheInfo("directory")
	directory := cache + "/" + bucketName
	if err != nil {
		panic(err)
	}

	var hash [16]byte
	var bts []byte
	var header map[string]string
	header = make(map[string]string)
	if size >= 10485760 {

		errw := writeCacheFile(directory, objectName, input)
		if errw != nil {
			return
		}
		filePath := directory + "/" + objectName
		hashw, erre := upload.UploadFile(filePath)
		if erre != nil {
			return
		}
		logrus.Info("upload hash result:", hex.EncodeToString(hashw))
	} else {
		bts, err = yts3.ReadAll(input, size)
		if err != nil {
			return result, err
		}
		hash = md5.Sum(bts)
		logrus.Println("length:::", len(bts))
	}

	db.lock.Lock()
	defer db.lock.Unlock()

	bucket := db.buckets[bucketName]
	if bucket == nil {
		return result, yts3.BucketNotFound(bucketName)
	}

	item := &bucketData{
		name:         objectName,
		body:         bts,
		hash:         hash[:],
		etag:         `"` + hex.EncodeToString(hash[:]) + `"`,
		metadata:     meta,
		lastModified: db.timeSource.Now(),
	}
	item.metadata["ETag"] = item.etag
	logrus.Info("upload hash etag:", item.etag)
	// logrus.Info("fileSize:::::::::", len(item.body))

	if size < 10485760 {
		resulthash, err1 := upload.UploadBytes(item.body)
		if err1 != nil {
			logrus.Printf("ERR", err1)
			return
		}
		logrus.Info("upload hash result:", hex.EncodeToString(resulthash))
	}

	//update meta data

	header["ETag"] = hex.EncodeToString(hash[:])
	header["contentLength"] = strconv.FormatInt(size, 10)
	metadata2, err2 := api.FileMetaMapTobytes(header)

	if err2 != nil {
		logrus.Errorf("[FileMetaMapTobytes ]:%s\n", err2)
		return
	}

	logrus.Println("bucket name is [" + bucketName + "]")
	err3 := c.NewObjectAccessor().CreateObject(bucketName, objectName, upload.VNU, metadata2)
	if err3 != nil {
		logrus.Errorf("[Save meta data ]:%s\n", err3)
	}
	if size >= 10485760 {
		filePath := directory + "/" + objectName
		deleteCacheFile(filePath)
	}

	return result, nil
}

//deleteCacheFile 删除缓存文件
func deleteCacheFile(path string) {
	del := os.Remove(path)
	if del != nil {
		fmt.Println(del)
	}
}

func writeCacheFile(directory, fileName string, input io.Reader) error {

	// path := directory + "/" + fileName
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
	}
	if !strings.HasSuffix(directory, "/") {
		directory = directory + "/"
	}
	filePath := directory + fileName
	f, err := os.OpenFile(filePath, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	readbuf := make([]byte, 8192)
	for {
		num, err := input.Read(readbuf)
		if err != nil && err != io.EOF {
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
	return nil
}

//CreateBucket create bucket
func (db *Backend) CreateBucket(publicKey, name string) error {
	db.lock.Lock()
	defer db.lock.Unlock()

	if db.buckets[name] != nil {
		return yts3.ResourceError(yts3.ErrBucketAlreadyExists, name)
	}

	db.buckets[name] = newBucket(publicKey, name, db.timeSource.Now(), db.nextVersion)
	return nil
}

func (db *Backend) nextVersion() yts3.VersionID {
	v, scr := db.versionGenerator.Next(db.versionScratch)
	db.versionScratch = scr
	return v
}

func (db *Backend) DeleteMulti(publicKey, bucketName string, objects ...string) (result yts3.MultiDeleteResult, err error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	bucket := db.buckets[bucketName]
	if bucket == nil {
		return result, yts3.BucketNotFound(bucketName)
	}

	now := db.timeSource.Now()

	for _, object := range objects {
		dresult, err := bucket.rm(publicKey, bucketName, object, now)
		_ = dresult // FIXME: what to do with rm result in multi delete?

		if err != nil {
			errres := yts3.ErrorResultFromError(err)
			if errres.Code == yts3.ErrInternal {
				// FIXME: log
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

func (db *Backend) HeadObject(publicKey, bucketName, objectName string) (*yts3.Object, error) {
	db.lock.RLock()
	defer db.lock.RUnlock()

	bucket := db.buckets[bucketName]
	if bucket == nil {
		return nil, yts3.BucketNotFound(bucketName)
	}

	obj := bucket.object(objectName)
	if obj == nil || obj.data.deleteMarker {
		return nil, yts3.KeyNotFound(objectName)
	}
	return obj.data.toObject(nil, false)
}

func (db *Backend) GetObject(publicKey, bucketName, objectName string, rangeRequest *yts3.ObjectRangeRequest) (*yts3.Object, error) {
	db.lock.RLock()
	defer db.lock.RUnlock()

	bucket := db.buckets[bucketName]
	if bucket == nil {
		return nil, yts3.BucketNotFound(bucketName)
	}

	obj := bucket.object(objectName)
	if obj == nil || obj.data.deleteMarker {
		return nil, yts3.KeyNotFound(objectName)
	}

	result, err := obj.data.toObject(rangeRequest, true)
	if err != nil {
		return nil, err
	}
	content := getContentByMeta(result.Metadata)
	result.Size = content.Size

	if bucket.versioning != yts3.VersioningEnabled {
		result.VersionID = ""
	}

	c := api.GetClient(publicKey)
	download, errMsg := c.NewDownloadFile(bucketName, objectName, primitive.NilObjectID)
	if errMsg != nil {
		logrus.Printf("%v\n", errMsg)
	}
	result.Contents = download.Load().(io.ReadCloser)

	return result, nil
}

func (db *Backend) DeleteObject(publicKey, bucketName, objectName string) (result yts3.ObjectDeleteResult, rerr error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	bucket := db.buckets[bucketName]
	if bucket == nil {
		return result, yts3.BucketNotFound(bucketName)
	}

	return bucket.rm(publicKey, bucketName, objectName, db.timeSource.Now())
}

func (db *Backend) DeleteBucket(publicKey, bucketName string) error {
	db.lock.Lock()
	defer db.lock.Unlock()

	if db.buckets[bucketName] == nil {
		return yts3.ErrNoSuchBucket
	}

	if db.buckets[bucketName].objects.Len() > 0 {
		return yts3.ResourceError(yts3.ErrBucketNotEmpty, bucketName)
	}
	c := api.GetClient(publicKey)
	bucketAccessor := c.NewBucketAccessor()
	err := bucketAccessor.DeleteBucket(bucketName)
	if err != nil {
		logrus.Println(err)
	}

	return nil
}
