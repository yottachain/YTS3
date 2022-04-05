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

	"github.com/patrickmn/go-cache"
	"github.com/ryszard/goskiplist/skiplist"
	uuid "github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/api"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/pkt"

	"github.com/yottachain/YTS3/yts3"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

var (
	emptyPrefix = &yts3.Prefix{}
	//RegDb init
	RegDb *Backend
	//UserAllBucketsCACHE cache
	UserAllBucketsCACHE = cache.New(time.Duration(6000000)*time.Minute, time.Duration(6000000)*time.Minute)
	// emptyVersionsPage = &yts3.ListBucketVersionsPage{}
)

type Backend struct {
	buckets          map[string]*bucket
	timeSource       yts3.TimeSource
	versionGenerator *versionGenerator
	versionSeed      int64
	versionSeedSet   bool
	versionScratch   []byte
	Lock             sync.RWMutex
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
	db.Lock.RLock()
	defer db.Lock.RUnlock()
	c := api.GetClient(publicKey)
	if c == nil {
		return nil, yts3.ResourceError(yts3.ErrInvalidAccessKeyID, "YTA"+publicKey)
	}
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
		lastModifyTime, err := time.ParseInLocation("20060102T150405Z", lastModifyString, time.Local)
		if err == nil {
			content.LastModified = yts3.ContentTime{lastModifyTime}
		}
	}
	return &content
}

//ListBucket s3 listObjects
func (db *Backend) ListBucket(publicKey, name string, prefix *yts3.Prefix, page yts3.ListBucketPage) (*yts3.ObjectList, error) {
	db.Lock.RLock()
	defer db.Lock.RUnlock()
	if len(db.buckets) == 0 {
		if v, has := UserAllBucketsCACHE.Get(publicKey); has {
			if RegDb, ok := v.(*Backend); ok {
				db = RegDb
			}
		}
		/*
			v, _ := UserAllBucketsCACHE.Get(publicKey)
			// logrus.Infof("found::::", found)
			// logrus.Infof("value::", v)
			RegDb = v.(*Backend)

			if RegDb != nil {
				db = RegDb
			}
		*/
	}
	var response = yts3.NewObjectList()
	c := api.GetClient(publicKey)
	sklist := skiplist.NewStringMap()
	filename := ""
	for {
		objectAccessor := c.NewObjectAccessor()
		items, err := objectAccessor.ListObject(name, filename, prefix.Prefix, false, primitive.ObjectID{}, 1000)
		if err != nil {
			return response, fmt.Errorf(err.String())
		}
		logrus.Infof("items len %d\n", len(items))
		for _, v := range items {
			meta, err := api.BytesToFileMetaMap(v.Meta, primitive.ObjectID{})
			if err != nil {
				continue
			}
			t := time.Unix(v.FileId.Timestamp().Unix(), 0)
			s := t.Format("20060102T150405Z")
			//ts, _ := time.ParseInLocation("2006-01-02 15:04:05", s, time.Local)
			meta["x-amz-meta-s3b-last-modified"] = s
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
					lastModified: content.LastModified.Time,
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
	db.Lock.RLock()
	defer db.Lock.RUnlock()

	return db.buckets[name] != nil, nil
}

func objectExists(publicKey, bucket, objectKey string) (exists bool) {

	c := api.GetClient(publicKey)

	isExist, err := c.NewObjectAccessor().ObjectExist(bucket, objectKey)
	if err != nil {
		logrus.Errorf("err:%s\n", err)
	}
	return isExist
}

//PutObject upload file
func (db *Backend) PutObject(publicKey, bucketName, objectName string, meta map[string]string, input io.Reader, size int64) (result yts3.PutObjectResult, err error) {

	//isExist := objectExists(publicKey, bucketName, objectName)

	//if isExist == true {
	//	return result, yts3.ErrNotImplemented
	//}

	db.Lock.Lock()
	defer db.Lock.Unlock()
	if len(db.buckets) == 0 {
		if v, has := UserAllBucketsCACHE.Get(publicKey); has {
			if RegDb, ok := v.(*Backend); ok {
				db = RegDb
			}
		}
		/*
			v, _ := UserAllBucketsCACHE.Get(publicKey)
			RegDb = v.(*Backend)

			if RegDb != nil {
				db = RegDb
			}
		*/
	}
	bucket := db.buckets[bucketName]
	if bucket == nil {
		return result, yts3.BucketNotFound(bucketName)
	}
	c := api.GetClient(publicKey)
	// upload := c.NewUploadObject()
	// iniPath := env.YTFS_HOME + "conf/yotta_config.ini"
	// cfg, err := conf.CreateConfig(iniPath)
	// cache := cfg.GetCacheInfo("directory")
	s3cache := env.GetS3Cache()
	u1, err := uuid.FromString("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
	if err != nil {
		fmt.Printf("Something went wrong: %s", err)
	}
	fmt.Printf("Successfully parsed: %s\n", u1)
	directory := s3cache + bucketName + "/" + u1.String()

	var hash []byte
	var bts []byte
	var header map[string]string
	header = make(map[string]string)
	if size >= 10485760 {

		errw := writeCacheFile(directory, objectName, input)
		if errw != nil {
			logrus.Errorf("【PutObject】 write cache error:%s\n", errw)
			return
		}
		filePath := directory + "/" + objectName
		md5bytes, erre := c.UploadFile(filePath, bucketName, objectName)
		// erre := upload.UploadFile(filePath)
		if erre != nil {
			logrus.Errorf("Err: %s\n", erre)
			return
		}
		hash = md5bytes
	} else {
		bts, err = yts3.ReadAll(input, size)
		if err != nil {
			return result, err
		}
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

	if size < 10485760 {
		if size > 0 {
			// err1 := upload.UploadBytes(item.body)
			md5Hash, err1 := c.UploadBytes(item.body, bucketName, objectName)
			if err1 != nil {
				logrus.Printf("ERR:%s\n", err1)
				return
			}
			hash = md5Hash
			// logrus.Infof("upload hash result:%s\n", hex.EncodeToString(hash))
		}

	}
	//update meta data
	if size == 0 {
		hashz := md5.Sum(bts)
		logrus.Infof("zero file Etag:%s \n", hex.EncodeToString(hashz[:]))
		header["ETag"] = hex.EncodeToString(hashz[:])
	} else {
		// hash = upload.GetMD5()
		header["ETag"] = hex.EncodeToString(hash[:])
		logrus.Infof("[ "+objectName+" ]"+"is ETag:%s\n", hex.EncodeToString(hash[:]))
	}

	header["contentLength"] = strconv.FormatInt(size, 10)
	metadata2, err2 := api.FileMetaMapTobytes(header)

	if err2 != nil {
		logrus.Errorf("[FileMetaMapTobytes ]:%s\n", err2)
		return
	}

	if size == 0 {
		errzero := c.NewObjectAccessor().CreateObject(bucketName, objectName, primitive.NewObjectID(), metadata2)
		if errzero != nil {
			logrus.Errorf("[Save meta data ]:%s\n", errzero)
			return
		}
	} else {
		// err3 := c.NewObjectAccessor().CreateObject(bucketName, objectName, upload.VNU, metadata2)
		// if err3 != nil {
		// 	logrus.Errorf("[Save meta data ]:%s\n", err3)
		// 	return
		// }
	}

	// if size >= 10485760 {
	// 	filePath := directory + "/" + objectName
	// 	deleteCacheFile(filePath)
	// }
	logrus.Infof("File upload success,file md5 value : %s\n", hex.EncodeToString(hash[:]))

	return result, nil
}

//MultipartUpload 分段上传
func (db *Backend) MultipartUpload(publicKey, bucketName, objectName string, partsPath []string, size int64) (result yts3.PutObjectResult, err error) {
	db.Lock.Lock()
	defer db.Lock.Unlock()
	if len(db.buckets) == 0 {
		if v, has := UserAllBucketsCACHE.Get(publicKey); has {
			if RegDb, ok := v.(*Backend); ok {
				db = RegDb
			}
		}
		/*
			v, _ := UserAllBucketsCACHE.Get(publicKey)
			RegDb = v.(*Backend)

			if RegDb != nil {
				db = RegDb
			}
		*/
	}
	bucket := db.buckets[bucketName]
	if bucket == nil {
		return result, yts3.BucketNotFound(bucketName)
	}
	c := api.GetClient(publicKey)

	var meta map[string]string

	var hash []byte
	var bts []byte
	// var header map[string]string
	// header = make(map[string]string)

	// errB := upload.UploadMultiFile(partsPath)
	md5Bytes, errB := c.UploadMultiPartFile(partsPath, bucketName, objectName)
	if errB != nil {
		logrus.Errorf("errB:%s\n", errB)
		return
	}
	// hash = upload.GetMD5()
	// hash = md5.Sum(hash1)

	hash = md5Bytes
	item := &bucketData{
		name:         objectName,
		body:         bts,
		hash:         hash[:],
		etag:         `"` + hex.EncodeToString(hash[:]) + `"`,
		metadata:     meta,
		lastModified: db.timeSource.Now(),
	}
	logrus.Infof("upload hash etag:%s\n", item.etag)

	//update meta data

	// header["ETag"] = hex.EncodeToString(hash[:])
	// header["contentLength"] = strconv.FormatInt(size, 10)
	// metadata2, err2 := api.FileMetaMapTobytes(header)

	// if err2 != nil {
	// 	logrus.Errorf("[FileMetaMapTobytes ]:%s\n", err2)
	// 	return
	// }

	// err3 := c.NewObjectAccessor().CreateObject(bucketName, objectName, upload.VNU, metadata2)
	// if err3 != nil {
	// 	logrus.Errorf("[Save meta data ]:%s\n", err3)
	// }

	logrus.Infof("File upload success,file md5 value : %s\n", hex.EncodeToString(hash[:]))

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
	logrus.Infof("file directory:%s\n", directory)
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
	db.Lock.Lock()
	defer db.Lock.Unlock()
	if len(db.buckets) == 0 {
		if v, has := UserAllBucketsCACHE.Get(publicKey); has {
			if RegDb, ok := v.(*Backend); ok {
				db = RegDb
			}
		}
		/*
			v, _ := UserAllBucketsCACHE.Get(publicKey)
			RegDb = v.(*Backend)

			if RegDb != nil {
				db = RegDb
			}
		*/
	}
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
	db.Lock.Lock()
	defer db.Lock.Unlock()
	if len(db.buckets) == 0 {
		if v, has := UserAllBucketsCACHE.Get(publicKey); has {
			if RegDb, ok := v.(*Backend); ok {
				db = RegDb
			}
		}
		/*
			v, _ := UserAllBucketsCACHE.Get(publicKey)
			RegDb = v.(*Backend)

			if RegDb != nil {
				db = RegDb
			}
		*/
	}
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
	db.Lock.RLock()
	defer db.Lock.RUnlock()
	if len(db.buckets) == 0 {
		if v, has := UserAllBucketsCACHE.Get(publicKey); has {
			if RegDb, ok := v.(*Backend); ok {
				db = RegDb
			}
		}
		/*
			v, _ := UserAllBucketsCACHE.Get(publicKey)
			RegDb = v.(*Backend)

			if RegDb != nil {
				db = RegDb
			}
		*/
	}
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

type ContentReader struct {
	io.ReadCloser
}

func (cr *ContentReader) Close() error {
	return cr.ReadCloser.Close()
}

func (cr *ContentReader) Read(buf []byte) (int, error) {
	var nc int
	var err2 error
	for i := 0; i < 5; i++ {
		n, err := cr.ReadCloser.Read(buf)
		if err == nil {
			nc = n
			break
		} else {
			err2 = err
		}
	}
	return nc, err2
}

func (db *Backend) GetObjectV2(publicKey, bucketName, objectName string, rangeRequest *yts3.ObjectRangeRequest, prefix *yts3.Prefix, page yts3.ListBucketPage) (*yts3.Object, error) {
	db.Lock.RLock()
	defer db.Lock.RUnlock()

	isExistObject := objectExists(publicKey, bucketName, objectName)
	if isExistObject {
		c := api.GetClient(publicKey)
		download, errMsg := c.NewDownloadLastVersion(bucketName, objectName)
		if errMsg != nil {
			//logrus.Errorf("Err:%s\n", errMsg)
			return nil, yts3.ErrNoSuchKey
		}
		if len(db.buckets) == 0 {
			if v, has := UserAllBucketsCACHE.Get(publicKey); has {
				if RegDb, ok := v.(*Backend); ok {
					db = RegDb
				}
			}
			/*
				v, _ := UserAllBucketsCACHE.Get(publicKey)
				RegDb = v.(*Backend)

				if RegDb != nil {
					db = RegDb
				}
			*/
		}
		bucket := db.buckets[bucketName]
		if bucket == nil {
			return nil, yts3.BucketNotFound(bucketName)
		}

		db.ListBucket(publicKey, bucketName, prefix, page)

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
		eg := content.ETag
		logrus.Infof("Etag:%s\n", eg)

		hash, err := hex.DecodeString(eg)
		if err != nil {
			fmt.Println(err)
		}
		aa := hex.EncodeToString(hash[:])
		logrus.Infof("aa:%s\n", aa)
		result.Hash = hash
		return result, nil
	} else {
		return nil, nil
	}
}

func (db *Backend) GetObject(publicKey, bucketName, objectName string, rangeRequest *yts3.ObjectRangeRequest) (*yts3.Object, error) {
	db.Lock.RLock()
	defer db.Lock.RUnlock()

	isExistObject := objectExists(publicKey, bucketName, objectName)
	if isExistObject {
		c := api.GetClient(publicKey)
		download, errMsg := c.NewDownloadLastVersion(bucketName, objectName)
		if errMsg != nil {
			logrus.Errorf("Err:%s\n", errMsg)
		}
		if len(db.buckets) == 0 {
			if v, has := UserAllBucketsCACHE.Get(publicKey); has {
				if RegDb, ok := v.(*Backend); ok {
					db = RegDb
				}
			}
			/*
				v, _ := UserAllBucketsCACHE.Get(publicKey)
				RegDb = v.(*Backend)

				if RegDb != nil {
					db = RegDb
				}
			*/
		}
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
		eg := content.ETag
		logrus.Infof("Etag:%s\n", eg)

		hash, err := hex.DecodeString(eg)
		if err != nil {
			fmt.Println(err)
		}
		aa := hex.EncodeToString(hash[:])
		logrus.Infof("aa:%s\n", aa)
		result.Hash = hash
		return result, nil
	} else {
		return nil, nil
	}
}

func (db *Backend) DeleteObject(publicKey, bucketName, objectName string) (result yts3.ObjectDeleteResult, rerr error) {
	db.Lock.Lock()
	defer db.Lock.Unlock()

	bucket := db.buckets[bucketName]
	//if bucket == nil {
	//	return result, yts3.BucketNotFound(bucketName)
	//}

	return bucket.rm(publicKey, bucketName, objectName, db.timeSource.Now())
}

func (db *Backend) DeleteBucket(publicKey, bucketName string) error {
	db.Lock.Lock()
	defer db.Lock.Unlock()

	if db.buckets[bucketName] == nil {
		return yts3.ErrNoSuchBucket
	}

	c := api.GetClient(publicKey)

	bucketAccessor := c.NewBucketAccessor()

	err := bucketAccessor.DeleteBucket(bucketName)
	if err != nil {
		if err.Code == pkt.BUCKET_NOT_EMPTY {
			return yts3.ResourceError(yts3.ErrBucketNotEmpty, bucketName)
		} else if err.Code == pkt.INVALID_BUCKET_NAME {
			return yts3.ResourceError(yts3.ErrNoSuchBucket, bucketName)
		}
		logrus.Errorf("Error msg: %s\n", err)
	}
	delete(db.buckets, bucketName)

	return nil
}
