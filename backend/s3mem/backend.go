package s3mem

import (
	"crypto/md5"
	"encoding/hex"
	"io"
	"sync"

	"github.com/yottachain/YTS3/internal/goskipiter"
	"github.com/yottachain/YTS3/yts3"
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

func (db *Backend) ListBuckets(publicKey string) ([]yts3.BucketInfo, error) {
	db.lock.RLock()
	defer db.lock.RUnlock()

	var buckets = make([]yts3.BucketInfo, 0, len(db.buckets))
	for _, bucket := range db.buckets {
		buckets = append(buckets, yts3.BucketInfo{
			Name:         bucket.name,
			CreationDate: bucket.creationDate,
		})
	}

	return buckets, nil
}

func (db *Backend) ListBucket(name string, prefix *yts3.Prefix, page yts3.ListBucketPage) (*yts3.ObjectList, error) {
	if prefix == nil {
		prefix = emptyPrefix
	}

	db.lock.RLock()
	defer db.lock.RUnlock()

	storedBucket := db.buckets[name]
	if storedBucket == nil {
		return nil, yts3.BucketNotFound(name)
	}

	var response = yts3.NewObjectList()
	var iter = goskipiter.New(storedBucket.objects.Iterator())
	var match yts3.PrefixMatch

	if page.Marker != "" {
		iter.Seek(page.Marker)
		iter.Next() // Move to the next item after the Marker
	}

	var cnt int64 = 0

	var lastMatchedPart string

	for iter.Next() {
		item := iter.Value().(*bucketObject)

		if !prefix.Match(item.data.name, &match) {
			continue

		} else if match.CommonPrefix {
			if match.MatchedPart == lastMatchedPart {
				continue // Should not count towards keys
			}
			response.AddPrefix(match.MatchedPart)
			lastMatchedPart = match.MatchedPart

		} else {
			response.Add(&yts3.Content{
				Key:          item.data.name,
				LastModified: yts3.NewContentTime(item.data.lastModified),
				ETag:         `"` + hex.EncodeToString(item.data.hash) + `"`,
				Size:         int64(len(item.data.body)),
			})
		}

		cnt++
		if page.MaxKeys > 0 && cnt >= page.MaxKeys {
			response.NextMarker = item.data.name
			response.IsTruncated = iter.Next()
			break
		}
	}

	return response, nil
}

func (db *Backend) BucketExists(name string) (exists bool, err error) {
	db.lock.RLock()
	defer db.lock.RUnlock()
	return db.buckets[name] != nil, nil
}

func (db *Backend) PutObject(bucketName, objectName string, meta map[string]string, input io.Reader, size int64) (result yts3.PutObjectResult, err error) {
	bts, err := yts3.ReadAll(input, size)
	if err != nil {
		return result, err
	}

	db.lock.Lock()
	defer db.lock.Unlock()

	bucket := db.buckets[bucketName]
	if bucket == nil {
		return result, yts3.BucketNotFound(bucketName)
	}

	hash := md5.Sum(bts)

	item := &bucketData{
		name:         objectName,
		body:         bts,
		hash:         hash[:],
		etag:         `"` + hex.EncodeToString(hash[:]) + `"`,
		metadata:     meta,
		lastModified: db.timeSource.Now(),
	}
	bucket.put(objectName, item)

	return result, nil
}

func (db *Backend) CreateBucket(name string) error {
	db.lock.Lock()
	defer db.lock.Unlock()

	if db.buckets[name] != nil {
		return yts3.ResourceError(yts3.ErrBucketAlreadyExists, name)
	}

	db.buckets[name] = newBucket(name, db.timeSource.Now(), db.nextVersion)
	return nil
}

func (db *Backend) nextVersion() yts3.VersionID {
	v, scr := db.versionGenerator.Next(db.versionScratch)
	db.versionScratch = scr
	return v
}

func (db *Backend) DeleteMulti(bucketName string, objects ...string) (result yts3.MultiDeleteResult, err error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	bucket := db.buckets[bucketName]
	if bucket == nil {
		return result, yts3.BucketNotFound(bucketName)
	}

	now := db.timeSource.Now()

	for _, object := range objects {
		dresult, err := bucket.rm(object, now)
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
