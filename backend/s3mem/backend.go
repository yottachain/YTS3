package s3mem

import (
	"io"
	"strconv"
	"time"

	"github.com/yottachain/YTS3/yts3"
)

var (
	emptyPrefix = &yts3.Prefix{}
)

type Backend struct {
	timeSource       yts3.TimeSource
	versionGenerator *versionGenerator
	versionSeed      int64
	versionSeedSet   bool
	versionScratch   []byte
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
	b := &Backend{}
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

func (db *Backend) nextVersion() yts3.VersionID {
	v, scr := db.versionGenerator.Next(db.versionScratch)
	db.versionScratch = scr
	return v
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
