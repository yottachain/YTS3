package yts3

import (
	"encoding/base64"
	"encoding/xml"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/yottachain/YTCoreService/env"
)

type Yts3 struct {
	storage                 Backend
	versioned               VersionedBackend
	timeSource              TimeSource
	timeSkew                time.Duration
	metadataSizeLimit       int
	integrityCheck          bool
	failOnUnimplementedPage bool
	hostBucket              bool
	uploader                *uploader
	requestID               *env.AtomInt64
	log                     Logger
}

func New(backend Backend, options ...Option) *Yts3 {
	s3 := &Yts3{
		storage:           backend,
		timeSkew:          DefaultSkewLimit,
		metadataSizeLimit: DefaultMetadataSizeLimit,
		integrityCheck:    true,
		uploader:          newUploader(),
		requestID:         env.NewAtomInt64(0),
	}
	// versioned MUST be set before options as one of the options disables it:
	s3.versioned, _ = backend.(VersionedBackend)
	for _, opt := range options {
		opt(s3)
	}
	if s3.log == nil {
		s3.log = DiscardLog()
	}
	if s3.timeSource == nil {
		s3.timeSource = DefaultTimeSource()
	}
	return s3
}

func GetBetweenStr(str, start, end string) string {
	n := strings.Index(str, start)
	if n == -1 {
		n = 0
	}
	str = string([]byte(str)[n:])
	m := strings.Index(str, end)
	if m == -1 {
		m = len(str)
	}
	str = string([]byte(str)[:m])
	return str
}

func (g *Yts3) getBucketLocation(bucketName string, w http.ResponseWriter, r *http.Request) error {
	result := GetBucketLocation{
		Xmlns:              "http://s3.amazonaws.com/doc/2006-03-01/",
		LocationConstraint: "",
	}
	return g.xmlEncoder(w).Encode(result)
}

func listBucketPageFromQuery(query url.Values) (page ListBucketPage, rerr error) {
	maxKeys, err := parseClampedInt(query.Get("max-keys"), DefaultMaxBucketKeys, 0, MaxBucketKeys)
	if err != nil {
		return page, err
	}
	page.MaxKeys = maxKeys
	if _, page.HasMarker = query["marker"]; page.HasMarker {
		page.Marker = query.Get("marker")

	} else if _, page.HasMarker = query["continuation-token"]; page.HasMarker {
		tok, err := base64.URLEncoding.DecodeString(query.Get("continuation-token"))
		if err != nil {
			// FIXME: log
			return page, ErrInvalidToken
		}
		page.Marker = string(tok)

	} else if _, page.HasMarker = query["start-after"]; page.HasMarker {
		page.Marker = query.Get("start-after")
	}
	return page, nil
}

func formatHeaderTime(t time.Time) string {
	tc := t.In(time.UTC)
	return tc.Format("Mon, 02 Jan 2006 15:04:05") + " GMT"
}

func metadataSize(meta map[string]string) int {
	total := 0
	for k, v := range meta {
		total += len(k) + len(v)
	}
	return total
}
func metadataHeaders(headers map[string][]string, at time.Time, sizeLimit int) (map[string]string, error) {
	meta := make(map[string]string)
	for hk, hv := range headers {
		if strings.HasPrefix(hk, "X-Amz-") {
			meta[hk] = hv[0]
		}
	}
	meta["Last-Modified"] = formatHeaderTime(at)
	if sizeLimit > 0 && metadataSize(meta) > sizeLimit {
		return meta, ErrMetadataTooLarge
	}
	return meta, nil
}
func (g *Yts3) nextRequestID() uint64 {
	return uint64(g.requestID.Add(1))
}

func (g *Yts3) httpError(w http.ResponseWriter, r *http.Request, err error) {
	resp := ensureErrorResponse(err, "") // FIXME: request id
	if resp.ErrorCode() == ErrInternal {
		g.log.Print(LogErr, err)
	}
	w.WriteHeader(resp.ErrorCode().Status())
	if r.Method != http.MethodHead {
		if err := g.xmlEncoder(w).Encode(resp); err != nil {
			g.log.Print(LogErr, err)
			return
		}
	}
}

func (g *Yts3) xmlEncoder(w http.ResponseWriter) *xml.Encoder {
	w.Write([]byte(xml.Header))
	w.Header().Set("Content-Type", "application/xml")
	xe := xml.NewEncoder(w)
	xe.Indent("", "  ")
	return xe
}

func (g *Yts3) Server() http.Handler {
	var handler http.Handler = &withCORS{r: http.HandlerFunc(g.routeBase), log: g.log}
	if g.timeSkew != 0 {
		handler = g.timeSkewMiddleware(handler)
	}
	if g.hostBucket {
		handler = g.hostBucketMiddleware(handler)
	}
	return handler
}

func (g *Yts3) timeSkewMiddleware(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, rq *http.Request) {
		timeHdr := rq.Header.Get("x-amz-date")
		if timeHdr != "" {
			// rqTime, _ := time.ParseInLocation("20060102T150405Z", timeHdr, time.Local)
			// at := g.timeSource.Now()
			// skew := at.Sub(rqTime)

			// if skew < -g.timeSkew || skew > g.timeSkew {
			// 	g.httpError(w, rq, requestTimeTooSkewed(at, g.timeSkew))
			// 	return
			// }
		}
		handler.ServeHTTP(w, rq)
	})
}

func (g *Yts3) hostBucketMiddleware(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, rq *http.Request) {
		parts := strings.SplitN(rq.Host, ".", 2)
		bucket := parts[0]
		p := rq.URL.Path
		rq.URL.Path = "/" + bucket
		if p != "/" {
			rq.URL.Path += p
		}
		g.log.Print(LogInfo, p, "=>", rq.URL)

		handler.ServeHTTP(w, rq)
	})
}

func (g *Yts3) writeGetOrHeadObjectResponse(obj *Object, w http.ResponseWriter, r *http.Request) error {
	if obj.IsDeleteMarker {
		w.Header().Set("x-amz-version-id", string(obj.VersionID))
		w.Header().Set("x-amz-delete-marker", "true")
		return KeyNotFound(obj.Name)
	}
	for mk, mv := range obj.Metadata {
		w.Header().Set(mk, mv)
	}
	w.Header().Set("Accept-Ranges", "bytes")
	// w.Header().Set("ETag", `"`+hex.EncodeToString(obj.Hash)+`"`)
	etag := obj.Metadata["ETag"]
	// newETag := etag[1 : len(etag)-1]
	w.Header().Set("ETag", etag)
	w.Header().Set("Content-Length", string(obj.Size))
	if obj.VersionID != "" {
		w.Header().Set("x-amz-version-id", string(obj.VersionID))
	}
	return nil
}

func ErrorResultFromError(err error) ErrorResult {
	switch err := err.(type) {
	case *resourceErrorResponse:
		return ErrorResult{
			Resource:  err.Resource,
			RequestID: err.RequestID,
			Message:   err.Message,
			Code:      err.Code,
		}
	case *ErrorResponse:
		return ErrorResult{
			RequestID: err.RequestID,
			Message:   err.Message,
			Code:      err.Code,
		}
	case Error:
		return ErrorResult{Code: err.ErrorCode()}
	default:
		return ErrorResult{Code: ErrInternal}
	}
}

func (g *Yts3) xmlDecodeBody(rdr io.ReadCloser, into interface{}) error {
	body, err := ioutil.ReadAll(rdr)
	defer rdr.Close()
	if err != nil {
		return err
	}
	if err := xml.Unmarshal(body, into); err != nil {
		return ErrorMessage(ErrMalformedXML, err.Error())
	}
	return nil
}
