package yts3

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/textproto"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
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
	requestID               uint64
	log                     Logger
}

func New(backend Backend, options ...Option) *Yts3 {
	s3 := &Yts3{
		storage:           backend,
		timeSkew:          DefaultSkewLimit,
		metadataSizeLimit: DefaultMetadataSizeLimit,
		integrityCheck:    true,
		uploader:          newUploader(),
		requestID:         0,
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

func (g *Yts3) listBuckets(w http.ResponseWriter, r *http.Request) error {

	Authorization := r.Header.Get("Authorization")
	publicKey := GetBetweenStr(Authorization, "YTA", "/")
	content := publicKey[3:]
	fmt.Println("publicKey:", content)
	// c := api.GetClient(content)
	// bucketAccessor := c.NewBucketAccessor()
	// fmt.Println("UserName:", c.Username)
	// names, err1 := bucketAccessor.ListBucket()

	buckets, err := g.storage.ListBuckets(content)
	if err != nil {
		return err
	}
	// if err1 != nil {
	// 	logrus.Errorf("[ListBucket ]AuthSuper ERR:%s\n", err1)
	// }
	// var buckets []BucketInfo
	// len := len(names)
	// for i := 0; i < len; i++ {
	// 	bucketInfo := BucketInfo{}
	// 	bucketInfo.Name = names[i]
	// 	bucketInfo.CreationDate = NewContentTime(time.Now())
	// 	buckets = append(buckets, bucketInfo)
	// }

	s := &Storage{
		Xmlns:   "http://s3.amazonaws.com/doc/2006-03-01/",
		Buckets: buckets,
		Owner: &UserInfo{
			ID:          "fe7272ea58be830e56fe1663b10fafef",
			DisplayName: "YTS3",
		},
	}

	return g.xmlEncoder(w).Encode(s)
}

func (g *Yts3) getBucketLocation(bucketName string, w http.ResponseWriter, r *http.Request) error {
	g.log.Print(LogInfo, "GET BUCKET LOCATION")

	result := GetBucketLocation{
		Xmlns:              "http://s3.amazonaws.com/doc/2006-03-01/",
		LocationConstraint: "",
	}

	return g.xmlEncoder(w).Encode(result)
}

func (g *Yts3) listBucket(bucketName string, w http.ResponseWriter, r *http.Request) error {
	Authorization := r.Header.Get("Authorization")
	publicKey := GetBetweenStr(Authorization, "YTA", "/")
	content := publicKey[3:]
	// fmt.Println("publicKey:", content)
	// c := api.GetClient(content)
	// userName := c.Username

	// objectAccessor := c.NewObjectAccessor()

	g.log.Print(LogInfo, "LIST BUCKET")

	q := r.URL.Query()
	prefix := prefixFromQuery(q)
	page, err := listBucketPageFromQuery(q)
	if err != nil {
		return err
	}

	isVersion2 := q.Get("list-type") == "2"

	g.log.Print(LogInfo, "bucketname:", bucketName)
	g.log.Print(LogInfo, "prefix    :", prefix)
	g.log.Print(LogInfo, "page      :", fmt.Sprintf("%+v", page))
	// fileName := prefix.Prefix
	// var startObjectID primitive.ObjectID
	// limitCount := 1000
	// ls, err1 := objectAccessor.ListObject(bucketName, fileName, prefix.Prefix, isVersion2, startObjectID, uint32(limitCount))
	// if err1 != nil {
	// 	logrus.Info("Pull objects is error ", err1)
	// }
	// objects := ObjectList{}
	// var contents []*Content
	// if len(ls) > 0 {
	// 	var header map[string]string
	// 	for i := 0; i < len(ls); i++ {
	// 		// object := ObjectList{}
	// 		meta := ls[i].Meta
	// 		header, _ = api.BytesToFileMetaMap(meta, ls[i].VersionId)
	// 		content := Content{}
	// 		content.ETag = "etag"
	// 		content.Key = ls[i].FileName
	// 		contentLen := header["contentLength"]

	// 		content.Size, err = strconv.ParseInt(contentLen, 10, 32)
	// 		if err != nil {
	// 			logrus.Error(err)
	// 		}
	// 		content.Owner.DisplayName = userName
	// 		contents = append(contents, &content)
	// 		// item.FileLength = header["contentLength"]
	// 		// item.TimeStamp = header["x-amz-date"]
	// 		// item.nVerid = ls[i].VersionId
	// 		// objectItems = append(objectItems, item)

	// 	}
	// }

	objects, err := g.storage.ListBucket(content, bucketName, &prefix, page)

	if err != nil {
		if err == ErrInternalPageNotImplemented && !g.failOnUnimplementedPage {
			objects, err = g.storage.ListBucket(content, bucketName, &prefix, ListBucketPage{})
			if err != nil {
				return err
			}

		} else if err == ErrInternalPageNotImplemented && g.failOnUnimplementedPage {
			return ErrNotImplemented
		} else {
			return err
		}
	}

	// objects.Contents = contents
	base := ListBucketResultBase{
		Xmlns:          "http://s3.amazonaws.com/doc/2006-03-01/",
		Name:           bucketName,
		CommonPrefixes: objects.CommonPrefixes,
		Contents:       objects.Contents,
		IsTruncated:    objects.IsTruncated,
		Delimiter:      prefix.Delimiter,
		Prefix:         prefix.Prefix,
		MaxKeys:        page.MaxKeys,
	}

	if !isVersion2 {
		var result = &ListBucketResult{
			ListBucketResultBase: base,
			Marker:               page.Marker,
		}
		if base.Delimiter != "" {
			result.NextMarker = objects.NextMarker
		}
		return g.xmlEncoder(w).Encode(result)

	} else {
		var result = &ListBucketResultV2{
			ListBucketResultBase: base,
			KeyCount:             int64(len(objects.CommonPrefixes) + len(objects.Contents)),
			StartAfter:           q.Get("start-after"),
			ContinuationToken:    q.Get("continuation-token"),
		}
		if objects.NextMarker != "" {
			result.NextContinuationToken = base64.URLEncoding.EncodeToString([]byte(objects.NextMarker))
		}

		if _, ok := q["fetch-owner"]; !ok {
			for _, v := range result.Contents {
				v.Owner = nil
			}
		}

		return g.xmlEncoder(w).Encode(result)
	}
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

func (g *Yts3) createObject(bucket, object string, w http.ResponseWriter, r *http.Request) (err error) {
	g.log.Print(LogInfo, "CREATE OBJECT:", bucket, object)

	meta, err := metadataHeaders(r.Header, g.timeSource.Now(), g.metadataSizeLimit)
	if err != nil {
		return err
	}

	if _, ok := meta["X-Amz-Copy-Source"]; ok {
		// return g.copyObject(bucket, object, meta, w, r)
	}

	contentLength := r.Header.Get("Content-Length")
	if contentLength == "" {
		return ErrMissingContentLength
	}

	size, err := strconv.ParseInt(contentLength, 10, 64)
	if err != nil || size < 0 {
		w.WriteHeader(http.StatusBadRequest)
		return nil
	}

	if len(object) > KeySizeLimit {
		return ResourceError(ErrKeyTooLong, object)
	}

	var md5Base64 string
	if g.integrityCheck {
		md5Base64 = r.Header.Get("Content-MD5")

		if _, ok := r.Header[textproto.CanonicalMIMEHeaderKey("Content-MD5")]; ok && md5Base64 == "" {
			return ErrInvalidDigest // Satisfies s3tests
		}
	}

	// hashingReader is still needed to get the ETag even if integrityCheck
	rdr, err := newHashingReader(r.Body, md5Base64)
	defer r.Body.Close()
	if err != nil {
		return err
	}

	result, err := g.storage.PutObject(bucket, object, meta, rdr, size)
	if err != nil {
		return err
	}

	if result.VersionID != "" {
		g.log.Print(LogInfo, "CREATED VERSION:", bucket, object, result.VersionID)
		w.Header().Set("x-amz-version-id", string(result.VersionID))
	}
	w.Header().Set("ETag", `"`+hex.EncodeToString(rdr.Sum(nil))+`"`)

	return nil
}

func (g *Yts3) createBucket(bucket string, w http.ResponseWriter, r *http.Request) error {
	g.log.Print(LogInfo, "CREATE BUCKET:", bucket)

	if err := ValidateBucketName(bucket); err != nil {
		return err
	}
	if err := g.storage.CreateBucket(bucket); err != nil {
		return err
	}

	w.Header().Set("Location", "/"+bucket)
	w.Write([]byte{})
	return nil
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
	return atomic.AddUint64(&g.requestID, 1)
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

// Create the AWS S3 API
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
			rqTime, _ := time.Parse("20060102T150405Z", timeHdr)
			at := g.timeSource.Now()
			skew := at.Sub(rqTime)

			if skew < -g.timeSkew || skew > g.timeSkew {
				g.httpError(w, rq, requestTimeTooSkewed(at, g.timeSkew))
				return
			}
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

func (g *Yts3) deleteMulti(bucket string, w http.ResponseWriter, r *http.Request) error {
	g.log.Print(LogInfo, "delete multi", bucket)

	var in DeleteRequest

	defer r.Body.Close()
	dc := xml.NewDecoder(r.Body)
	if err := dc.Decode(&in); err != nil {
		return ErrorMessage(ErrMalformedXML, err.Error())
	}

	keys := make([]string, len(in.Objects))
	for i, o := range in.Objects {
		keys[i] = o.Key
	}

	out, err := g.storage.DeleteMulti(bucket, keys...)
	if err != nil {
		return err
	}

	if in.Quiet {
		out.Deleted = nil
	}

	return g.xmlEncoder(w).Encode(out)
}

func (g *Yts3) createObjectBrowserUpload(bucket string, w http.ResponseWriter, r *http.Request) error {
	g.log.Print(LogInfo, "CREATE OBJECT THROUGH BROWSER UPLOAD")

	const _24MB = (1 << 20) * 24
	if err := r.ParseMultipartForm(_24MB); nil != err {
		return ErrMalformedPOSTRequest
	}

	keyValues := r.MultipartForm.Value["key"]
	if len(keyValues) != 1 {
		return ErrIncorrectNumberOfFilesInPostRequest
	}
	key := keyValues[0]

	g.log.Print(LogInfo, "(BUC)", bucket)
	g.log.Print(LogInfo, "(KEY)", key)

	fileValues := r.MultipartForm.File["file"]
	if len(fileValues) != 1 {
		return ErrIncorrectNumberOfFilesInPostRequest
	}
	fileHeader := fileValues[0]

	infile, err := fileHeader.Open()
	if err != nil {
		return err
	}
	defer infile.Close()

	meta, err := metadataHeaders(r.MultipartForm.Value, g.timeSource.Now(), g.metadataSizeLimit)
	if err != nil {
		return err
	}

	if len(key) > KeySizeLimit {
		return ResourceError(ErrKeyTooLong, key)
	}

	rdr, err := newHashingReader(infile, "")
	if err != nil {
		return err
	}

	result, err := g.storage.PutObject(bucket, key, meta, rdr, fileHeader.Size)
	if err != nil {
		return err
	}
	if result.VersionID != "" {
		w.Header().Set("x-amz-version-id", string(result.VersionID))
	}

	w.Header().Set("ETag", `"`+hex.EncodeToString(rdr.Sum(nil))+`"`)
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
