package yts3

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"net/textproto"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTS3/conf"
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
	logrus.Infof("publicKey:", content)
	fmt.Println("publicKey size:", len(content))
	if len(content) > 50 {
		publicKeyLength := strings.Index(content, ":")
		contentNew := content[:publicKeyLength]
		fmt.Println("new::::", contentNew)
		content = contentNew
	}

	buckets, err := g.storage.ListBuckets(content)
	if err != nil {
		return err
	}

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
	logrus.Infof("GET BUCKET LOCATION")

	result := GetBucketLocation{
		Xmlns:              "http://s3.amazonaws.com/doc/2006-03-01/",
		LocationConstraint: "",
	}

	return g.xmlEncoder(w).Encode(result)
}

func (g *Yts3) listBucket(bucketName string, w http.ResponseWriter, r *http.Request) error {
	logrus.Infof("LIST BUCKET")
	Authorization := r.Header.Get("Authorization")
	publicKey := GetBetweenStr(Authorization, "YTA", "/")
	content := publicKey[3:]
	// fmt.Println("publicKey:", len(content))
	if len(content) > 50 {
		publicKeyLength := strings.Index(content, ":")
		contentNew := content[:publicKeyLength]
		// fmt.Println("new::::", contentNew)
		content = contentNew
	}

	q := r.URL.Query()
	prefix := prefixFromQuery(q)
	page, err := listBucketPageFromQuery(q)
	if err != nil {
		return err
	}

	isVersion2 := q.Get("list-type") == "2"

	logrus.Infof("bucketname:", bucketName)
	logrus.Infof("prefix    :", prefix)
	logrus.Infof("page      :", fmt.Sprintf("%+v", page))

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
	logrus.Infof("CREATE OBJECT:", bucket, object)
	Authorization := r.Header.Get("Authorization")
	publicKey := GetBetweenStr(Authorization, "YTA", "/")
	content := publicKey[3:]
	fmt.Println("publicKey:", len(content))
	if len(content) > 50 {
		publicKeyLength := strings.Index(content, ":")
		contentNew := content[:publicKeyLength]
		fmt.Println("new::::", contentNew)
		content = contentNew
	}
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

	result, err := g.storage.PutObject(content, bucket, object, meta, rdr, size)
	if err != nil {
		return err
	}

	if result.VersionID != "" {
		logrus.Infof("CREATED VERSION:", bucket, object, result.VersionID)
		w.Header().Set("x-amz-version-id", string(result.VersionID))
	}
	w.Header().Set("ETag", `"`+hex.EncodeToString(rdr.Sum(nil))+`"`)

	return nil
}

func (g *Yts3) createBucket(bucket string, w http.ResponseWriter, r *http.Request) error {
	logrus.Infof("CREATE BUCKET:", bucket)

	Authorization := r.Header.Get("Authorization")
	publicKey := GetBetweenStr(Authorization, "YTA", "/")
	content := publicKey[3:]
	fmt.Println("publicKey:", len(content))
	if len(content) > 50 {
		publicKeyLength := strings.Index(content, ":")
		contentNew := content[:publicKeyLength]
		fmt.Println("new::::", contentNew)
		content = contentNew
	}
	if err := ValidateBucketName(bucket); err != nil {
		return err
	}
	if err := g.storage.CreateBucket(content, bucket); err != nil {
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

func (g *Yts3) getObject(bucket, object string, versionID VersionID, w http.ResponseWriter, r *http.Request) error {

	logrus.Infof("GET OBJECT")
	logrus.Infof("Bucket:", bucket)
	logrus.Infof("└── Object:", object)
	Authorization := r.Header.Get("Authorization")

	// debug 调试用
	if Authorization == "" {
		logrus.Infof("publicKey is null")
		// return
	}

	publicKey := GetBetweenStr(Authorization, "YTA", "/")
	content := publicKey[3:]
	if len(content) > 50 {
		publicKeyLength := strings.Index(content, ":")
		contentNew := content[:publicKeyLength]
		content = contentNew
	}
	rnge, err := parseRangeHeader(r.Header.Get("Range"))
	if err != nil {
		return err
	}

	var obj *Object

	{
		if versionID == "" {
			obj, err = g.storage.GetObject(content, bucket, object, rnge)
			if err != nil {
				return err
			}
		} else {

		}
	}

	if obj == nil {
		logrus.Errorf("unexpected nil object for key", bucket, object)
		return ErrInternal
	}
	defer obj.Contents.Close()

	if err := g.writeGetOrHeadObjectResponse(obj, w, r); err != nil {
		return err
	}

	obj.Range.writeHeader(obj.Size, w)

	if _, err := io.Copy(w, obj.Contents); err != nil {
		return err
	}

	return nil
}

func (g *Yts3) headObject(bucket, object string, versionID VersionID, w http.ResponseWriter, r *http.Request) error {

	logrus.Infof("HEAD OBJECT")
	logrus.Infof("Bucket:", bucket)
	logrus.Infof("└── Object:", object)
	Authorization := r.Header.Get("Authorization")
	publicKey := GetBetweenStr(Authorization, "YTA", "/")
	content := publicKey[3:]
	if len(content) > 50 {
		publicKeyLength := strings.Index(content, ":")
		contentNew := content[:publicKeyLength]

		content = contentNew
	}
	logrus.Infof("publicKey=", content)
	obj, err := g.storage.HeadObject(content, bucket, object)
	if err != nil {
		return err
	}
	if obj == nil {
		logrus.Errorf("unexpected nil object for key", bucket, object)
		return ErrInternal
	}
	defer obj.Contents.Close()

	if err := g.writeGetOrHeadObjectResponse(obj, w, r); err != nil {
		return err
	}

	w.Header().Set("Content-Length", fmt.Sprintf("%d", obj.Size))

	return nil
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
	w.Header().Set("ETag", `"`+hex.EncodeToString(obj.Hash)+`"`)

	if obj.VersionID != "" {
		w.Header().Set("x-amz-version-id", string(obj.VersionID))
	}
	return nil
}

func (g *Yts3) deleteMulti(bucket string, w http.ResponseWriter, r *http.Request) error {
	logrus.Infof("delete multi", bucket)
	Authorization := r.Header.Get("Authorization")
	publicKey := GetBetweenStr(Authorization, "YTA", "/")
	content := publicKey[3:]
	if len(content) > 50 {
		publicKeyLength := strings.Index(content, ":")
		contentNew := content[:publicKeyLength]
		content = contentNew
	}
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

	out, err := g.storage.DeleteMulti(content, bucket, keys...)
	if err != nil {
		return err
	}

	if in.Quiet {
		out.Deleted = nil
	}

	return g.xmlEncoder(w).Encode(out)
}

func (g *Yts3) createObjectBrowserUpload(bucket string, w http.ResponseWriter, r *http.Request) error {
	logrus.Infof("CREATE OBJECT THROUGH BROWSER UPLOAD")
	Authorization := r.Header.Get("Authorization")
	publicKey := GetBetweenStr(Authorization, "YTA", "/")
	content := publicKey[3:]
	if len(content) > 50 {
		publicKeyLength := strings.Index(content, ":")
		contentNew := content[:publicKeyLength]
		content = contentNew
	}
	const _24MB = (1 << 20) * 24
	if err := r.ParseMultipartForm(_24MB); nil != err {
		return ErrMalformedPOSTRequest
	}

	keyValues := r.MultipartForm.Value["key"]
	if len(keyValues) != 1 {
		return ErrIncorrectNumberOfFilesInPostRequest
	}
	key := keyValues[0]

	logrus.Infof("(BUC)", bucket)
	logrus.Infof("(KEY)", key)

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

	result, err := g.storage.PutObject(content, bucket, key, meta, rdr, fileHeader.Size)
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

func (g *Yts3) deleteObject(bucket, object string, w http.ResponseWriter, r *http.Request) error {
	logrus.Infof("DELETE:", bucket, object)
	Authorization := r.Header.Get("Authorization")
	publicKey := GetBetweenStr(Authorization, "YTA", "/")
	content := publicKey[3:]
	if len(content) > 50 {
		publicKeyLength := strings.Index(content, ":")
		contentNew := content[:publicKeyLength]
		content = contentNew
	}
	result, err := g.storage.DeleteObject(content, bucket, object)
	if err != nil {
		logrus.Errorf("Error:", err)
		return err
	}

	if result.IsDeleteMarker {
		w.Header().Set("x-amz-delete-marker", "true")
	} else {
		w.Header().Set("x-amz-delete-marker", "false")
	}

	if result.VersionID != "" {
		w.Header().Set("x-amz-version-id", string(result.VersionID))
	}

	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (g *Yts3) deleteBucket(bucket string, w http.ResponseWriter, r *http.Request) error {
	logrus.Infof("DELETE BUCKET:", bucket)
	Authorization := r.Header.Get("Authorization")
	publicKey := GetBetweenStr(Authorization, "YTA", "/")
	content := publicKey[3:]
	if len(content) > 50 {
		publicKeyLength := strings.Index(content, ":")
		contentNew := content[:publicKeyLength]
		content = contentNew
	}
	fmt.Println("publicKey:", content)
	if err := g.storage.DeleteBucket(content, bucket); err != nil {
		logrus.Errorf("Error Msg:", err)
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (g *Yts3) listMultipartUploadParts(bucket, object string, uploadID UploadID, w http.ResponseWriter, r *http.Request) error {
	query := r.URL.Query()

	marker, err := parseClampedInt(query.Get("part-number-marker"), 0, 0, math.MaxInt64)
	if err != nil {
		logrus.Errorf("Error Msg:", err)
		return ErrInvalidURI
	}

	maxParts, err := parseClampedInt(query.Get("max-parts"), DefaultMaxUploadParts, 0, MaxUploadPartsLimit)
	if err != nil {
		logrus.Errorf("Error Msg:", err)
		return ErrInvalidURI
	}

	out, err := g.uploader.ListParts(bucket, object, uploadID, int(marker), maxParts)
	if err != nil {
		logrus.Errorf("Error Msg:", err)
		return err
	}

	return g.xmlEncoder(w).Encode(out)
}

func (g *Yts3) putMultipartUploadPart(bucket, object string, uploadID UploadID, w http.ResponseWriter, r *http.Request) error {

	partNumber, err := strconv.ParseInt(r.URL.Query().Get("partNumber"), 10, 0)
	if err != nil || partNumber <= 0 || partNumber > MaxUploadPartNumber {
		return ErrInvalidPart
	}

	size, err := strconv.ParseInt(r.Header.Get("Content-Length"), 10, 64)
	if err != nil || size <= 0 {
		return ErrMissingContentLength
	}

	upload, err := g.uploader.Get(bucket, object, uploadID)
	if err != nil {
		logrus.Errorf("PutMultipartUploadPart Error Msg:", err)
		return err
	}

	defer r.Body.Close()
	var rdr io.Reader = r.Body

	if g.integrityCheck {
		md5Base64 := r.Header.Get("Content-MD5")
		if _, ok := r.Header[textproto.CanonicalMIMEHeaderKey("Content-MD5")]; ok && md5Base64 == "" {
			return ErrInvalidDigest
		}

		if md5Base64 != "" {
			var err error
			rdr, err = newHashingReader(rdr, md5Base64)
			if err != nil {
				return err
			}
		}
	}

	body, err := ReadAll(rdr, size)
	if err != nil {
		return err
	}

	if int64(len(body)) != r.ContentLength {
		return ErrIncompleteBody
	}

	etag, err := upload.AddPart(bucket, object, int(partNumber), g.timeSource.Now(), body)
	if err != nil {
		return err
	}

	w.Header().Add("ETag", etag)
	return nil
}

func (g *Yts3) abortMultipartUpload(bucket, object string, uploadID UploadID, w http.ResponseWriter, r *http.Request) error {
	logrus.Infof("abort multipart upload", bucket, object, uploadID)
	if _, err := g.uploader.Complete(bucket, object, uploadID); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (g *Yts3) completeMultipartUpload(bucket, object string, uploadID UploadID, w http.ResponseWriter, r *http.Request) error {
	logrus.Infof("complete multipart upload", bucket, object, uploadID)

	Authorization := r.Header.Get("Authorization")
	publicKey := GetBetweenStr(Authorization, "YTA", "/")
	content := publicKey[3:]
	if len(content) > 50 {
		publicKeyLength := strings.Index(content, ":")
		contentNew := content[:publicKeyLength]
		content = contentNew
	}
	var in CompleteMultipartUploadRequest
	if err := g.xmlDecodeBody(r.Body, &in); err != nil {
		logrus.Errorf("xmlDecodeBody ERR :", err)
		return err
	}
	defer r.Body.Close()
	upload, err := g.uploader.Complete(bucket, object, uploadID)
	if err != nil {
		logrus.Errorf("upload complete ERR :", err)
		return err
	}

	fileBody, etag, err := upload.Reassemble(&in)
	if err != nil {
		logrus.Errorf("fileBody, etag ERR :", err)
		return err
	}
	result, err := g.storage.PutObject(content, bucket, object, upload.Meta, bytes.NewReader(fileBody), int64(len(fileBody)))
	if err != nil {
		logrus.Errorf("put boject ERR :", err)
		// return err
	}
	if result.VersionID != "" {
		w.Header().Set("x-amz-version-id", string(result.VersionID))
	}

	return g.xmlEncoder(w).Encode(&CompleteMultipartUploadResult{
		ETag:   etag,
		Bucket: bucket,
		Key:    object,
	})
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

func (g *Yts3) listMultipartUploads(bucket string, w http.ResponseWriter, r *http.Request) error {
	query := r.URL.Query()
	prefix := prefixFromQuery(query)
	marker := uploadListMarkerFromQuery(query)

	maxUploads, err := parseClampedInt(query.Get("max-uploads"), DefaultMaxUploads, 0, MaxUploadsLimit)
	if err != nil {
		return ErrInvalidURI
	}
	if maxUploads == 0 {
		maxUploads = DefaultMaxUploads
	}

	out, err := g.uploader.List(bucket, marker, prefix, maxUploads)
	if err != nil {
		return err
	}

	return g.xmlEncoder(w).Encode(out)
}

func (g *Yts3) initiateMultipartUpload(bucket, object string, w http.ResponseWriter, r *http.Request) error {
	logrus.Infof("initiate multipart upload")

	iniPath := "conf/yotta_config.ini"
	cfg, err := conf.CreateConfig(iniPath)
	if err != nil {
		logrus.Errorf("Error Msg:", err)
		return err
	}
	cache := cfg.GetCacheInfo("directory")
	directory := cache + "/" + bucket

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

	meta, err := metadataHeaders(r.Header, g.timeSource.Now(), g.metadataSizeLimit)
	if err != nil {
		return err
	}
	if err := g.ensureBucketExists(bucket); err != nil {
		return err
	}

	upload := g.uploader.Begin(bucket, object, meta, g.timeSource.Now())
	out := InitiateMultipartUpload{
		UploadID: upload.ID,
		Bucket:   bucket,
		Key:      object,
	}
	return g.xmlEncoder(w).Encode(out)
}

func (g *Yts3) ensureBucketExists(bucket string) error {
	exists, err := g.storage.BucketExists(bucket)
	if err != nil {
		return err
	}
	if !exists {
		return ResourceError(ErrNoSuchBucket, bucket)
	}
	return nil
}
