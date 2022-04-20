package yts3

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/textproto"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/api"
	"github.com/yottachain/YTCoreService/env"
)

var CreateObjectNum *int32 = new(int32)

func (g *Yts3) createObject(bucket, object string, w http.ResponseWriter, r *http.Request) (err error) {
	MaxCreateObjNum := env.GetConfig().GetRangeInt("MaxCreateObjNum", 20, 100, 50)
	count := atomic.AddInt32(CreateObjectNum, 1)
	defer atomic.AddInt32(CreateObjectNum, -1)
	logrus.Infof("[S3Upload]CreateObject request number: %d\n", count)
	if count > int32(MaxCreateObjNum) {
		logrus.Error("[S3Upload]CreateObject request too frequently.\n")
		return errors.New("CreateObject request too frequently.\n")
	}
	logrus.Infof("[S3Upload]CREATE OBJECT:%s/%s\n", bucket, object)
	Authorization := r.Header.Get("Authorization")
	if Authorization == "" {
		logrus.Error("[[S3Upload]]ErrAuthorization\n")
		return ErrAuthorization
	}
	publicKey := GetBetweenStr(Authorization, "YTA", "/")
	content := publicKey[3:]
	if len(content) > 50 {
		publicKeyLength := strings.Index(content, ":")
		contentNew := content[:publicKeyLength]
		content = contentNew
	}
	meta, err := metadataHeaders(r.Header, g.timeSource.Now(), g.metadataSizeLimit)
	if err != nil {
		return err
	}
	if _, ok := meta["X-Amz-Copy-Source"]; ok {
		return g.copyObject(bucket, object, meta, w, r)
	}
	contentLength := r.Header.Get("Content-Length")
	if contentLength == "" {
		return ErrMissingContentLength
	} else if contentLength == "154" {
		var rdr io.Reader = r.Body
		lnn := 10485760
		body, err := ReadAll(rdr, int64(lnn))
		if err != nil {
			contentLength = fmt.Sprintf("%d", len(body))
		}
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
			return ErrInvalidDigest
		}
	}
	rdr, err := newHashingReader(r.Body, md5Base64)
	if err != nil {
		return err
	}
	uri := r.URL.Path
	if strings.HasSuffix(uri, "/") {
		var bts []byte
		metazero := make(map[string]string)
		hashz := md5.Sum(bts)
		metazero["ETag"] = hex.EncodeToString(hashz[:])
		metazero["contentLength"] = "0"
		metadata2, err2 := api.FileMetaMapTobytes(metazero)
		if err2 != nil {
			logrus.Errorf("[S3Upload]FileMetaMapTobytes err:%s\n", err2)
			return
		}
		c := api.GetClient(content)
		errzero := c.NewObjectAccessor().CreateObject(bucket, object, env.ZeroLenFileID(), metadata2)
		if errzero != nil {
			logrus.Errorf("[S3Upload]Save meta err:%s\n", errzero)
			return
		}
	} else {
		result, err := g.storage.PutObject(content, bucket, object, meta, rdr, size, count)
		if err != nil {
			return err
		}
		if result.VersionID != "" {
			logrus.Infof("[S3Upload]CREATED VERSION:%s%s%d\n", bucket, object, result.VersionID)
			w.Header().Set("x-amz-version-id", string(result.VersionID))
		}
	}
	w.Header().Set("ETag", `"`+hex.EncodeToString(rdr.Sum(nil))+`"`)
	return nil
}

func (g *Yts3) createObjectBrowserUpload(bucket string, w http.ResponseWriter, r *http.Request) error {
	MaxCreateObjNum := env.GetConfig().GetRangeInt("MaxCreateObjNum", 20, 100, 50)
	count := atomic.AddInt32(CreateObjectNum, 1)
	defer atomic.AddInt32(CreateObjectNum, -1)
	logrus.Infof("[S3Upload]createObjectBrowserUpload request number: %d\n", count)
	if count > int32(MaxCreateObjNum) {
		return errors.New("createObjectBrowserUpload request too frequently.\n")
	}
	logrus.Infof("[S3Upload]CREATE OBJECT THROUGH BROWSER UPLOAD\n")
	Authorization := r.Header.Get("Authorization")
	if Authorization == "" {
		logrus.Error("[S3Upload]createObjectBrowserUpload ErrAuthorization\n")
		return ErrAuthorization
	}
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
	logrus.Infof("[S3Upload](BUC)%s,(KEY)%s\n", bucket, key)
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
	result, err := g.storage.PutObject(content, bucket, key, meta, rdr, fileHeader.Size, count)
	if err != nil {
		return err
	}
	if result.VersionID != "" {
		w.Header().Set("x-amz-version-id", string(result.VersionID))
	}
	w.Header().Set("ETag", `"`+hex.EncodeToString(rdr.Sum(nil))+`"`)
	return nil
}
