package yts3

import (
	"errors"
	"io"
	"math"
	"net/http"
	"net/textproto"
	"os"
	"strconv"
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/env"
)

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
	logrus.Infof("[MultipartUpload]initiate multipart upload\n")
	s3cache := env.GetS3Cache()
	directory := s3cache + "/" + bucket + "/"
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
	meta, err := metadataHeaders(r.Header, g.timeSource.Now(), g.metadataSizeLimit)
	if err != nil {
		logrus.Errorf("[MultipartUpload]metadataHeaders err::::: %s\n", err)
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

func (g *Yts3) completeMultipartUpload(bucket, object string, uploadID UploadID, w http.ResponseWriter, r *http.Request) error {
	logrus.Infof("[MultipartUpload]complete multipart upload %s %s %s\n", bucket, object, uploadID)
	Authorization := r.Header.Get("Authorization")
	if Authorization == "" {
		logrus.Error("[MultipartUpload]completeMultipartUpload ErrAuthorization\n")
		return ErrAuthorization
	}
	publicKey := GetBetweenStr(Authorization, "YTA", "/")
	content := publicKey[3:]
	if len(content) > 50 {
		publicKeyLength := strings.Index(content, ":")
		contentNew := content[:publicKeyLength]
		content = contentNew
	}
	var in CompleteMultipartUploadRequest
	if err := g.xmlDecodeBody(r.Body, &in); err != nil {
		logrus.Errorf("[MultipartUpload]xmlDecodeBody ERR :%s\n", err)
		return err
	}
	defer r.Body.Close()
	upload, err := g.uploader.Complete(bucket, object, uploadID)
	if err != nil {
		logrus.Errorf("[MultipartUpload]upload complete ERR :%s\n", err)
		return err
	}
	fileBody, etag, err := upload.Reassemble(&in)
	if err != nil {
		logrus.Errorf("[MultipartUpload]fileBody, etag ERR :%s\n", err)
		return err
	}
	logrus.Info("[MultipartUpload]fileBody size %d\n", len(fileBody))
	s3cache := env.GetS3Cache()
	directory := s3cache + "/" + bucket + "/" + object
	files, _, _ := ListDir(directory)
	size, _ := DirSize(directory)
	result, err := g.storage.MultipartUpload(content, bucket, object, files, size)
	if err != nil {
		logrus.Errorf("[MultipartUpload]put boject ERR :%s\n", err)
		return err
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

func (g *Yts3) listMultipartUploadParts(bucket, object string, uploadID UploadID, w http.ResponseWriter, r *http.Request) error {
	query := r.URL.Query()
	marker, err := parseClampedInt(query.Get("part-number-marker"), 0, 0, math.MaxInt64)
	if err != nil {
		logrus.Errorf("[MultipartUpload]parseClampedInt Error Msg:%s\n", err)
		return ErrInvalidURI
	}
	maxParts, err := parseClampedInt(query.Get("max-parts"), DefaultMaxUploadParts, 0, MaxUploadPartsLimit)
	if err != nil {
		logrus.Errorf("[MultipartUpload]parseClampedInt Error Msg:%s\n", err)
		return ErrInvalidURI
	}
	out, err := g.uploader.ListParts(bucket, object, uploadID, int(marker), maxParts)
	if err != nil {
		logrus.Errorf("[MultipartUpload]ListParts Error Msg:%s\n", err)
		return err
	}
	return g.xmlEncoder(w).Encode(out)
}

func (g *Yts3) putMultipartUploadPart(bucket, object string, uploadID UploadID, w http.ResponseWriter, r *http.Request) error {
	Authorization := r.Header.Get("Authorization")
	if Authorization == "" {
		logrus.Error("[MultipartUpload]ErrAuthorization\n")
		return ErrAuthorization
	}
	publicKey := GetBetweenStr(Authorization, "YTA", "/")
	content := publicKey[3:]
	if len(content) > 50 {
		publicKeyLength := strings.Index(content, ":")
		contentNew := content[:publicKeyLength]
		content = contentNew
	}
	partNumber, err := strconv.ParseInt(r.URL.Query().Get("partNumber"), 10, 0)
	if err != nil || partNumber <= 0 || partNumber > MaxUploadPartNumber {
		logrus.Errorf("[MultipartUpload]Parse partNumber err:\n", err)
		return ErrInvalidPart
	}
	size, err := strconv.ParseInt(r.Header.Get("Content-Length"), 10, 64)
	if err != nil || size <= 0 {
		return ErrMissingContentLength
	}
	upload, err := g.uploader.Get(bucket, object, uploadID)
	if err != nil {
		logrus.Errorf("[MultipartUpload]uploader.Get Error Msg:%s\n", err)
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
	etag, err := upload.AddPart(bucket, object, int(partNumber), g.timeSource.Now(), rdr, size)
	if err != nil {
		return err
	}
	w.Header().Add("ETag", etag)
	return nil
}

func (g *Yts3) abortMultipartUpload(bucket, object string, uploadID UploadID, w http.ResponseWriter, r *http.Request) error {
	logrus.Infof("[MultipartUpload]abort multipart upload : %s %s %d\n", bucket, object, uploadID)
	if _, err := g.uploader.Complete(bucket, object, uploadID); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}
