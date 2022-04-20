package yts3

import (
	"encoding/xml"
	"net/http"
	"strings"

	"github.com/sirupsen/logrus"
)

func (g *Yts3) deleteObject(bucket, object string, w http.ResponseWriter, r *http.Request) error {
	logrus.Infof("[S3Delete]DELETE:%s%s\n", bucket, object)
	Authorization := r.Header.Get("Authorization")
	if Authorization == "" {
		logrus.Error("[S3Delete]ErrAuthorization\n")
		return ErrAuthorization
	}
	publicKey := GetBetweenStr(Authorization, "YTA", "/")
	content := publicKey[3:]
	if len(content) > 50 {
		publicKeyLength := strings.Index(content, ":")
		contentNew := content[:publicKeyLength]
		content = contentNew
	}
	result, err := g.storage.DeleteObject(content, bucket, object)
	if err != nil {
		logrus.Errorf("[S3Delete]Error:%s\n", err)
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
	logrus.Infof("[S3Delete]DELETE BUCKET:%s\n", bucket)
	Authorization := r.Header.Get("Authorization")
	if Authorization == "" {
		logrus.Error("[deleteBucket]ErrAuthorization\n")
		return ErrAuthorization
	}
	publicKey := GetBetweenStr(Authorization, "YTA", "/")
	content := publicKey[3:]
	if len(content) > 50 {
		publicKeyLength := strings.Index(content, ":")
		contentNew := content[:publicKeyLength]
		content = contentNew
	}
	if err := g.storage.DeleteBucket(content, bucket); err != nil {
		logrus.Errorf("[S3Delete]Error Msg:%s\n", err)
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (g *Yts3) deleteMulti(bucket string, w http.ResponseWriter, r *http.Request) error {
	logrus.Infof("[S3Delete]delete multi : %s\n", bucket)
	Authorization := r.Header.Get("Authorization")
	if Authorization == "" {
		logrus.Error("[S3Delete]delteMulti ErrAuthorization\n")
		return ErrAuthorization
	}
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
