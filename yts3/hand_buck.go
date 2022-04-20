package yts3

import (
	"net/http"
	"strings"

	"github.com/sirupsen/logrus"
)

func (g *Yts3) createBucket(bucket string, w http.ResponseWriter, r *http.Request) error {
	logrus.Infof("[CreateBucket]%s\n", bucket)
	Authorization := r.Header.Get("Authorization")
	if Authorization == "" {
		logrus.Error("[CreateBucket]ErrAuthorization\n")
		return ErrAuthorization
	}
	publicKey := GetBetweenStr(Authorization, "YTA", "/")
	content := publicKey[3:]
	if len(content) > 50 {
		publicKeyLength := strings.Index(content, ":")
		contentNew := content[:publicKeyLength]
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

func (g *Yts3) listBuckets(w http.ResponseWriter, r *http.Request) error {
	Authorization := r.Header.Get("Authorization")
	if Authorization == "" {
		logrus.Error("[listBuckets]ErrAuthorization\n")
		return ErrAuthorization
	}
	publicKey := GetBetweenStr(Authorization, "YTA", "/")
	content := publicKey[3:]
	if len(content) > 50 {
		publicKeyLength := strings.Index(content, ":")
		contentNew := content[:publicKeyLength]
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
