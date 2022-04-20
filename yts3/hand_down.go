package yts3

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"

	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/env"
)

var GetObjectNum *int32 = new(int32)

func (g *Yts3) getObject(bucket, object string, versionID VersionID, w http.ResponseWriter, r *http.Request) error {
	MaxGetObjNum := env.GetConfig().GetRangeInt("MaxGetObjNum", 20, 100, 50)
	count := atomic.AddInt32(GetObjectNum, 1)
	defer atomic.AddInt32(GetObjectNum, -1)
	logrus.Infof("[S3Download]getObject request number: %d\n", count)
	if count > int32(MaxGetObjNum) {
		return errors.New("getObject request too frequently.\n")
	}
	logrus.Infof("[S3Download]GET OBJECT:/%s/%s\n", bucket, object)
	Authorization := r.Header.Get("Authorization")
	if Authorization == "" {
		logrus.Error("[S3Download]getObject ErrAuthorization\n")
		return ErrAuthorization
	}
	publicKey := GetBetweenStr(Authorization, "YTA", "/")
	content := publicKey[3:]
	if len(content) > 50 {
		publicKeyLength := strings.Index(content, ":")
		contentNew := content[:publicKeyLength]
		content = contentNew
	}
	q := r.URL.Query()
	prefix := prefixFromQuery(q)
	page, err := listBucketPageFromQuery(q)
	if err != nil {
		return err
	}
	rnge, err := parseRangeHeader(r.Header.Get("Range"))
	if err != nil {
		return err
	}
	var obj *Object
	if versionID == "" {
		obj, err = g.storage.GetObjectV2(content, bucket, object, rnge, &prefix, page)
		if err != nil {
			return err
		}
	}
	if obj == nil {
		logrus.Errorf("[S3Download]unexpected nil object for key:%s%s\n", bucket, object)
		return ErrInternal
	}
	defer obj.Contents.Close()

	if err := g.writeGetOrHeadObjectResponse(obj, w, r); err != nil {
		return err
	}
	logrus.Infof("[S3Download]content length:%d\n", obj.Size)
	obj.Range.writeHeader(obj.Size, w)
	if _, err := io.Copy(w, obj.Contents); err != nil {
		logrus.Errorf("[S3Download]Write err:%s\n", err)
		return err
	}
	logrus.Infof("[S3Download]/%s/%s download successful.\n", bucket, object)
	return nil
}

func (g *Yts3) headObject(bucket, object string, versionID VersionID, w http.ResponseWriter, r *http.Request) error {
	MaxGetObjNum := env.GetConfig().GetRangeInt("MaxGetObjNum", 20, 100, 50)
	count := atomic.AddInt32(GetObjectNum, 1)
	defer atomic.AddInt32(GetObjectNum, -1)
	logrus.Infof("[S3Download]headObject request number: %d\n", count)
	if count > int32(MaxGetObjNum) {
		return errors.New("headObject request too frequently.\n")
	}
	logrus.Infof("[S3Download]HEAD OBJECT,Bucket:%s,Object:%s\n", bucket, object)
	Authorization := r.Header.Get("Authorization")
	if Authorization == "" {
		logrus.Error("[S3Download]headObject ErrAuthorization\n")
		return ErrAuthorization
	}
	publicKey := GetBetweenStr(Authorization, "YTA", "/")
	content := publicKey[3:]
	if len(content) > 50 {
		publicKeyLength := strings.Index(content, ":")
		contentNew := content[:publicKeyLength]
		content = contentNew
	}
	q := r.URL.Query()
	prefix := prefixFromQuery(q)
	page, err := listBucketPageFromQuery(q)
	if err != nil {
		return err
	}
	rnge, err := parseRangeHeader(r.Header.Get("Range"))
	if err != nil {
		return err
	}
	var obj *Object
	obj, err = g.storage.GetObjectV2(content, bucket, object, rnge, &prefix, page)
	if err != nil {
		return err
	}
	if obj == nil {
		logrus.Errorf("[S3Download]unexpected nil object for key ： %s%s\n", bucket, object)
		return ErrInternal
	}
	defer obj.Contents.Close()
	if err := g.writeGetOrHeadObjectResponse(obj, w, r); err != nil {
		return err
	}
	w.Header().Set("Content-Length", fmt.Sprintf("%d", obj.Size))
	logrus.Infof("[S3Download]/%s/%s download successful.\n", bucket, object)
	return nil
}
