package yts3

import (
	"encoding/base64"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"

	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/env"
)

var ListBucketNum *int32 = new(int32)

func (g *Yts3) listBucket(bucketName string, w http.ResponseWriter, r *http.Request) error {
	MaxListNum := env.GetConfig().GetRangeInt("MaxListNum", 1, 10, 2)
	count := atomic.AddInt32(ListBucketNum, 1)
	defer atomic.AddInt32(ListBucketNum, -1)
	if count > int32(MaxListNum) {
		return errors.New("listBucket request too frequently")
	}
	Authorization := r.Header.Get("Authorization")
	if Authorization == "" {
		logrus.Error("[ListBucket]ErrAuthorization\n")
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
	if page.MaxKeys > 10000 {
		page.MaxKeys = 10000
	}
	isVersion2 := q.Get("list-type") == "2"
	logrus.Infof("[ListBucket]Request bucketname:%s,prefix:%s,Marker:%s,HasMarker:%v,MaxKeys:%d\n", bucketName, prefix, page.Marker, page.HasMarker, page.MaxKeys)
	objects, err := g.storage.ListBucket(content, bucketName, &prefix, page)
	if err != nil {
		return err
	}
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
		if objects.NextMarker != "" {
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
