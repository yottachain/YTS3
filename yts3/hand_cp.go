package yts3

import (
	"encoding/hex"
	"net/http"
	"strings"

	"github.com/sirupsen/logrus"
)

func (g *Yts3) copyObject(bucket, object string, meta map[string]string, w http.ResponseWriter, r *http.Request) (err error) {
	source := meta["X-Amz-Copy-Source"]
	logrus.Infof("[CopyObject]/%s/%s,source:%s\n", bucket, object, source)
	Authorization := r.Header.Get("Authorization")
	if Authorization == "" {
		logrus.Error("[CopyObject]ErrAuthorization\n")
		return ErrAuthorization
	}
	publicKey := GetBetweenStr(Authorization, "YTA", "/")
	content := publicKey[3:]
	if len(content) > 50 {
		publicKeyLength := strings.Index(content, ":")
		contentNew := content[:publicKeyLength]
		content = contentNew
	}
	if len(object) > KeySizeLimit {
		return ResourceError(ErrKeyTooLong, object)
	}
	// XXX No support for versionId subresource
	parts := strings.SplitN(strings.TrimPrefix(source, "/"), "/", 2)
	srcBucket := parts[0]
	srcKey := strings.SplitN(parts[1], "?", 2)[0]

	srcObj, err := g.storage.GetObject(content, srcBucket, srcKey, nil)
	if err != nil {
		return err
	}
	if srcObj == nil {
		logrus.Errorf("[CopyObject]unexpected nil object for key /%s/%s", bucket, object)
		return ErrInternal
	}
	defer srcObj.Contents.Close()
	for k, v := range srcObj.Metadata {
		if _, found := meta[k]; !found && k != "X-Amz-Acl" {
			meta[k] = v
		}
	}
	result, err := g.storage.PutObject(content, bucket, object, meta, srcObj.Contents, srcObj.Size, 0)
	if err != nil {
		return err
	}
	if srcObj.VersionID != "" {
		w.Header().Set("x-amz-copy-source-version-id", string(srcObj.VersionID))
	}
	if result.VersionID != "" {
		logrus.Errorf("[CopyObject]CREATED VERSION:/%s/%s/%s", bucket, object, result.VersionID)
		w.Header().Set("x-amz-version-id", string(result.VersionID))
	}
	return g.xmlEncoder(w).Encode(CopyObjectResult{
		ETag:         `"` + hex.EncodeToString(srcObj.Hash) + `"`,
		LastModified: NewContentTime(g.timeSource.Now()),
	})
}
