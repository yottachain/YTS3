package controller

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/api"
	"github.com/yottachain/YTCoreService/env"
)

type buckets struct {
	buckets []string
}

type Bucket struct {
	bucketName     string
	version_status string
}

//CreateBucket 创建bucket
func CreateBucket(g *gin.Context) {
	defer env.TracePanic("CreateBucket")

	bucket := g.Query("bucketName")

	publicKey := g.Query("publicKey")

	var header map[string]string

	header = make(map[string]string)

	header["version_status"] = "Enabled"

	meta, err := api.BucketMetaMapToBytes(header)
	if err != nil {
		logrus.Errorf("[ListBucket ]AuthSuper ERR:%s\n", err)
	}
	content := publicKey[3:]

	c := api.GetClient(content)
	bucketAccessor := c.NewBucketAccessor()
	err2 := bucketAccessor.CreateBucket(bucket, meta)
	if err2 != nil {
		logrus.Errorf("[ListBucket ]AuthSuper ERR:%s\n", err2)
		g.JSON(http.StatusMethodNotAllowed, gin.H{"error": "create bucket error"})
	} else {
		buck := Bucket{}
		buck.bucketName = bucket
		buck.version_status = "Enabled"
		g.JSON(http.StatusOK, buck)
	}

}

//ListBucket list all bucket
func ListBucket(g *gin.Context) {
	publicKey := g.Query("publicKey")
	content := publicKey[3:]

	fmt.Println("publicKey::::", content)
	c := api.GetClient(content)
	bucketAccessor := c.NewBucketAccessor()
	fmt.Println("UserName:", c.Username)
	names, err := bucketAccessor.ListBucket()

	if err != nil {
		logrus.Errorf("[ListBucket ]AuthSuper ERR:%s\n", err)
	}

	g.JSON(http.StatusOK, names)
}
