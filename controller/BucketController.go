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

//ListBucket list all bucket
func ListBucket(g *gin.Context) {
	defer env.TracePanic()
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

	// buckets.buckets := names
	g.JSON(http.StatusOK, names)
}
