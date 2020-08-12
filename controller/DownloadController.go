package controller

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/api"
	"github.com/yottachain/YTCoreService/env"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

//DownloadFile 指定下载路径下载文件
func DownloadFile(g *gin.Context) {
	defer env.TracePanic()
	bucketName := g.Query("bucketName")

	objectKey := g.Query("key")

	publicKey := g.Query("publicKey")

	filePath := g.Query("path")
	content := publicKey[3:]
	c := api.GetClient(content)

	// download, err := c.NewDownloadBytes(bucketName, objectKey, primitive.NilObjectID)
	download, err := c.NewDownloadFile(bucketName, objectKey, primitive.NilObjectID)

	if err != nil {
		logrus.Errorf("[DownloadFile ]AuthSuper ERR:%s\n", err)
	}

	err2 := download.SaveToFile(filePath)

	if err2 != nil {
		logrus.Errorf("[DownloadFile ]AuthSuper ERR:%s\n", err)
	}
	g.String(http.StatusOK, "Download is Success")
}
