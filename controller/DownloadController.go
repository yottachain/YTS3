package controller

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/common/log"
	"github.com/yottachain/YTCoreService/api"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

//DownloadFile 指定下载路径下载文件
func DownloadFile(g *gin.Context) {

	bucketName := g.Query("bucketName")

	objectKey := g.Query("key")

	publicKey := g.Query("publicKey")

	filePath := g.Query("path")
	content := publicKey[3:]
	c := api.GetClient(content)

	// download, err := c.NewDownloadBytes(bucketName, objectKey, primitive.NilObjectID)
	download, err := c.NewDownloadFile(bucketName, objectKey, primitive.NilObjectID)

	if err != nil {
		log.Info("Download is error, fileName:" + objectKey)
	}

	err2 := download.SaveToFile(filePath)

	if err2 != nil {
		log.Info("Download is error, fileName:" + objectKey)
	}
	g.String(http.StatusOK, "Download is Success")
}
