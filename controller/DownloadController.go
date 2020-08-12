package controller

import (
	"fmt"
	"io"

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

	// filePath := g.Query("path")
	content := publicKey[3:]
	c := api.GetClient(content)
	g.Writer.Header().Add("Content-Disposition", fmt.Sprintf("attachment; filename=%s", objectKey))
	g.Writer.Header().Add("Content-Type", "application/octet-stream")
	download, erra := c.NewDownloadFile(bucketName, objectKey, primitive.NilObjectID)

	if erra != nil {
		logrus.Errorf("[DownloadFile ]AuthSuper ERR:%s\n", erra)
	}
	reader := download.Load()
	readbuf := make([]byte, 8192)
	for {
		num, err := reader.Read(readbuf)
		if err != nil && err != io.EOF {
			// return err
		}
		if num > 0 {
			bs := readbuf[0:num]
			g.Writer.Write(bs)
		}
		if err != nil && err == io.EOF {
			break
		}
	}

	fmt.Println("Download File Success")

}
