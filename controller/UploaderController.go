package controller

import (
	"fmt"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/common/log"
	"github.com/yottachain/YTCoreService/api"
)

//UploadFile 根据路径上传文件
func UploadFile(g *gin.Context) {

	bucketName := g.PostForm("bucketName")

	publicKey := g.PostForm("publicKey")
	filepath := g.PostForm("path")

	var filename string
	filename = path.Base(filepath)
	fmt.Println("filename=", filename)
	content := publicKey[3:]
	c := api.GetClient(content)

	//根据路径上传文件
	upload := c.NewUploadObject()
	hash, err := upload.UploadFile(filepath)
	if err != nil {

	}
	var header map[string]string
	header = make(map[string]string)
	timeUnix := time.Now().Unix()
	fileSize := getFileSize(filepath)
	header["ETag"] = string(hash)
	header["x-amz-date"] = string(timeUnix)
	header["contentLength"] = string(fileSize)
	meta, err1 := api.FileMetaMapTobytes(header)
	if err1 != nil {
		log.Error(err1.Error())
	}
	//写元数据
	c.NewObjectAccessor().CreateObject(bucketName, filename, upload.VNU, meta)

	//如果成功返回文件hash
	// return string(hash)
	g.String(http.StatusOK, string(hash))
}

//GetProgress 查询上传进度
// func GetProgress(g *gin.Context) {
// 	publicKey := g.PostForm("publicKey")
// 	c := api.GetClient(publicKey)
// 	upload := c.NewUploadObject()

// 	ii := upload.GetProgress()
// }

func getFileSize(filename string) int64 {
	var result int64
	filepath.Walk(filename, func(path string, f os.FileInfo, err error) error {
		result = f.Size()
		return nil
	})
	return result
}
