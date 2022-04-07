package controller

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/patrickmn/go-cache"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/api"
)

var upload_progress_CACHE = cache.New(time.Duration(600000)*time.Second, time.Duration(600000)*time.Second)

//UploadFile 根据路径上传文件
func UploadFile(g *gin.Context) {
	bucketName := g.PostForm("bucketName")

	publicKey := g.PostForm("publicKey")
	files := g.PostForm("path")

	var filename string
	filename = filepath.Base(files)
	content := publicKey[3:]
	c := api.GetClient(content)
	if c == nil {
		logrus.Error("pubilic is null.\n")
		return
	}

	//根据路径上传文件
	upload := c.NewUploadObject()

	putUploadObject(bucketName, filename, publicKey, upload)

	err := upload.UploadFile(files)
	if err != nil {
		logrus.Errorf("[UploadFile ]AuthSuper ERR:%s\n", err)
	}
	var header map[string]string
	header = make(map[string]string)
	timeUnix := time.Now().Unix()

	fileSize := getFileSize(files)
	hash := upload.GetMD5()
	header["ETag"] = hex.EncodeToString(hash)
	header["x-amz-date"] = string(timeUnix)
	header["contentLength"] = strconv.FormatInt(fileSize, 10)
	meta, err1 := api.FileMetaMapTobytes(header)
	if err1 != nil {
		logrus.Errorf("[FileMetaMapTobytes ]AuthSuper ERR:%s\n", err1)
	}
	//写元数据
	c.NewObjectAccessor().CreateObject(bucketName, filename, upload.VNU, meta)

	//如果成功返回文件hash
	// return string(hash)
	g.String(http.StatusOK, hex.EncodeToString(hash))
}

//GetProgress 查询上传进度
// func GetProgress(g *gin.Context) {
// 	publicKey := g.Query("publicKey")
// 	bucketName := g.Query("bucketName")
// 	fileName := g.Query("fileName")

// 	ii := getUploadProgress(bucketName, fileName, publicKey)

// 	g.String(http.StatusOK, strconv.FormatInt(int64(ii), 10))
// }
func GetProgress(g *gin.Context) {
	publicKey := g.Query("publicKey")
	bucketName := g.Query("bucketName")
	fileName := g.Query("fileName")
	content := publicKey[3:]
	c := api.GetClient(content)
	if c == nil {
		logrus.Error("pubilic is null.\n")
		return
	}

	ii := c.GetProgress(bucketName, fileName)
	// ii := getUploadProgress(bucketName, fileName, publicKey)

	g.String(http.StatusOK, strconv.FormatInt(int64(ii), 10))
}

//putUploadObject 将上传实例加入到缓存中 用于进度查询
func putUploadObject(bucketName, fileName, publicKey string, upload *api.UploadObject) {

	key := bucketName + fileName + publicKey

	data := []byte(key)
	has := md5.Sum(data)
	md5str := fmt.Sprintf("%x", has)
	upload_progress_CACHE.SetDefault(md5str, upload)
}

//getUploadProgress 查询进度
func getUploadProgress(bucketName, fileName, publicKey string) int32 {
	var num int32
	key := bucketName + fileName + publicKey

	data := []byte(key)
	has := md5.Sum(data)
	md5str := fmt.Sprintf("%x", has)
	v, found := upload_progress_CACHE.Get(md5str)

	if found {
		ii := v.(*api.UploadObject).GetProgress()
		num = ii
	} else {
		num = 0
	}
	return num
}

func getFileSize(filename string) int64 {
	var result int64
	filepath.Walk(filename, func(path string, f os.FileInfo, err error) error {
		result = f.Size()
		return nil
	})
	return result
}

func SaveFileToLocal(g *gin.Context) {
	file, err := g.FormFile("uploadfile")
	if err != nil {
		return
	}
	fileName := file.Filename
	fmt.Println("文件名：", fileName)
	if err := g.SaveUploadedFile(file, fileName); err != nil {
		g.String(http.StatusBadRequest, "保存失败 Error:%s", err.Error())
		return
	}
	up, err1 := api.NewUploadEncObject(fileName)
	if err1 != nil {
		return
	}
	err1 = up.Upload()
	if err1 != nil {
		return
	} else {
		del := os.Remove(fileName)
		if del != nil {
			fmt.Println(del)
		}
	}

}
