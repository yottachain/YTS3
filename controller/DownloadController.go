package controller

import (
	"crypto/md5"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/patrickmn/go-cache"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/api"
	"github.com/yottachain/YTCoreService/env"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

var download_progress_CACHE = cache.New(time.Duration(600000)*time.Second, time.Duration(600000)*time.Second)

//FileBlockAndShardInfo 文件信息
type FileBlockAndShardInfo struct {
	FileName   string //文件名
	FileLength int64
	BlockCount int //文件总块数量
	Blocks     []*Block
}

//Block 块信息
type Block struct {
	BlockNum     string //分块编号
	BlockEncPath string //16进制加密
	BlockSrcPath string //16进制非加密
	BlockZipPath string
	Shards       []*Shard
}

//Shard 分片信息
type Shard struct {
	ShardID      int
	ShardSrcPath string //分片16进制数据
}

//GetFileAllInfo 获取文件详细信息
func GetFileAllInfo(g *gin.Context) {

	fileName := g.Query("fileName")

	fileDirectory := g.Query("directory")
	fileBlockAndShardInfo := FileBlockAndShardInfo{}
	var blocks []*Block
	// var shards []Shard

	fileBlockAndShardInfo.FileName = fileName
	list, err := getDirList(fileDirectory + "/" + fileName)
	if err != nil {
		fmt.Println(err)
		return
	}
	fileBlockAndShardInfo.BlockCount = len(list) - 1
	files := fileDirectory + "/" + fileName + "/source.dat"
	fileSize := getFileSize(files)
	fileBlockAndShardInfo.FileLength = fileSize
	// g.JSON(http.StatusOK, fileBlockAndShardInfo)
	// fmt.Println("len:::::", len(list)-1)

	for i := 0; i < len(list)-1; i++ {
		blockinfo := Block{}
		var shardslist []*Shard
		// blockinfo.BlockID = i

		blockName := "block" + strconv.FormatInt(int64(i), 10)
		blockinfo.BlockNum = blockName
		blockinfo.BlockEncPath = fileDirectory + "/" + fileName + "/" + blockName + "/block.enc"
		blockinfo.BlockSrcPath = fileDirectory + "/" + fileName + "/" + blockName + "/block.src"
		blockinfo.BlockZipPath = fileDirectory + "/" + fileName + "/" + blockName + "/block.zip"
		blockDirectory := fileDirectory + "/" + fileName + "/" + blockName + "/"
		// blockInList, err := getDirList(blockDirectory)
		blockInList, err := ioutil.ReadDir(blockDirectory)
		if err != nil {
			fmt.Println(err)
			return
		}
		for ii := 0; ii < len(blockInList)-3; ii++ {
			shard := Shard{}
			shard.ShardID = ii
			shard.ShardSrcPath = blockDirectory + "/" + strconv.FormatInt(int64(ii), 10)
			shardslist = append(shardslist, &shard)
		}
		blockinfo.Shards = shardslist
		blocks = append(blocks, &blockinfo)
	}
	fileBlockAndShardInfo.Blocks = blocks

	g.JSON(http.StatusOK, fileBlockAndShardInfo)

}

// func GetBlockInfo(g *gin.Context) {
// 	block := Block{}
// 	fileName := g.Query("fileName")

// 	fileDirectory := g.Query("directory")

// 	blockNum := g.Query("blockNum")
// 	block.BlockNum = blockNum

// 	blockDirectory := fileDirectory + "/" + fileName + "/" + blockNum
// 	// list, err := getDirList(blockDirectory)
// 	// if err != nil {
// 	// 	fmt.Println(err)
// 	// 	return
// 	// }

// 	blockSrc := blockDirectory + "/block.src"
// 	blockZip := blockDirectory + "/block.zip"
// 	blockEnc := blockDirectory + "/block.enc"
// 	bsSrc, err := ioutil.ReadFile(blockSrc)
// 	bsEnc, err := ioutil.ReadFile(blockEnc)
// 	bsZip, err := ioutil.ReadFile(blockZip)
// 	//ioutil.ReadAll()

// 	if err != nil {

// 	}
// 	block.BlockSrc = hex.EncodeToString(bsSrc)
// 	// fmt.Println("block.BlockSrc:::::", block.BlockSrc)

// 	block.BlockEnc = hex.EncodeToString(bsEnc)
// 	block.BlockZip = hex.EncodeToString(bsZip)
// 	g.JSON(http.StatusOK, block)
// }

func getDirList(dirpath string) ([]string, error) {
	var dir_list []string
	dir_err := filepath.Walk(dirpath,
		func(path string, f os.FileInfo, err error) error {
			if f == nil {
				return err
			}
			if f.IsDir() {
				dir_list = append(dir_list, path)
				return nil
			}

			return nil
		})
	return dir_list, dir_err
}

//DownloadFileOld 指定下载路径下载文件
func DownloadFileOld(g *gin.Context) {
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

//DownloadFile 下载
func DownloadFile(g *gin.Context) {
	defer env.TracePanic()
	bucketName := g.Query("bucketName")

	fileName := g.Query("fileName")

	publicKey := g.Query("publicKey")

	savePath := g.Query("path")

	// filePath := g.Query("path")
	content := publicKey[3:]
	c := api.GetClient(content)

	download, err := c.NewDownloadFile(bucketName, fileName, primitive.NilObjectID)
	if err != nil {
		logrus.Errorf("[DownloadFile ]AuthSuper ERR:%s\n", err)
	}

	putDownloadObject(bucketName, fileName, publicKey, download)

	if err != nil {
		logrus.Errorf("[DownloadFile ]AuthSuper ERR:%s\n", err)
	}

	download.SaveToPath(savePath + "/" + fileName)
}

//putUploadObject 将上传实例加入到缓存中 用于进度查询
func putDownloadObject(bucketName, fileName, publicKey string, upload *api.DownloadObject) {

	key := bucketName + fileName + publicKey + "download"

	data := []byte(key)
	has := md5.Sum(data)
	md5str := fmt.Sprintf("%x", has)
	download_progress_CACHE.SetDefault(md5str, upload)
}

//GetDownloadProgress 查询上传进度
func GetDownloadProgress(g *gin.Context) {
	defer env.TracePanic()
	publicKey := g.Query("publicKey")
	bucketName := g.Query("bucketName")
	fileName := g.Query("fileName")

	ii := getDownloadProgress(bucketName, fileName, publicKey)

	g.String(http.StatusOK, fmt.Sprintf("%x", ii))
}

//getDownloadProgress 查询进度
func getDownloadProgress(bucketName, fileName, publicKey string) int32 {
	var num int32
	key := bucketName + fileName + publicKey + "download"

	data := []byte(key)
	has := md5.Sum(data)
	md5str := fmt.Sprintf("%x", has)
	v, found := download_progress_CACHE.Get(md5str)

	if found {
		ii := v.(*api.DownloadObject).GetProgress()
		num = ii
	} else {
		num = 0
	}
	return num
}
