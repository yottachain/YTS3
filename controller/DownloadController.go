package controller

import (
	"crypto/md5"
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/env"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/patrickmn/go-cache"
	"github.com/yottachain/YTCoreService/api"
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
		blockName := "block" + strconv.FormatInt(int64(i), 10)
		blockinfo.BlockNum = blockName
		blockinfo.BlockEncPath = fileDirectory + "/" + fileName + "/" + blockName + "/block.enc"
		blockinfo.BlockSrcPath = fileDirectory + "/" + fileName + "/" + blockName + "/block.src"
		blockinfo.BlockZipPath = fileDirectory + "/" + fileName + "/" + blockName + "/block.zip"
		blockDirectory := fileDirectory + "/" + fileName + "/" + blockName + "/"
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

//func GetBlockInfo(g *gin.Context) {
//	block := Block{}
//	fileName := g.Query("fileName")
//
//	fileDirectory := g.Query("directory")
//
//	blockNum := g.Query("blockNum")
//	block.BlockNum = blockNum
//
//	blockDirectory := fileDirectory + "/" + fileName + "/" + blockNum
//	// list, err := getDirList(blockDirectory)
//	// if err != nil {
//	// 	fmt.Println(err)
//	// 	return
//	// }
//
//	blockSrc := blockDirectory + "/block.src"
//	blockZip := blockDirectory + "/block.zip"
//	blockEnc := blockDirectory + "/block.enc"
//	bsSrc, err := ioutil.ReadFile(blockSrc)
//	bsEnc, err := ioutil.ReadFile(blockEnc)
//	bsZip, err := ioutil.ReadFile(blockZip)
//	//ioutil.ReadAll()
//
//	if err != nil {
//
//	}
//	block.BlockSrc = hex.EncodeToString(bsSrc)
//	// fmt.Println("block.BlockSrc:::::", block.BlockSrc)
//
//	block.BlockEnc = hex.EncodeToString(bsEnc)
//	block.BlockZip = hex.EncodeToString(bsZip)
//	g.JSON(http.StatusOK, block)
//}

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

func DownloadFileForSGX(g *gin.Context) {

	userName := g.Query("userName")
	bucketName := g.Query("bucketName")
	fileName := g.Query("fileName")
	blockNum := g.Query("blockNum")

	num, errN := strconv.ParseInt(blockNum, 10, 32)
	if errN != nil {
	}

	//content := publicKey[3:]
	c := api.GetClientByName(userName)
	//c := api.GetClient(content)
	// c.DownloadToSGX()
	// sgx,err :=c.DownloadToSGX(bucketName, fileName)
	sgx, err := c.DownloadToSGX(bucketName, fileName)

	if err != nil {
		logrus.Errorf("Download block Faild ,Err:%s\n", err)
		g.JSON(http.StatusSeeOther, gin.H{"msg": err.Msg, "code": err.Code})
	} else {
		data, err := sgx.LoadBlock(int32(num))
		if err != nil {
			logrus.Errorf("Download block Faild ,Err:%s\n", err)
			g.JSON(http.StatusSeeOther, gin.H{"msg": err.Msg, "code": err.Code})
		}
		// g.ProtoBuf(http.StatusOK, data)
		if data == nil {
			g.JSON(http.StatusOK, nil)
		} else {
			g.JSON(http.StatusOK, data)
		}

	}

}

//DownloadFile 下载
func DownloadFile(g *gin.Context) {
	defer env.TracePanic("DownloadFile")
	bucketName := g.Query("bucketName")

	fileName := g.Query("fileName")

	publicKey := g.Query("publicKey")

	savePath := g.Query("path")

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

	errn := download.SaveToPath(savePath + "/" + fileName)
	// errn := download.SaveToFile(savePath + "/" + fileName)
	if errn != nil {
		logrus.Errorf("[DownloadFile ]AuthSuper ERR:%s\n", errn)
	} else {
		logrus.Infof("[ " + fileName + " ]" + " is Download Success")
	}

}

//putUploadObject 将上传实例加入到缓存中 用于进度查询
func putDownloadObject(bucketName, fileName, publicKey string, upload *api.DownloadObject) {

	key := bucketName + fileName + publicKey + "download"

	data := []byte(key)
	has := md5.Sum(data)
	md5str := fmt.Sprintf("%x", has)
	logrus.Infof("md5str set : %s", md5str)
	download_progress_CACHE.SetDefault(md5str, upload)
}

//GetDownloadProgress 查询上传进度
func GetDownloadProgress(g *gin.Context) {
	// defer env.TracePanic()
	publicKey := g.Query("publicKey")
	bucketName := g.Query("bucketName")
	fileName := g.Query("fileName")

	ii := getDownloadProgress(bucketName, fileName, publicKey)

	g.String(http.StatusOK, strconv.FormatInt(int64(ii), 10))
}

//getDownloadProgress 查询进度
func getDownloadProgress(bucketName, fileName, publicKey string) int32 {
	var num int32
	key := bucketName + fileName + publicKey + "download"

	data := []byte(key)
	has := md5.Sum(data)
	md5str := fmt.Sprintf("%x", has)
	logrus.Infof("md5str get : %s", md5str)
	v, found := download_progress_CACHE.Get(md5str)

	logrus.Infof("key is value : \n", found)
	if found {
		ii := v.(*api.DownloadObject).GetProgress()
		num = ii
	} else {
		num = 0
	}
	return num
}
