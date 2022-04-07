package controller

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/api"
	"github.com/yottachain/YTCoreService/env"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type ObjectItem struct {
	FileName   string
	FileLength string
	TimeStamp  string
	nVerid     primitive.ObjectID
}

//GetObjects 获取文件列表
func GetObjects(g *gin.Context) {
	defer env.TracePanic("GetObjects")
	var objectItems []ObjectItem
	item := ObjectItem{}
	bucketName := g.Query("bucketName")
	publicKey := g.Query("publicKey")
	content := publicKey[3:]
	// fileName := g.Query("fileName")
	prefix := g.Query("prefix")
	// maxKeys := g.Query("maxKeys")
	// limitCount, err := strconv.ParseInt(maxKeys, 10, 32)
	var flag bool = false
	// CheckErr(err)
	var fileName string
	limitCount := 1000

	for flag == false {
		// startObjectID := primitive.NilObjectID
		var startObjectID primitive.ObjectID
		ls := listObjects(content, bucketName, fileName, prefix, false, startObjectID, uint32(limitCount))
		if len(ls) > 0 {
			for _, object := range ls {
				item.FileName = object.FileName
				item.TimeStamp = object.TimeStamp
				item.FileLength = object.FileLength
				objectItems = append(objectItems, item)
			}
		}
		fmt.Println(len(ls))
		if len(ls) < int(limitCount) {
			flag = true
		} else {
			startObjectID = ls[len(ls)-1].nVerid
			fileName = ls[len(ls)-1].FileName
			fmt.Println("fileName:", fileName)
		}

	}

	g.JSON(http.StatusOK, objectItems)
}

func listObjects(publicKey, buck, fileName, prefix string, wversion bool, nVerid primitive.ObjectID, limit uint32) []ObjectItem {
	var objectItems []ObjectItem

	item := ObjectItem{}
	c := api.GetClient(publicKey)
	if c == nil {
		logrus.Error("pubilic is null.\n")
	}

	objectAccessor := c.NewObjectAccessor()

	ls, err := objectAccessor.ListObject(buck, fileName, prefix, wversion, nVerid, limit)

	if err != nil {
		logrus.Infof("Pull objects is error:%s ", err)
	}
	// objItem := *[]ObjectItem
	// objItem = ls
	if len(ls) > 0 {
		var header map[string]string

		for i := 0; i < len(ls); i++ {
			item.FileName = ls[i].FileName
			meta := ls[i].Meta
			header, _ = api.BytesToFileMetaMap(meta, ls[i].VersionId)

			item.FileLength = header["contentLength"]
			item.TimeStamp = header["x-amz-date"]
			item.nVerid = ls[i].VersionId
			objectItems = append(objectItems, item)

		}
	}

	return objectItems
}

//GetFileBlockDetails 查询文件分块信息
func GetFileBlockDetails(g *gin.Context) {
	defer env.TracePanic("GetFileBlockDetails")
	fileName := g.Query("fileName")
	bucketName := g.Query("bucketName")
	publicKey := g.Query("publicKey")
	content := publicKey[3:]
	c := api.GetClient(content)
	if c == nil {
		logrus.Error("public is null.\n")
		return
	}
	info, err := c.NewObjectMeta(bucketName, fileName, primitive.NilObjectID)

	if err != nil {
		logrus.Errorf("[GetFileBlockDetails ]AuthSuper ERR:%s\n", err)
		// panic(err)
	}
	g.JSON(http.StatusOK, info)
}

//CheckErr 检查错误原因
// func CheckErr(err error) {
// 	if err != nil {
// 		panic(err)
// 	}
// }
