package controller

import (
	"net/http"

	"github.com/ethereum/go-ethereum/log"
	"github.com/gin-gonic/gin"
	"github.com/yottachain/YTCoreService/api"
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
		if len(ls) < int(limitCount) {
			flag = true
		} else {
			startObjectID = ls[len(ls)-1].nVerid
		}

	}

	g.JSON(http.StatusOK, objectItems)
}

func listObjects(publicKey, buck, fileName, prefix string, wversion bool, nVerid primitive.ObjectID, limit uint32) []ObjectItem {
	var objectItems []ObjectItem

	item := ObjectItem{}
	c := api.GetClient(publicKey)

	objectAccessor := c.NewObjectAccessor()

	ls, err := objectAccessor.ListObject(buck, fileName, prefix, wversion, nVerid, limit)

	if err != nil {
		log.Info("Pull objects is error ", err)
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

//CheckErr 检查错误原因
func CheckErr(err error) {
	if err != nil {
		panic(err)
	}
}
