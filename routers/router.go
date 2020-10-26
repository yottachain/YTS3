package routers

import (
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/yottachain/YTS3/controller"
)

//InitRouter 初始化路由
func InitRouter() (router *gin.Engine) {
	router = gin.Default()
	config := cors.DefaultConfig()
	config.AllowAllOrigins = true
	router.Use(cors.New(config))

	v1 := router.Group("/api/v1")
	{
		v1.POST("/insertuser", controller.Register)
		v1.GET("/createBucket", controller.CreateBucket)
		v1.POST("/upload", controller.UploadFile)
		v1.GET("/getObject", controller.DownloadFile)
		v1.GET("/getObjectProgress", controller.GetDownloadProgress)
		v1.GET("/listBucket", controller.GetObjects)
		v1.GET("/listAllBucket", controller.ListBucket)
		v1.GET("/getProgress", controller.GetProgress)
		// v1.GET("/getFileInfo", controller.GetFileBlockDetails)
		// v1.GET("/getFileAllInfo", controller.GetFileAllInfo)
	}

	return
}
