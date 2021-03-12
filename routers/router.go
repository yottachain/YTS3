package routers

import (
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"github.com/unrolled/secure"
	"github.com/yottachain/YTS3/controller"
)

//InitRouter 初始化路由
func InitRouter() (router *gin.Engine) {
	router = gin.Default()
	config := cors.DefaultConfig()
	config.AllowAllOrigins = true
	// router.Use(cors.New(config))
	router.Use(TlsHandler())
	v1 := router.Group("/api/v1")
	{
		v1.POST("/insertuser", controller.Register)
		v1.GET("/addPubkey", controller.AddPubkey)
		v1.GET("/createBucket", controller.CreateBucket)
		v1.POST("/upload", controller.UploadFile)
		v1.GET("/getObject", controller.DownloadFile)
		v1.GET("/getBlockForSGX", controller.DownloadFileForSGX)
		v1.GET("/getObjectProgress", controller.GetDownloadProgress)
		v1.GET("/listBucket", controller.GetObjects)
		v1.GET("/listAllBucket", controller.ListBucket)
		v1.GET("/getProgress", controller.GetProgress)
		v1.GET("/getYts3Version", controller.GetProgramVersion)
		v1.GET("/getFileInfo", controller.GetFileBlockDetails)
		v1.GET("/getFileAllInfo", controller.GetFileAllInfo)
		v1.POST("/importAuthFile", controller.ImporterAuth)
		v1.GET("/exporterAuthData", controller.ExporterAuthData)
		v1.GET("/licensedTo", controller.LicensedTo)
	}

	return
}

func TlsHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		secureMiddleware := secure.New(secure.Options{
			SSLRedirect: true,
			SSLHost:     "192.168.1.5:8080",
		})
		err := secureMiddleware.Process(c.Writer, c.Request)

		// If there was an error, do not continue.
		if err != nil {
			logrus.Errorf("Https err:%s\n", err)
			return
		}

		c.Next()
	}
}
