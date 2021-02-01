package controller

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/api"
	"github.com/yottachain/YTCoreService/env"
)

//Exportclient 1.注册导出授权的用户实例
func exportclient(userName, privateKey string) (*api.Client, error) {

	exportclient, err := api.NewClientV2(&env.UserInfo{
		UserName: userName,
		Privkey:  []string{privateKey}}, 3)
	if err != nil {
		logrus.Panicf("注册导出授权用户失败:%s\n", err)
	}

	return exportclient, nil
}

//Importclient 2.注册导入授权的用户实例
func Importclient(userName, privateKey string) (*api.Client, error) {
	importclient, err := api.NewClientV2(&env.UserInfo{
		UserName: userName,
		Privkey:  []string{privateKey}}, 3)
	if err != nil {
		logrus.Panicf("注册导入授权用户失败:%s\n", err)
	}

	return importclient, nil
}

//UploadForAuth 3.通过上传接口,给导出授权的用户上传一个文件
func UploadForAuth(g *gin.Context) {
	userName := g.Query("userName")
	publicKey := g.Query("publicKey")
	bucketName := g.Query("bucketName")
	fileName := g.Query("fileName")
	filepath := g.Query("path")
	content := publicKey[3:]
	c := api.GetClient(content)

	privateKey := c.AccessorKey
	exportclient, err := exportclient(userName, privateKey)
	if err != nil {

	}
	md5, yerr := exportclient.UploadFile(filepath, bucketName, fileName)
	if yerr != nil {
		logrus.Panicf("上传文件失败:%s\n", yerr.Msg)
	}
	// return md5
	g.JSON(http.StatusOK, gin.H{"md5": md5})
}

//ExporterAuthData 导出授权文件证书，byte类型
func ExporterAuthData(g *gin.Context) {
	bucketName := g.Query("bucketName")
	fileName := g.Query("fileName")
	ownerPublic := g.Query("ownerPublic")
	otherPublicKey := g.Query("otherPublicKey")
	content := ownerPublic[3:]
	c := api.GetClient(content)
	exporter, yerr := c.ExporterAuth(bucketName, fileName)
	if yerr != nil {
		logrus.Panicf("初始化授权导出失败:%s\n", yerr.Msg)
	}

	authdata, yerr := exporter.Export(otherPublicKey)
	if yerr != nil {
		logrus.Panicf("导出授权文件失败:%s\n", yerr.Msg)
	}
	g.JSON(http.StatusOK, gin.H{"authdata": string(authdata[:])})
}

//ImporterAuth 导入授权文件
func ImporterAuth(g *gin.Context) {

	bucketName := g.Query("bucketName")
	fileName := g.Query("fileName")
	ownerPublic := g.Query("ownerPublic")
	// otherPublicKey := g.Query("otherPublicKey")
	authdata := []byte(g.Query("authdate"))
	content := ownerPublic[3:]
	c := api.GetClient(content)
	importer := c.ImporterAuth(bucketName, fileName)
	yerr := importer.Import(authdata)
	if yerr != nil {
		logrus.Panicf("导入授权文件失败:%s\n", yerr.Msg)
		g.JSON(http.StatusUnauthorized, gin.H{"status": "导入授权文件失败"})
	} else {
		g.JSON(http.StatusOK, gin.H{"status": "导入授权文件完成"})
	}

}
