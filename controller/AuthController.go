package controller

import (
	"crypto/rand"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/api"

	"github.com/gin-gonic/gin"
)

const AUTH_FAILURE = 0x55

//Exportclient 1.注册导出授权的用户实例
func exportclient(userName, privateKey string) (*api.Client, error) {

	exportclient, err := api.NewClient(&api.UserInfo{
		UserName: userName,
		Privkey:  []string{privateKey}}, 3)
	if err != nil {
		logrus.Panicf("注册导出授权用户失败:%s\n", err)
	}

	return exportclient, nil
}

//Importclient 2.注册导入授权的用户实例
func Importclient(userName, privateKey string) (*api.Client, error) {
	importclient, err := api.NewClient(&api.UserInfo{
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

	privateKey := c.SignKey.PublicKey
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
// func ExporterAuthData(g *gin.Context) {
// 	bucketName := g.Query("bucketName")
// 	fileName := g.Query("fileName")
// 	ownerPublic := g.Query("ownerPublic")
// 	otherPublicKey := g.Query("otherPublicKey")
// 	content := ownerPublic[3:]
// 	c := api.GetClient(content)
// 	exporter, yerr := c.ExporterAuth(bucketName, fileName)
// 	if yerr != nil {
// 		logrus.Panicf("初始化授权导出失败:%s\n", yerr.Msg)
// 	}
// 	newOtherPublicKey := otherPublicKey[3:]
// 	authdata, yerr := exporter.Export(newOtherPublicKey)
// 	if yerr != nil {
// 		logrus.Panicf("导出授权文件失败:%s\n", yerr.Msg)
// 	}
// 	g.JSON(http.StatusOK, gin.H{"authdata": authdata})
// }
func ExporterAuthData(g *gin.Context) {
	// defer env.TracePanic("ExporterAuthData")

	// bucketName := g.Query("bucketName")
	// fileName := g.Query("fileName")
	// ownerPublic := g.Query("ownerPublic")
	// otherPublicKey := g.Query("otherPublicKey")
	// content := ownerPublic[3:]
	// c := api.GetClient(content)
	// logrus.Infof("bucketName:%s\n", bucketName)
	// exporter, yerr := c.ExporterAuth(bucketName, fileName)
	// if yerr != nil {
	// 	logrus.Panicf("初始化授权导出失败:%s\n", yerr.Msg)
	// }
	// newOtherPublicKey := otherPublicKey[3:]
	// authdata, yerr := exporter.Export(newOtherPublicKey)
	// if yerr != nil {
	// 	logrus.Panicf("导出授权文件失败:%s\n", yerr.Msg)
	// }
	// logrus.Infof("-------------------------------------------------\n")

	// g.Header("Content-Type", "application/octet-stream")
	// // // g.Header("Content-Disposition", fileContentDisposition)
	// u1 := GetRandomString2(32)
	// directory := env.GetS3Cache() + "authdata"
	// writeCacheAuth(directory)
	// filePath := env.GetS3Cache() + "authdata/" + u1 + ".dat"
	// // WriteFile(filePath,authdata,perm os.FileMode)

	// err := ioutil.WriteFile(filePath, authdata, 0777)
	// if err != nil {
	// 	// handle error
	// }

	// g.Data(http.StatusOK, "Content-Type", authdata)
	// g.JSON(http.StatusOK, gin.H{"authPath": filePath})
}

type Auth struct {
	BucketName  string `form:"bucketName" json:"bucketName" binding:"required"`
	FileName    string `form:"fileName" json:"fileName" xml:"fileName" binding:"required"`
	OwnerPublic string `form:"ownerPublic" json:"ownerPublic" xml:"ownerPublic" binding:"required"`
	AuthPath    string `form:"path" json:"path" binding:"required"`
}

//ImporterAuth 导入授权文件
func ImporterAuth(g *gin.Context) {

	// var json Auth

	// if err := g.Bind(&json); err != nil {
	// 	g.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	// }
	// bucketName := json.BucketName
	// fileName := json.FileName
	// ownerPublic := json.OwnerPublic
	// // authdata := json.Authdata
	// authPath := json.AuthPath
	// newauthdata, err := ioutil.ReadFile(authPath) // just pass the file name
	// if err != nil {
	// 	fmt.Print(err)
	// }
	// // bucketName := g.Query("bucketName")
	// // fileName := g.Query("fileName")
	// // ownerPublic := g.Query("ownerPublic")
	// // otherPublicKey := g.Query("otherPublicKey")
	// // newauthdata := []byte(authdata)
	// // logrus.Infof("authdata2:\n", newauthdata)
	// content := ownerPublic[3:]
	// c := api.GetClient(content)
	// importer := c.ImporterAuth(bucketName, fileName)
	// yerr := importer.Import(newauthdata)
	// if yerr != nil {
	// 	logrus.Panicf("导入授权文件失败:%s\n", yerr.Msg)
	// 	g.JSON(http.StatusUnauthorized, gin.H{"status": "导入授权文件失败"})
	// } else {
	// 	del := os.Remove(authPath)
	// 	if del != nil {
	// 		fmt.Println(del)
	// 	}
	// 	logrus.Info("..........授权文件已经被清理..........%s\n")
	// 	g.JSON(http.StatusOK, gin.H{"status": "导入授权文件完成"})
	// }

}

func writeCacheAuth(directory string) error {

	s, err := os.Stat(directory)
	if err != nil {
		if !os.IsExist(err) {
			err = os.MkdirAll(directory, os.ModePerm)
			if err != nil {
				return err
			}
		} else {
			return err
		}
	} else {
		if !s.IsDir() {
			return errors.New("The specified path is not a directory.")
		}
	}
	if !strings.HasSuffix(directory, "/") {
		directory = directory + "/"
	}

	return nil
}

func GetRandomString2(n int) string {
	randBytes := make([]byte, n/2)
	rand.Read(randBytes)
	return fmt.Sprintf("%x", randBytes)
}

func LicensedTo(g *gin.Context) {
	publicKeyA := g.Query("ownerPublickey")
	userNameB := g.Query("otherUserName")
	publicKeyB := g.Query("otherPublickey")
	bucketName := g.Query("bucketName")
	objectKey := g.Query("objectKey")

	content := publicKeyA[3:]
	clientA := api.GetClient(content)

	newPublicKeyB := publicKeyB[3:]

	auth, yeer := clientA.Auth(bucketName, objectKey)
	if yeer != nil {
		g.JSON(http.StatusSeeOther, gin.H{"yeer code": yeer.Code, "Msg": yeer.Msg})
	} else {
		beer := auth.LicensedTo(userNameB, newPublicKeyB)
		if beer != nil {
			logrus.Infof("UserNameB:%s\n", userNameB)
			g.JSON(http.StatusSeeOther, gin.H{"beer code": beer.Code, "Msg": beer.Msg})
		} else {
			g.JSON(http.StatusOK, gin.H{"msg": "[ " + objectKey + " ] 授权给用户" + userNameB + " 完成."})
		}
	}

}
