package controller

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/api"
	"github.com/yottachain/YTCoreService/env"
)

//User 用户注册
type User struct {
	UserName   string `form:"userName" json:"userName" binding:"required"`
	PrivateKey string `form:"privateKey" json:"privateKey" xml:"privateKey" binding:"required"`
}

//Register 用户注册
func Register(g *gin.Context) {
	defer env.TracePanic("Register")
	var json User

	if err := g.Bind(&json); err != nil {
		g.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	}
	userName := json.UserName

	privateKey := json.PrivateKey
	// log.Info("userName : " + userName)
	// log.Info("privateKey : " + privateKey)

	// if count == 0 {
	_, err2 := api.NewClient(userName, privateKey)
	if err2 != nil {
		// CheckErr(err2)
		logrus.Infof("err:%s\n", err2)
		return
	}
	logrus.Infof("User Register Success,UserName: %s\n", userName)
	// }
	g.JSON(http.StatusOK, gin.H{"status": "Register success " + userName})
}
