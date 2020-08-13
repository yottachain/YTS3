package controller

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/common/log"
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
	defer env.TracePanic()
	var json User

	if err := g.Bind(&json); err != nil {
		g.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	}
	userName := json.UserName

	privateKey := json.PrivateKey
	// log.Info("userName : " + userName)
	// log.Info("privateKey : " + privateKey)

	// if count == 0 {
	c, err2 := api.NewClient(userName, privateKey)
	if err2 != nil {
		// CheckErr(err2)
		log.Info(err2)
	}
	log.Info("User Register Success,UserName:" + userName)
	fmt.Println("UserID:", c.UserId)
	// }
	g.JSON(http.StatusOK, gin.H{"status": "Register success " + userName})
}
