package controller

import (
	"bytes"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/api"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTS3/backend/s3mem"
)

//User 用户注册
type User struct {
	UserName   string `form:"userName" json:"userName" binding:"required"`
	PrivateKey string `form:"privateKey" json:"privateKey" xml:"privateKey" binding:"required"`
}

func Login(g *gin.Context) {
	var content bytes.Buffer
	content.WriteString("<!DOCTYPE html>\n\n")
	content.WriteString("<html>\n\n")
	content.WriteString("	<head>\n\n")
	content.WriteString("		<title>登录</title>\n\n")
	content.WriteString("		<meta charset=\"UTF-8\" name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">\n\n")
	content.WriteString("	</head>\n\n")
	content.WriteString("	<body>\n\n")
	content.WriteString("   <p>登录:</p>\n")
	content.WriteString("<form action=\"/api/v1/insertuser\" method=\"post\"  name=\"form1\" id=\"form1\">\n")
	content.WriteString("<p>用户名: <input type=\"text\" name=\"userName\" value=\"\" /> </p>\n")
	content.WriteString("<p>私钥: <input type=\"text\" name=\"privateKey\" value=\"\" /></p>\n")
	content.WriteString("<p> <input type=\"submit\" name=\"submit\" id=\"submit\" value=\"提交\" /> </p>\n")
	content.WriteString("</form>\n")
	content.WriteString("	 </body>\n\n")
	content.WriteString("</html>")
	g.Writer.Header().Set("Content-Type", "text/html")
	g.Writer.WriteString(string(content.Bytes()))

}

//Register 用户注册
func Register(g *gin.Context) {
	defer env.TracePanic("Register")
	ii := 1
	var userName string = g.Request.FormValue("userName")
	var privateKey string = g.Request.FormValue("privateKey")
	if userName == "" || privateKey == "" {
		var json User
		if err := g.Bind(&json); err != nil {
			g.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			userName = json.UserName
			privateKey = json.PrivateKey
		}
	}
	if userName == "" || privateKey == "" {
		logrus.Info("[Register]userName or privateKey is empty\n")
		g.JSON(http.StatusUnauthorized, gin.H{"status": http.StatusUnauthorized, "Msg": "userName or privateKey is empty"})
		return
	}
	var client *api.Client
	var err2 error
	for {
		client, err2 = api.NewClientV2(&env.UserInfo{
			UserName: userName,
			Privkey:  []string{privateKey}}, 3)
		if err2 != nil {
			ii++
			if ii <= 3 {
				time.Sleep(time.Second * 5)
			} else {
				logrus.Infof("[Register]err:%s\n", err2)
				break
			}
		} else {
			break
		}
	}
	if err2 != nil {
		logrus.Errorf("[Register]User Register Failed, %s\n", err2)
		g.JSON(http.StatusUnauthorized, gin.H{"status": http.StatusUnauthorized, "Msg": "Register Failed!Please checked userName and privateKey "})
	} else {
		db := s3mem.New()
		_, initerr := db.ListBuckets(client.SignKey.PublicKey)
		if initerr != nil {
			return
		}
		logrus.Infof("[Register]User Register Success,UserName: %s\n", userName)
		g.JSON(http.StatusOK, gin.H{"status": http.StatusOK, "Msg": "Register success " + userName})
	}
}

func AddPubkey(g *gin.Context) {

	userName := g.Query("userName")
	publicKey := g.Query("publicKey")
	logrus.Infof("userName:%s\n", userName)
	logrus.Infof("publicKey:%s\n", publicKey)
	content := publicKey[3:]

	num, err := api.AddPublicKey(userName, content)

	if err != nil {
		logrus.Infof("err:%s\n", err)
		g.JSON(http.StatusAccepted, err)
	} else {
		logrus.Infof("NUM:%d\n", num)
		g.JSON(http.StatusOK, num)
	}
	//
	//	// api.AddP
	//
}
