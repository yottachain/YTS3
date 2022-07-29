package controller

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/env"
	ytcrypto "github.com/yottachain/YTCrypto"
)

const (
	//Function tag
	Function = "function"
	//SNID tag
	SNID = "snID"
	//MinerID tag
	MinerID = "minerID"
	//BlockID tag
	BlockID = "blockID"
	//ShardID tag
	ShardID = "shardID"
)

const (
	Success             = 0
	ReadRequestFailed   = -1
	ReadResponseFailed  = -2
	InvalidParameter    = -3
	SignFailed          = -4
	VerifyFailed        = -5
	JsonMarshalFailed   = -6
	CreateRequestFailed = -7
	ServerProcessFailed = -8
	SendTrx1Failed      = -10
	SendTrx2Failed      = -11
)

var codeMap = map[int]string{
	Success:             "ok",
	ReadRequestFailed:   "parse http request failed",
	ReadResponseFailed:  "parse http response failed",
	InvalidParameter:    "invalid parameters",
	SignFailed:          "sign failed",
	VerifyFailed:        "verify signature failed",
	JsonMarshalFailed:   "format json data failed",
	CreateRequestFailed: "create http request failed",
	ServerProcessFailed: "server processing failed",
	SendTrx1Failed:      "transaction1 execute failed",
	SendTrx2Failed:      "transaction2 execute failed",
}

type Response struct {
	Code        int    `json:"code"`
	Msg         string `json:"msg"`
	AccountName string `json:"accountName"`
}

type CreateRespCli struct {
	AccountName string `json:"accountName"`
	OwnerPK     string `json:"ownerPK"`
	OwnerSK     string `json:"ownerSK"`
	StoragePK   string `json:"storagePK"`
	StorageSK   string `json:"storageSK"`
}

type ResponseCli struct {
	Code int            `json:"code"`
	Msg  string         `json:"msg"`
	Data *CreateRespCli `json:"data"`
}

type CreateParam struct {
	AccountPubKey string `json:"accountPubKey"`
	StoragePubKey string `json:"storagePubKey"`
	Signature     string `json:"signature"`
}

func CreateResponseCli(code int, err error, data *CreateRespCli) *ResponseCli {
	msg := codeMap[code]
	if err != nil {
		msg = fmt.Sprintf("%s: %s", msg, err.Error())
	}
	return &ResponseCli{Code: code, Msg: msg, Data: data}
}

type AccClient struct {
	//server    *echo.Echo
	httpCli   *http.Client
	serverURL string
}

//NewClient create new client instance
func NewClient(serverURL string) *AccClient {
	//return &AccClient{server: echo.New(), httpCli: &http.Client{}, serverURL: serverURL}
	return &AccClient{httpCli: &http.Client{}, serverURL: serverURL}
}

func CreateAccountCli(g *gin.Context) {
	serverURL := env.GetConfig().GetString("eosServerURL", "http://150.138.84.47:8080")
	newClient := NewClient(serverURL)
	entry := log.WithFields(log.Fields{Function: "CreateAccountCli"})
	//创建用户公私钥
	privKeyOwner, pubKeyOwner := ytcrypto.CreateKey()
	//创建存储公私钥
	privKeyStorage, pubKeyStorage := ytcrypto.CreateKey()
	//使用私钥5KauH6CfJ2UEVi119ukjdBq3GStSF9wnQAgciGb6qNhq6zLZrFM对两个公钥签名，格式为"用户公钥|存储公钥"，公钥均包含YTA前缀
	signature, err := ytcrypto.Sign("5KauH6CfJ2UEVi119ukjdBq3GStSF9wnQAgciGb6qNhq6zLZrFM", []byte(fmt.Sprintf("%s|%s", fmt.Sprintf("YTA%s", pubKeyOwner), fmt.Sprintf("YTA%s", pubKeyStorage))))
	if err != nil {
		entry.WithError(err).Error("create signature failed")
		g.JSON(http.StatusInternalServerError, CreateResponseCli(JsonMarshalFailed, err, nil))
		//g.JSON(http.StatusInternalServerError, gin.H{"status": http.StatusInternalServerError, "Msg": "create signature failed"})
	}
	//创建账号请求结构体
	request := &CreateParam{AccountPubKey: fmt.Sprintf("YTA%s", pubKeyOwner), StoragePubKey: fmt.Sprintf("YTA%s", pubKeyStorage), Signature: signature}
	//JSON序列换结构体
	jsonStr, err := json.Marshal(request)
	if err != nil {
		entry.WithError(err).Error("json marshal failed")
		g.JSON(http.StatusInternalServerError, CreateResponseCli(JsonMarshalFailed, err, nil))
	}
	//POST发送至服务端的account/create接口
	req, err := http.NewRequest("POST", fmt.Sprintf("%s/%s", serverURL, "account/create"), bytes.NewBuffer(jsonStr))
	if err != nil {
		entry.WithError(err).Error("create request failed")
		g.JSON(http.StatusInternalServerError, CreateResponseCli(CreateRequestFailed, err, nil))
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := newClient.httpCli.Do(req)
	if err != nil {
		entry.WithError(err).Error("server processing failed")
		g.JSON(http.StatusInternalServerError, CreateResponseCli(ServerProcessFailed, err, nil))
	}
	defer resp.Body.Close()
	//HTTP响应码不为200则返回错误
	if resp.StatusCode != http.StatusOK {
		err := fmt.Errorf("status code is %d", resp.StatusCode)
		entry.WithError(err).Error("server failed")
		g.JSON(http.StatusInternalServerError, CreateResponseCli(ServerProcessFailed, err, nil))
	}
	reader := io.Reader(resp.Body)
	//响应为gzip压缩格式时的内容处理
	if strings.Contains(resp.Header.Get("Content-Encoding"), "gzip") {
		gbuf, err := gzip.NewReader(reader)
		if err != nil {
			entry.WithError(err).Error("decompress request body failed")
			g.JSON(http.StatusInternalServerError, CreateResponseCli(ReadResponseFailed, err, nil))
		}
		reader = io.Reader(gbuf)
		defer gbuf.Close()
	}
	//反序列换服务端响应为Response结构体
	serverResp := new(Response)
	err = json.NewDecoder(reader).Decode(serverResp)
	if err != nil {
		entry.WithError(err).Error("decode response failed")
		g.JSON(http.StatusInternalServerError, CreateResponseCli(JsonMarshalFailed, err, nil))
	}
	//响应码不为0时报错
	if serverResp.Code != Success {
		logrus.Info("")
		entry.WithError(err).Error("create account failed")
		g.JSON(http.StatusInternalServerError, CreateResponseCli(ServerProcessFailed, errors.New(serverResp.Msg), nil))
	}
	entry.Infof("create account %s: owner pubkey -> %s, storage pubkey -> %s", serverResp.AccountName, pubKeyOwner, pubKeyStorage)
	//账号创建成功后返回CreateRespCli结构体
	g.JSON(http.StatusOK, CreateResponseCli(Success, nil, &CreateRespCli{AccountName: serverResp.AccountName, OwnerPK: fmt.Sprintf("YTA%s", pubKeyOwner), OwnerSK: privKeyOwner, StoragePK: fmt.Sprintf("YTA%s", pubKeyStorage), StorageSK: privKeyStorage}))
}
