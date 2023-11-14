package main

import (
	"github.com/gin-gonic/gin"
	"net/http"
)

const (
	SUCCESS = 200
	ERROR   = 500
)

type Response struct {
	Code int         `json:"code"`
	Data interface{} `json:"data"`
	Msg  string      `json:"msg"`
}

func FailWithMsg(msg string, c *gin.Context) {
	Result(ERROR, map[string]any{}, msg, c)
}

func OKWithMsg(msg string, c *gin.Context) {
	Result(SUCCESS, map[string]any{}, msg, c)
}

func OKWithData(data any, c *gin.Context) {
	Result(SUCCESS, data, "success", c)
}

func Result(code int, data any, msg string, c *gin.Context) {
	c.JSON(http.StatusOK, Response{
		Code: code,
		Data: data,
		Msg:  msg,
	})
}
