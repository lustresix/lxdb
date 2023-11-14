package main

import (
	"github.com/gin-gonic/gin"
)

type Request struct {
	Data map[string]string `json:"data" msg:"请输入存入的数据"`
	Key  []string          `json:"key" msg:"请输入查询的数据"`
}
type CommonApi struct {
}

func (CommonApi) PutApi(c *gin.Context) {
	var cr Request
	if err := c.ShouldBindJSON(&cr); err != nil {
		FailWithMsg(err.Error(), c)
		return
	}
	err := putData(cr.Data)
	if err != nil {
		FailWithMsg(err.Error(), c)
		return
	}
	OKWithMsg("数据插入成功!", c)
	return
}

func (CommonApi) GetApi(c *gin.Context) {
	var cr Request
	if err := c.ShouldBindJSON(&cr); err != nil {
		FailWithMsg(err.Error(), c)
		return
	}
	value, err := getValue(cr.Key)
	if err != nil {
		FailWithMsg(err.Error(), c)
		return
	}
	OKWithData(value, c)
	return
}

func (CommonApi) DelApi(c *gin.Context) {
	var cr Request
	if err := c.ShouldBindJSON(&cr); err != nil {
		FailWithMsg(err.Error(), c)
		return
	}
	err := deleteData(cr.Key)
	if err != nil {
		FailWithMsg(err.Error(), c)
		return
	}
	OKWithMsg("数据删除成功!", c)
	return
}

func (CommonApi) ListApi(c *gin.Context) {
	keys := ListKey()
	OKWithData(keys, c)
	return
}
