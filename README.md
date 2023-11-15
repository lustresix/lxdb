
[![piYRNbF.png](https://z1.ax1x.com/2023/11/15/piYRNbF.png)](https://imgse.com/i/piYRNbF)
# 👋LxDB

类似于redis，基于`bitcask`内核的k-v数据库。支持事务，resp，http接口，提供以Btree，AdaptiveRadixTree，B+tree为基础的索引。

[![MIT License](https://img.shields.io/badge/License-MIT-green.svg)](https://choosealicense.com/licenses/mit/)                      ![Static Badge](https://img.shields.io/badge/go-100%25-blue)
[![GPLv3 License](https://img.shields.io/badge/License-GPL%20v3-yellow.svg)](https://opensource.org/licenses/)           [![AGPL License](https://img.shields.io/badge/license-AGPL-blue.svg)](http://www.gnu.org/licenses/agpl-3.0)


## 🏁部署项目

要部署这个项目，请运行

```bash
  go get https://github.com/lustresix/lxdb
```


## 🚀使用方法/示例
### 快速使用

```go
package main

import (
    "fmt"
	LustreDB "github.com/lustresix/lxdb"
)

func main() {
  // simple
	options := LustreDB.DefaultOptions
	db, _ := LustreDB.Open(options)

	err = db.Put([]byte("hello"), []byte("world1"))

	get, _ := db.Get([]byte("hello"))
	fmt.Println("val = ", string(get))

	_ = db.Delete([]byte("hello"))

    // Transaction
    batchOptions := LustreDB.DefaultWriteBatchOptions
	wb := open.NewWriteBatch(batchOptions)
	
    _ = wb.Put([]byte("hello1"), []byte("world1"))
	_ = wb.Delete([]byte("hello1"))
	
    err = wb.Commit()
	if err != nil {
		return 
	}
}

```
### redis-cli
启动数据库
```bash
.\cmd
```
打开redis-cli
```bash
redis-cli -p 6380
```
### http支持
```bash
.\http
```
端口: 8080
```bash
[GIN-debug] POST   /put                      --> main.CommonApi.PutApi-fm (3 handlers)
[GIN-debug] POST   /get                      --> main.CommonApi.GetApi-fm (3 handlers)
[GIN-debug] POST   /delete                   --> main.CommonApi.DelApi-fm (3 handlers)
[GIN-debug] GET    /list                     --> main.CommonApi.ListApi-fm (3 handlers)
```
### API 参考

#### 存入k-v

```http
  POST /put
```

| 参数 | 类型     | 描述                |
| :-------- | :------- | :------------------------- |
| `data` | `map[string]string ` | **必选**. key and value |

#### 获取value

```http
  GET /get
```

| 参数 | 类型     | 描述                       |
| :-------- | :------- | :-------------------------------- |
| `key`      | `[]string` | **必选**. 要获取的value的key |

#### 删除k-v

```http
  POST /delete
```

| 参数 | 类型     | 描述                       |
| :-------- | :------- | :-------------------------------- |
| `key`      | `[]string` | **必选**. 要删除的key |

#### 获取key的列表

```http
  GET /list
```

## 📢技术栈

**语言:** Golang

**内核基础:** Bitcask

## 🔮存储结构
#### 数据文件
[![piYc336.png](https://z1.ax1x.com/2023/11/15/piYc336.png)](https://imgse.com/i/piYc336)

#### 事务逻辑
上传事务，自增seq，完成时为`seq+tex-fin`如果没有读取到则说明事务失败，不保存到索引中
[![piYgZGt.png](https://z1.ax1x.com/2023/11/15/piYgZGt.png)](https://imgse.com/i/piYgZGt)

## ✅ ToDOlist

- [ ] IO优化（文件锁，分区缓存）。
- [x] 兼容Redis协议和命令。
- [x] 支持http服务。
- [x] 支持tcp服务。
- [ ] 数据备份。
- [ ] 分布式集群。
## 📝反馈

如果你有任何反馈，请联系我：1911827241@qq.com

