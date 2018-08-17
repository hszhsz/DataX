# DataX  CassandraWriter


---


## 1 快速介绍

CassandraWriter 插件实现了写入数据到 cassandra 目的表的功能。在底层实现上， CassandraWriter 通过 JDBC 连接远程 cassandra 数据库，并执行相应的 insert into ... 的 sql 语句将数据写入 cassandra，内部会分批次提交入库。


## 2 功能说明

### 2.1 配置样例

* 这里使用一份从cassandra 读取， cassandra 导入的数据。

```json
{ "job":{
	"content":[
		{
			"reader":{
				"name":"cassandrareader",
				"parameter":{
					"host":"localhost",
					"connection":{
						"distance_max":2,
						"distance_min":1,
						"host":"localhost",
						"local_max":2,
						"local_min":1,
						"password":"",
						"port":9042,
						"username":""
					},
					"contactPoint":"localhost",
					"mode":"normal",
					"password":"",
					"allowFilter":true,
					"querySql":
						"select a,b,c,d from test.test where a>0 and f=0 ",
					"splitPk":"a",
					"username":""
				}
			},
			"writer":{
				 "name": "streamwriter",
                    "parameter": {
                        "print":true
                    }
	}}
	],
	"setting":{
		"speed":{
			"channel":"2"
		}
	}
}
}

```


### 3 参数说明

* **host**

	* 描述：目的数据库的 JDBC 连接信息

 	* 必选：是 <br />

	* 默认值：无 <br />

* **username**

	* 描述：目的数据库的用户名 <br />

	* 必选：否 <br />

	* 默认值：无 <br />

* **password**

	* 描述：目的数据库的密码 <br />

	* 必选：否 <br />

	* 默认值：无 <br />

* **table**

	* 描述：目的表的表名称

	* 必选：是 <br />

	* 默认值：无 <br />

* **column**

	* 描述：目的表需要写入数据的字段,字段之间用英文逗号分隔。例如: "column": ["id","name","age"]。如果要依次写入全部列，使用*表示, 例如: "column": ["*"]。

			**column配置项必须指定，不能留空！**

               注意：1、我们强烈不推荐你这样配置，因为当你目的表字段个数、类型等有改动时，你的任务可能运行不正确或者失败
                    2、 column 不能配置任何常量值

	* 必选：是 <br />

	* 默认值：否 <br />

* **preSql**

	* 描述：写入数据到目的表前，会先执行这里的标准语句。如果 Sql 中有你需要操作到的表名称，请使用 `@table` 表示，这样在实际执行 Sql 语句时，会对变量按照实际表名称进行替换。比如你的任务是要写入到目的端的100个同构分表(表名称为:datax_00,datax01, ... datax_98,datax_99)，并且你希望导入数据前，先对表中数据进行删除操作，那么你可以这样配置：`"preSql":["delete from 表名"]`，效果是：在执行到每个表写入数据前，会先执行对应的 delete from 对应表名称 <br />

	* 必选：否 <br />

	* 默认值：无 <br />

* **batchSize**

	* 描述：一次性批量提交的记录数大小，该值可以极大减少DataX与cassandra的网络交互次数，并提升整体吞吐量。但是该值设置过大可能会造成DataX运行进程OOM情况。<br />

	* 必选：否 <br />

	* 默认值：1024 <br />

* **duration**

	* 描述：批量提交等最大间隔时间，单位秒，该值与batchSize结合，可以极大减少DataX与cassandra的网络交互次数，并提升整体吞吐量。但是该值设置过大可能会造成DataX运行进程OOM情况。<br />

	* 必选：否 <br />

	* 默认值：1 <br />

* **ttl**
    * 描述：写入数据的存活时间，单位秒

    * 必选：否 <br />

    * 默认值：无 <br />



