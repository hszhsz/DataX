# CassandraReader 插件文档


___



## 1 快速介绍

CassandraReader插件实现了从Cassandra读取数据。在底层实现上，CassandraReader通过JDBC连接远程Cassandra数据库，并执行相应的sql语句将数据从mysql库中SELECT出来。

**不同于其他关系型数据库，CassandraReader CQL有很多不同**  **语法参考 http://git.terminus.io/bigdata/datax**

## 2 实现原理

简而言之，CassandraReader通过JDBC连接器连接到远程的Cassandra数据库，并根据用户配置的querySql信息，CassandraReader直接将其发送到Cassandral数据库，并将该SQL执行返回结果使用DataX自定义的数据类型拼装为抽象的数据集，并传递给下游Writer处理。



## 3 功能说明

### 3.1 配置样例

* 配置一个从Cassandra数据库同步抽取数据到本地的作业:

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
					"mode":"normal",
					"password":"",
					"allowFilter":true,
					"querySql":
						"select a,b,c,d from test.test where a>0 and f=0",
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



### 3.2 参数说明


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

* **querySql**

	* 描述：查询数据的cql，注意语法

	* 必选：是 <br />

	* 默认值：无 <br />

* **splitPk**

	* 描述：CassandraReader进行数据抽取时，如果指定splitPk，表示用户希望使用splitPk代表的字段进行数据分片，DataX因此会启动并发任务进行数据同步，这样可以大大提供数据同步的效能。

	  splitPk需要是表主键，因为表主键通常情况下比较均匀，因此切分出来的分片也不容易出现数据热点。

	  目前splitPk仅支持整形和字符串数据切分，`不支持浮点、日期等其他类型`。如果用户指定其他非支持类型，CassandraReader将报错！

	  如果splitPk不填写，包括不提供splitPk或者splitPk值为空，DataX视作使用单通道同步该表数据。

	* 必选：否 <br />

	* 默认值：空 <br />

* **allowFilter**
    * 描述：是否允许fliter

	* 必选：false <br />

	* 默认值：false <br />


```
CREATE TABLE test.test (
    a int PRIMARY KEY,
    b list<text>,
    c set<text>,
    d map<text, text>,
    e map<text, int>,
    f int,
    g map<int, text>,
    h list<int>,
    temp text
)


```