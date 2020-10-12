# DataX KafkaWriter


---

## 1 快速介绍

数据导入kafka的插件

## 2 实现原理

使用kafka的kafka-clients  maven依赖 api接口， 批量把从reader读入的数据写入kafka

## 3 功能说明

### 3.1 配置样例

#### job.json

```
{
  "job": {
    "setting": {
      "speed": {
        "channel":1
      }
    },
    "content": [
      {
        "reader": {
          ...
        },
        "writer": {
          "name": "kafkawriter",
          "parameter": {
            "topic": "test_topic",
            "bootstrapServers": "192.168.88.129:9092",
            "fieldDelimiter":""
          }
        }
        }
    ]
  }

}
```

#### 3.2 参数说明

* topic
 * 描述：kafka的主题
 * 必选：是
 * 默认值：无
 
* bootstrapServers 
  * 描述：kafka broker地址
  * 必选：是
  * 默认值

* fieldDelimiter
 * 描述：如果插入数据是array，就使用指定分隔符
 * 必选：否
 * 默认值：-,-


## 4 性能报告

### 4.1 环境准备


#### 4.1.3 DataX jvm 参数

-Xms1024m -Xmx1024m -XX:+HeapDumpOnOutOfMemoryError

### 4.2 测试报告



### 4.3 测试总结


## 5 约束限制

