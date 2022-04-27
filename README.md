​	

<a rel="noreferrer" target="_blank"></a>

<meta name="referrer" content="no-referrer">

# 数据集成路线2作业2实验报告

<center><h2>37组</h2></center>

## 团队成员及分工

| 姓名   | 学号      |
| ------ | --------- |
| 袁军平 | 191250189 |
| 吴子玥 | 191250153 |
| 崔晟   | 191870026 |
| 严佳欣 | 191850170 |
| 吉祥   | 191250056 |

 

我们小组是数据库表和流数据分开搭建的环境分开做的任务 所以我们分成两个部分叙述 以下是数据库表部分

## 数据库表部分

### 环境搭建

Ubuntu 21.04 本地虚拟机 Ubuntu21.04

![](https://i0.hdslb.com/bfs/album/4943f28dd9efaace8af08268cd2bf97298b1d518.jpg)		

`Jdk1.8` 参考了 https://www.cnblogs.com/stulzq/p/9286878.html 这篇博客在虚拟机上搭建了`jdk1.8` 环境并设置了环境变量

![](https://i0.hdslb.com/bfs/album/f60238e24d4342b0d7f1fcc34852019fe41bbc97.jpg) 

Hadoop2.7.4 https://blog.csdn.net/isoleo/article/details/78394777这篇博客在虚拟机上搭建了`hadoop2.7.4`

​		![](https://i0.hdslb.com/bfs/album/2cd341a91e84898516e756b714af89e271d4f115.jpg)

`Spark2.3.3` 参考链接 https://blog.csdn.net/walkcode/article/details/104208855 以及qq群里助教给的安装包搭建了 `spark` 环境

![](https://i0.hdslb.com/bfs/album/dfd775d40eb36bf828640a841af8684c1a2aee81.jpg) 

`ClickHouse` 环境搭建 参考官网文档https://clickhouse.com/docs/zh/getting-started/install/ 进行了 `Clickhouse` 环境搭建

![](https://i0.hdslb.com/bfs/album/add283b77c4ad914178cc6a9b37c5a9c3f5a1398.jpg@1e_1c.webp) 

### 如何确保尽可能及时 完成地获取数据

使用 `SparkSession` 连接 `HiveServer2`。根据助教提供的远程数仓信息进行连接读取数据。

![](https://i0.hdslb.com/bfs/album/d6bdea1f13dac091f4a39f4fc402d1530d9215d3.jpg) 

### 如何完成数据转换

通过 `DataFrame` 对于得到的数据进行数据转换

### 如何转储数据

通过 `clickhouse` 官网文档，使用 `jdbc` 连接 `clickHouse` 仓库，进行数据写入

## 实验内容（数据库表部分）

1. 读取远程数据仓库的内容 读取的表已在代码中体现

![](https://i0.hdslb.com/bfs/album/6c50c561148c5db33b9813fe6e0eedf37495f365.jpg) 

2. 对于得到的数据进行处理 其中 我们对于表 `pri_cust_contact_info` 进行了处理

![](https://i0.hdslb.com/bfs/album/6970e946e395e55844444a1e59a46609ee1cb64a.jpg) 

3. 数据写入 `clickHouse` 中对应的数据库 `group37`  参考官网文档 

![](https://i0.hdslb.com/bfs/album/b49878d0097e9729b3ab327cf53900e2d5f9b636.jpg) ![](https://i0.hdslb.com/bfs/album/77eb7f5cc93e6ba08646995b58c1e03796ab9923.jpg)

4. 最终结果

![](https://i0.hdslb.com/bfs/album/189cf9bd2894328aa4b3cfff4b144dd968c5a793.jpg) 

![](https://i0.hdslb.com/bfs/album/3a0cd47ac13982006824eb7aa732e8084b5baade.jpg) 

![](https://i0.hdslb.com/bfs/album/1d750a55a548d2dbeb5d63beb4bf0c3e47d93f4c.jpg) 

## 流数据部分

### 描述你获取和处理数据的过程

#### 环境搭建

全部使用 docker. clickhouse 作为一个数据库似乎很不适合容器化, 但对于一个作业来说还是可用的

包含的服务如下, 全部为当前最新版镜像

1. clickhouse
2. kafka (不依赖 Zookeeper)
3. flink (jobmanager+taskmanager)

```yaml
version: '3.3'
services:
  clickhouse-server:
      container_name: clickhouse
      ulimits:
          nofile:
              soft: 262144
              hard: 262144
      ports:
          - '18123:8123'
          - '19000:9000'
          - '19009:9009'
      image: yandex/clickhouse-server
      volumes:
        - "./config/clickhouse-server:/etc/clickhouse-server"
  kafka:
    container_name: kafka
    image: 'bitnami/kafka:latest'
    restart: always
    volumes:
      - "./data:/data/txt"
    ports:
      - '9094:9094'
    expose:
      - "9092"
    command:
      - 'sh'
      - '-c'
      - '/opt/bitnami/scripts/kafka/setup.sh && kafka-storage.sh format --config "$${KAFKA_CONF_FILE}" --cluster-id "lkorDA4qT6W1K_dk0LHvtg" --ignore-formatted  && /opt/bitnami/scripts/kafka/run.sh' # Kraft specific initialise
    environment:
    # 举例
    # kafka 始终位于 docker 容器中, 创建 topic 时, --bootstrap-server 指定为 kafka:9092
    # 若与 kafka 位于同一个 docker 网络, 指定 kafka 的 url 为 kafka:9092(kafka 是容器名), 
    # 与与 kafka 不在同一个网络, 指定 kafka 的 url 为 宿主机ip:9094
      - KAFKA_BROKER_ID=1
      - KAFKA_CFG_BROKER_ID=1
      - KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=1@127.0.0.1:9093
      - KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT1:PLAINTEXT
      - KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER
      - KAFKA_CFG_LOG_DIRS=/tmp/logs
      - KAFKA_CFG_PROCESS_ROLES=broker,controller
      - KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,PLAINTEXT1://:9094,CONTROLLER://:9093
      - KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092,PLAINTEXT1://172.17.0.1:9094
      - ALLOW_PLAINTEXT_LISTENER=yes
      - KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE=true
  # 在 kafka 启动后做一些配置
  kafkaset:
    container_name: kafkaset
    image: 'bitnami/kafka:latest'
    depends_on:
      - kafka
    restart: "no"
    entrypoint: ["sh", "-c","/opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092  --create  --topic test"]
 
  jobmanager:
    image: flink
    expose:
      - "6123"
    ports:
      - "8081:8081"
    command: jobmanager
    environment:
      - JOB_MANAGER_RPC_ADDRESS=jobmanager
  taskmanager:
    image: flink
    expose:
      - "6121"
      - "6122"
    depends_on:
      - jobmanager
    command: taskmanager
    links:
      - "jobmanager:jobmanager"
    environment:
      - JOB_MANAGER_RPC_ADDRESS=jobmanager

```

启动方法

```bash
docker-compose up -d
```

#### 获取数据

```bash
docker exec -it kafka /bin/bash
# 直接从控制台读取文件生产
cat data.txt |  /opt/bitnami/kafka/bin/kafka-console-producer.sh --broker-list kafka:9092 --topic test
```



使用 `flink-connector-kafka_2.12 1.14.4` , 与 flink 1.14.4 兼容. 使用 `OffsetsInitializer.committedOffsets` 而不是 `latest` 或者 `earliest`, 相当于保存了 offset , 发生意外可以继续消费. flink 离线的时候推送的消息也能正常消费

```scala
val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
val props = new Properties()
props.put("bootstrap.servers", "kafka:9092") 
props.put("auto.offset.reset", "earliest")
props.put("group.id", "test_flink")
// 需要设置 checkpoint, 从尚未 commit 的消息开始消费
val source: KafkaSource[String] = KafkaSource.builder[String].setBootstrapServers(props.getProperty("bootstrap.servers")).setTopics("test").setGroupId("my-group").setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.valueOf(
    props.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)
    .asInstanceOf[String]
    .toUpperCase(Locale.ROOT)
))).setValueOnlyDeserializer(new SimpleStringSchema).build

// 可以直接从 value 中获取 json 字符串
val value: DataStream[String] = env.fromSource(source, WatermarkStrategy.noWatermarks[String], "Kafka Source")
env.execute("Flink Streaming Java API Skeleton")

```

由于分了 14 个表, 使用 sideout 分流

```scala
val value1 = value.map(s => JsonMethods.parseOpt(s) match {
    // 解析成功
    case Some(value) => value
    // json 字符串无法解析
    case _ => JNothing
}).process(new MdSplitProcessFunction)
```

`MdSplitProcessFunction` 是自定义的 `ProcessFunction` , 根据 `eventType` 分流

```scala
// 需要实现这个方法
public abstract void processElement(I var1, ProcessFunction<I, O>.Context var2, Collector<O> var3) throws Exception;
```

使用 sideout 需要定义 `OutPutTags`, 举例

```scala
lazy val shopStream = new OutputTag[JValue]("shop")
context.output(shopStream, i) 
// 之后就可以对具体的流进行处理了
value1.getSideOutput(shopStream).map(...).addSink(...)
```

#### 数据转换

flink 有一套自己的 api, 但是我太不熟悉, 此处直接用 json4s 做了, 只需要定义一个 `case class`, 然后 `JsonMethods.parse(json).extract[T]` 就能转换成对应的 `case class`. 因为流数据的字段不全, 所以所有的字段都定义为 `Option` 的, 这样, 如果流数据 json 中无某个字段, `case class` 的对应字段指为 `None`

```scala
case class Shop(tran_channel: Option[String],
                order_code: Option[String],
                shop_code: Option[String],
                shop_name: Option[String],
                hlw_tran_type: Option[String],
                tran_date: Option[String],
                tran_time: Option[String],
                tran_amt: Option[Double],
                current_status: Option[String],
                score_num: Option[Double],
                uid: Option[String],
                legal_name: Option[String],
                etl_dt: Option[String],
                pay_channel: Option[String]
               )
```

自定义 `MapFunction`, 因为在分流的时候已经将 json 字符串转换为 `JValue` 对象, 此处直接 `extract` 就行了

```scala
class MapFuncionImp[T](implicit mf: Manifest[T], ti: TypeInformation[T]) extends MapFunction[JValue,T] {
  override def map(t: JValue): T = {
    implicit val formats: Formats = DefaultFormats + StringToInt + StringToDouble
    (t \ "eventBody").extractOpt[T] match {
      case Some(value) => value
    }
  }
}
// 像这样调用
 sideoutList.map(new MapFuncionImp[Shop]).addSink(...)
```



另外本次实验中尽管有些数据库表字段类型为数字, json 中还是有 `""` 包围, 所以还要手动实现两个 `json4s` 的序列化器

```scala
object StringToDouble extends CustomSerializer[Double](format => ( {
  case JString(x) => x.toDouble
}, {
  case x: Double => JDouble(x)
}))
object StringToInt extends CustomSerializer[Int](format => ( {
  case JString(x) => x.toInt
}, {
  case x: Int => JInt(x)
}))
```



#### 转储数据

比较麻烦, 因为我没找到能直接存内置数据类型的方法, 就用 `JDBC connector` 做了 https://nightlies.apache.org/flink/flink-docs-master/zh/docs/connectors/datastream/jdbc/

apache flink 官网的样例如下

```scala
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env
        .fromElements(...)
        .addSink(JdbcSink.sink(
                "insert into books (id, title, author, price, qty) values (?,?,?,?,?)",
                (ps, t) -> {
                    ps.setInt(1, t.id);
                    ps.setString(2, t.title);
                    ps.setString(3, t.author);
                    ps.setDouble(4, t.price);
                    ps.setInt(5, t.qty);
                },
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(getDbMetadata().getUrl())
                        .withDriverName(getDbMetadata().getDriverClass())
                        .build()));
env.execute();
```

可以看到需要手动给每个字段赋值, 非常麻烦, 于是我使用反射来做这件事, 少写一些代码



获取 sink 的方法, O 是样例类的类型 

```scala
def getSink[O](CkJdbcUrl: String, batchSize: Int)(implicit c: ClassTag[O]): SinkFunction[O] = {
    val clazz = classTag[O].runtimeClass
    val list = clazz.getDeclaredFields.toList
    JdbcSink.sink[O](
      // 将 case class 的字段名按顺序插入
      "insert into %s %s values (%s) ".format(
        schemas(clazz.getSimpleName),
        list.map(_.getName).toString.substring(4),
        "?," * (list.length - 1) + "?"
      ),
      new CkSinkBuilder[O],
      new JdbcExecutionOptions.Builder().withBatchSize(batchSize).build(),
      new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
        .withUrl(CkJdbcUrl)
        .build()
    )
  }
```



自定义 `JdbcStatementBuilder`, `O` 是样例类类型

```scala
// 坑点: ps.setXXX 是从 1 开始计数的
class CkSinkBuilder[O] extends JdbcStatementBuilder[O] {
  def accept(ps: PreparedStatement, v: O): Unit = {
    val list = v.getClass.getDeclaredFields.toList
    for (i <- 1 to list.length) {
      list(i - 1).setAccessible(true)
      val maybeValue = list(i - 1).get(v)
      maybeValue match {
        case someString: Some[Any] =>
          someString match {
            case Some(value) =>
              value match {
                // 本次实验就这些类型
                case d: Int =>
                  ps.setInt(i, d)
                case d: Double =>
                  ps.setDouble(i, d)
                case d: String =>
                  ps.setString(i, d)
                case _ =>
              }
            case _ =>
          }
        case _ =>
          // missing fields
          ps.setNull(i, Types.NULL)
      }
    }
  }
}
```

### 运行截图

![](https://i0.hdslb.com/bfs/album/bd52332ece691c7108fcddbc8ec6d1cc1b9e753e.png)

![](https://i0.hdslb.com/bfs/album/8965d79716031c5d61cfbfba42d8b167272eb943.png)![](https://i0.hdslb.com/bfs/album/81a6c6fa3663ba10a95ef80aa2610dd5feee8e34.png)