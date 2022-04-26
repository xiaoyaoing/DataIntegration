package com.DI.flink


import com.DI.flink.OutPutTags._
import com.DI.flink.pojo._
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcExecutionOptions, JdbcSink}
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.kafka.clients.consumer.{ConsumerConfig, OffsetResetStrategy}
import org.json4s.{JNothing, JValue}
import org.json4s.jackson.JsonMethods
import org.slf4j.{Logger, LoggerFactory}

import java.util
import java.util.{Locale, Properties}
import scala.reflect.{ClassTag, classTag}


/**
 * @author Rikka
 * @date 2022-04-22 17:32:00
 * @description 写得很罗嗦, addSink 相关的应该可以有办法复用, 但我不知道. 大概可以不做转换成 case class, 全程用 JValue 做
 */
object Main {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  var cases = List(shopStream, contractStream, djkStream, dsfStream, duebillStream, etcStream, grwyStream, gzdfStream, huanbStream, huanxStream, saStream, sbybStream, sdrqStream, sjyhStream)
  val schemas = Map("Shop" -> "v_tr_shop_mx", "Contract" -> "dm_v_tr_contract_mx", "Djk" -> "dm_v_tr_djk_mx", "Dsf" -> "dm_v_tr_dsf_mx",
    "Duebill" -> "dm_v_tr_duebill_mx", "Etc" -> "dm_v_tr_etc_mx", "Grwy" -> "dm_v_tr_grwy_mx", "Gzdf" -> "dm_v_tr_gzdf_mx",
    "Huanb" -> "dm_v_tr_huanb_mx", "Huanx" -> "dm_v_tr_huanx_mx", "Sa" -> "dm_v_tr_sa_mx", "Sbyb" -> "dm_v_tr_sbyb_mx", "Sdrq" -> "dm_v_tr_sdrq_mx",
    "Sjyh" -> "dm_v_tr_sjyh_mx"
  )

  def getSink[O](CkJdbcUrl: String, batchSize: Int)(implicit c: ClassTag[O]): SinkFunction[O] = {
    val clazz = classTag[O].runtimeClass
    val list = clazz.getDeclaredFields.toList
    JdbcSink.sink[O](
      // 我没有找到能按照字段插入的方法
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

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment



    // 开启 Checkpoint，每 1000毫秒进行一次 Checkpoint
    env.enableCheckpointing(1000)

    // Checkpoint 语义设置为 EXACTLY_ONCE
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    // CheckPoint 的超时时间
    env.getCheckpointConfig.setCheckpointTimeout(60000)

    // 同一时间，只允许 有 1 个 Checkpoint 在发生
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)

    // 两次 Checkpoint 之间的最小时间间隔为 500 毫秒
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    // 作业最多允许 Checkpoint 失败 1 次（flink 1.9 开始支持）
    env.getCheckpointConfig.setTolerableCheckpointFailureNumber(1)

    val props = new Properties()
    props.put("bootstrap.servers", "kafka:9092") // 提交至 flink
    //    props.put("bootstrap.servers", "127.0.0.1:9094") // IDEA 里面跑, 其他部署在 docker 容器
    props.put("auto.offset.reset", "earliest")
    props.put("group.id", "test_flink")
    val source: KafkaSource[String] = KafkaSource.builder[String].setBootstrapServers(props.getProperty("bootstrap.servers")).setTopics("test").setGroupId("my-group").setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.valueOf(
      props.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)
        .asInstanceOf[String]
        .toUpperCase(Locale.ROOT)
    ))).setValueOnlyDeserializer(new SimpleStringSchema).build

    val value: DataStream[String] = env.fromSource(source, WatermarkStrategy.noWatermarks[String], "Kafka Source")
//    val CkJdbcUrl = "jdbc:clickhouse://127.0.0.1:18123/dm" // IDEA 里跑, 其他部署在 docker
        val CkJdbcUrl = "jdbc:clickhouse://clickhouse:8123/dm" // 提交至 flink


    val value1 = value.map(s => JsonMethods.parseOpt(s) match {
      case Some(value) => value
      case _ => JNothing
    }).process(new MdSplitProcessFunction)


    val sideoutList = new util.ArrayList[DataStream[JValue]]

    cases.foreach(s => {
      sideoutList.add(value1.getSideOutput(s))
    })
    // case class 我不知道怎么通过类名反射, 也不知道怎么能把他存表, 只能全写出来了
    sideoutList.get(0).map(new MapFuncionImp[Shop]).addSink(getSink[Shop](CkJdbcUrl, 500))
    sideoutList.get(1).map(new MapFuncionImp[Contract]).addSink(getSink[Contract](CkJdbcUrl, 500))
    sideoutList.get(2).map(new MapFuncionImp[Djk]).addSink(getSink[Djk](CkJdbcUrl, 500))
    sideoutList.get(3).map(new MapFuncionImp[Dsf]).addSink(getSink[Dsf](CkJdbcUrl, 500))
    sideoutList.get(4).map(new MapFuncionImp[Duebill]).addSink(getSink[Duebill](CkJdbcUrl, 500))
    sideoutList.get(5).map(new MapFuncionImp[Etc]).addSink(getSink[Etc](CkJdbcUrl, 500))
    sideoutList.get(6).map(new MapFuncionImp[Grwy]).addSink(getSink[Grwy](CkJdbcUrl, 500))
    sideoutList.get(7).map(new MapFuncionImp[Gzdf]).addSink(getSink[Gzdf](CkJdbcUrl, 500))
    sideoutList.get(8).map(new MapFuncionImp[Huanb]).addSink(getSink[Huanb](CkJdbcUrl, 500))
    sideoutList.get(9).map(new MapFuncionImp[Huanx]).addSink(getSink[Huanx](CkJdbcUrl, 500))
    sideoutList.get(10).map(new MapFuncionImp[Sa]).addSink(getSink[Sa](CkJdbcUrl, 500))
    sideoutList.get(11).map(new MapFuncionImp[Sbyb]).addSink(getSink[Sbyb](CkJdbcUrl, 500))
    sideoutList.get(12).map(new MapFuncionImp[Sdrq]).addSink(getSink[Sdrq](CkJdbcUrl, 500))
    sideoutList.get(13).map(new MapFuncionImp[Sjyh]).addSink(getSink[Sjyh](CkJdbcUrl, 500))

    env.execute("Flink Streaming Java API Skeleton")
  }
}

