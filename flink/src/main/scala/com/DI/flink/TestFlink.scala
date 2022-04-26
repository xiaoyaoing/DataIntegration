package com.DI.flink


import com.DI.flink.OutPutTags._
import com.DI.flink.pojo.{Contract, Djk, Dsf, Duebill, Etc, Grwy, Gzdf, Huanb, Huanx, Sa, Sbyb, Sdrq, Shop, Sjyh}
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcExecutionOptions, JdbcSink, JdbcStatementBuilder}
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.json4s.JValue
import org.json4s.jackson.JsonMethods

import java.sql.{PreparedStatement, Types}
import java.util
import java.util.Properties
import scala.reflect.{ClassTag, classTag}

/**
 * @author Rikka
 * @date 2022-04-22 17:32:00
 * @description 写得很罗嗦, addSink 相关的应该可以有办法复用, 但我不知道. 大概可以不做转换成 case class, 全程用 JValue 做
 */

class CkSinkBuilder[O] extends JdbcStatementBuilder[O] {
  def accept(ps: PreparedStatement, v: O): Unit = {
    val list = v.getClass.getDeclaredFields.toList
    for (i <- 1 to list.length) {
      list(i - 1).setAccessible(true)
      val maybeValue =  list(i - 1).get(v)
      maybeValue match {
        case someString: Some[Any] =>
          someString match {
            case Some(value) =>
              value match {
                case d: Int =>
                  ps.setInt(i, d)
                case d: Double =>
                  ps.setDouble(i, d)
                case d: String =>
                  ps.setString(i, d)
                case _ =>
              }
            case _=>
          }
        case _ =>
          ps.setNull(i,Types.NULL)
      }
    }
  }
}

object TestFlink {


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
    val props = new Properties()
    //    props.put("bootstrap.servers", "kafka:9092")
    props.put("bootstrap.servers", "127.0.0.1:9094")
    props.put("group.id", "test_flink")
    val source: KafkaSource[String] = KafkaSource.builder[String].setBootstrapServers(props.getProperty("bootstrap.servers")).setTopics("test").setGroupId("my-group").setStartingOffsets(OffsetsInitializer.latest()).setValueOnlyDeserializer(new SimpleStringSchema).build
    val value: DataStream[String] = env.fromSource(source, WatermarkStrategy.noWatermarks[String], "Kafka Source")

    //    implicit val formats: Formats = new Formats {
    //      override val dateFormat = new MyDateFormat
    //    }

    val CkJdbcUrl = "jdbc:clickhouse://127.0.0.1:18123/dm"


    val value1 = value.map(s => JsonMethods.parseOpt(s) match {
      case Some(value) => value
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

