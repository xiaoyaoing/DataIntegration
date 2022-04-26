package com.DI.flink

import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.streaming.api.scala._
import org.json4s.JValue
/**
 * @author Rikka
 * @date 2022-04-26 01:35:00
 * @description
 */
object OutPutTags {
  lazy val shopStream = new OutputTag[JValue]("shop")
  lazy val contractStream = new OutputTag[JValue]("contract")
  lazy val dsfStream = new OutputTag[JValue]("dsf")
  lazy val sjyhStream = new OutputTag[JValue]("sjyh")
  lazy val duebillStream = new OutputTag[JValue]("duebill")
  lazy val etcStream = new OutputTag[JValue]("etc")
  lazy val grwyStream = new OutputTag[JValue]("grwy")
  lazy val gzdfStream = new OutputTag[JValue]("gzdf")
  lazy val huanbStream = new OutputTag[JValue]("huanb")
  lazy val huanxStream = new OutputTag[JValue]("huanx")
  lazy val saStream = new OutputTag[JValue]("sa")
  lazy val sbybStream = new OutputTag[JValue]("sbyb")
  lazy val sdrqStream = new OutputTag[JValue]("sdrq")
  lazy val djkStream = new OutputTag[JValue]("djk")
}











