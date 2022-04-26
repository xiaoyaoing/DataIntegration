package com.DI.flink

import com.DI.flink.serializer.{StringToDouble, StringToInt}
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.json4s.{DefaultFormats, Formats, JValue}

/**
 * @author Rikka
 * @date 2022-04-26 19:41:38
 * @description
 */
class MapFuncionImp[T](implicit mf: Manifest[T], ti: TypeInformation[T]) extends MapFunction[JValue,T] {
  override def map(t: JValue): T = {
    implicit val formats: Formats = DefaultFormats + StringToInt + StringToDouble
    (t \ "eventBody").extractOpt[T] match {
      case Some(value) => value
    }
  }
}
