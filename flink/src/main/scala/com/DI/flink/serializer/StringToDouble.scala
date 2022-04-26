package com.DI.flink.serializer

import org.json4s.{CustomSerializer, JDouble, JString}

/**
 * @author Rikka
 * @date 2022-04-26 00:06:32
 * @description
 */

object StringToDouble extends CustomSerializer[Double](format => ( {
  case JString(x) => x.toDouble
}, {
  case x: Double => JDouble(x)
}))
