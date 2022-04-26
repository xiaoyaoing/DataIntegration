package com.DI.flink.serializer

import org.json4s.{CustomSerializer, JDouble, JString}

/**
 * @author Rikka
 * @date 2022-04-26 00:06:32
 * @description 提供的 json 即使是数字也加了双引号, 所以需要手动实现一个
 */

object StringToDouble extends CustomSerializer[Double](format => ( {
  case JString(x) => x.toDouble
}, {
  case x: Double => JDouble(x)
}))
