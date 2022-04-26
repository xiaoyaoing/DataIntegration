package com.DI.flink.serializer

import org.json4s.{CustomSerializer, JInt, JString}

/**
 * @author Rikka
 * @date 2022-04-26 23:13:17
 * @description
 */
object StringToInt extends CustomSerializer[Int](format => ( {
  case JString(x) => x.toInt
}, {
  case x: Int => JInt(x)
}))
