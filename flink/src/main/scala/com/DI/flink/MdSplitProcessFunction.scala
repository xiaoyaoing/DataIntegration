package com.DI.flink

import com.DI.flink.OutPutTags.{contractStream, djkStream, dsfStream, duebillStream, etcStream, grwyStream, gzdfStream, huanbStream, huanxStream, saStream, sbybStream, sdrqStream, shopStream, sjyhStream}
import com.DI.flink.Main.logger
import com.DI.flink.serializer.{StringToDouble, StringToInt}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
import org.json4s.{DefaultFormats, Formats, JNothing, JValue}

/**
 * @author Rikka
 * @date 2022-04-26 01:18:23
 * @description 将解析后的 Json 对象分发到 sideout
 */


class MdSplitProcessFunction extends ProcessFunction[JValue, JValue] {
  var count = 0
  override def processElement(i: JValue, context: ProcessFunction[JValue, JValue]#Context, collector: Collector[JValue]): Unit = {
    logger.info("\n"+"="*100+"\n正在处理第 %d 条\n".format(count)+"="*100)
    count+=1
    implicit val formats: Formats = DefaultFormats + StringToInt + StringToDouble
    if(i.equals(JNothing))
      return
    (i \ "eventType").extractOpt[String] match {
      case Some(value) => value match {
        case "shop" => context.output(shopStream, i)
        case "contract" => context.output(contractStream, i)
        case "djk" => context.output(djkStream, i)
        case "dsf" => context.output(dsfStream, i)
        case "duebill" => context.output(duebillStream, i)
        case "etc" => context.output(etcStream, i)
        case "grwy" => context.output(grwyStream, i)
        case "gzdf" => context.output(gzdfStream, i)
        case "huanb" => context.output(huanbStream, i)
        case "huanx" => context.output(huanxStream, i)
        case "sa" => context.output(saStream, i)
        case "sbyb" => context.output(sbybStream, i)
        case "sdrq" => context.output(sdrqStream, i)
        case "sjyh" => context.output(sjyhStream, i)
      }
    }
  }

}