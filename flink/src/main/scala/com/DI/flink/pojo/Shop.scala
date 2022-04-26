package com.DI.flink.pojo

/**
 * @author Rikka
 * @date 2022-04-26 23:14:20
 * @description
 */
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
