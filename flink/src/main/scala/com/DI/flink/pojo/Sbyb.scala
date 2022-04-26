package com.DI.flink.pojo

/**
 * @author Rikka
 * @date 2022-04-26 23:17:25
 * @description
 */
case class Sbyb(uid: Option[String],
                cust_name: Option[String],
                tran_date: Option[String],
                tran_sts: Option[String],
                tran_org: Option[String],
                tran_teller_no: Option[String],
                tran_amt_fen: Option[Double],
                tran_type: Option[String],
                return_msg: Option[String],
                etl_dt: Option[String])
