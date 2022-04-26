package com.DI.flink.pojo

/**
 * @author Rikka
 * @date 2022-04-26 23:17:38
 * @description
 */
case class Sdrq(hosehld_no: Option[String],
                acct_no: Option[String],
                cust_name: Option[String],
                tran_type: Option[String],
                tran_date: Option[String],
                tran_amt_fen: Option[Double],
                channel_flg: Option[String],
                tran_org: Option[String],
                tran_teller_no: Option[String],
                tran_log_no: Option[String],
                batch_no: Option[String],
                tran_sts: Option[String],
                return_msg: Option[String],
                etl_dt: Option[String],
                uid: Option[String])
