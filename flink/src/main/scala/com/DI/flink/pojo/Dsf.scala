package com.DI.flink.pojo

/**
 * @author Rikka
 * @date 2022-04-26 23:15:34
 * @description
 */
case class Dsf(tran_date: Option[String],
               tran_log_no: Option[String],
               tran_code: Option[String],
               channel_flg: Option[String],
               tran_org: Option[String],
               tran_teller_no: Option[String],
               dc_flag: Option[String],
               tran_amt: Option[Double],
               send_bank: Option[String],
               payer_open_bank: Option[String],
               payer_acct_no: Option[String],
               payer_name: Option[String],
               payee_open_bank: Option[String],
               payee_acct_no: Option[String],
               payee_name: Option[String],
               tran_sts: Option[String],
               busi_type: Option[String],
               busi_sub_type: Option[String],
               etl_dt: Option[String],
               uid: Option[String])
