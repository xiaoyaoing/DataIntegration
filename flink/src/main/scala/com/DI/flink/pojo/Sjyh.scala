package com.DI.flink.pojo

/**
 * @author Rikka
 * @date 2022-04-26 23:17:49
 * @description
 */
case class Sjyh(uid: Option[String],
                mch_channel: Option[String],
                login_type: Option[String],
                ebank_cust_no: Option[String],
                tran_date: Option[String],
                tran_time: Option[String],
                tran_code: Option[String],
                tran_sts: Option[String],
                return_code: Option[String],
                return_msg: Option[String],
                sys_type: Option[String],
                payer_acct_no: Option[String],
                payer_acct_name: Option[String],
                payee_acct_no: Option[String],
                payee_acct_name: Option[String],
                tran_amt: Option[Double],
                etl_dt: Option[String])
