package com.DI.flink.pojo

/**
 * @author Rikka
 * @date 2022-04-26 23:16:54
 * @description
 */
case class Huanb(tran_flag: Option[String],
                 uid: Option[String],
                 cust_name: Option[String],
                 acct_no: Option[String],
                 tran_date: Option[String],
                 tran_time: Option[String],
                 tran_amt: Option[Double],
                 bal: Option[Double],
                 tran_code: Option[String],
                 dr_cr_code: Option[String],
                 pay_term: Option[Int],
                 tran_teller_no: Option[String],
                 pprd_rfn_amt: Option[Double],
                 pprd_amotz_intr: Option[Double],
                 tran_log_no: Option[String],
                 tran_type: Option[String],
                 dscrp_code: Option[String],
                 remark: Option[String],
                 etl_dt: Option[String])
