package com.DI.flink.pojo

/**
 * @author Rikka
 * @date 2022-04-26 23:17:15
 * @description
 */
case class Sa(uid: Option[String],
              card_no: Option[String],
              cust_name: Option[String],
              acct_no: Option[String],
              det_n: Option[Int],
              curr_type: Option[String],
              tran_teller_no: Option[String],
              cr_amt: Option[Double],
              bal: Option[Double],
              tran_amt: Option[Double],
              tran_card_no: Option[String],
              tran_type: Option[String],
              tran_log_no: Option[String],
              dr_amt: Option[Double],
              open_org: Option[String],
              dscrp_code: Option[String],
              remark: Option[String],
              tran_time: Option[String],
              tran_date: Option[String],
              sys_date: Option[String],
              tran_code: Option[String],
              remark_1: Option[String],
              oppo_cust_name: Option[String],
              agt_cert_type: Option[String],
              agt_cert_no: Option[String],
              agt_cust_name: Option[String],
              channel_flag: Option[String],
              oppo_acct_no: Option[String],
              oppo_bank_no: Option[String],
              src_dt: Option[String],
              etl_dt: Option[String])
