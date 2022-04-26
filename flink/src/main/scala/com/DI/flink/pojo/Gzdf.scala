package com.DI.flink.pojo

/**
 * @author Rikka
 * @date 2022-04-26 23:16:40
 * @description
 */
case class Gzdf(belong_org: Option[String],
                ent_acct: Option[String],
                ent_name: Option[String],
                eng_cert_no: Option[String],
                acct_no: Option[String],
                cust_name: Option[String],
                uid: Option[String],
                tran_date: Option[String],
                tran_amt: Option[Double],
                tran_log_no: Option[String],
                is_secu_card: Option[String],
                trna_channel: Option[String],
                batch_no: Option[String],
                etl_dt: Option[String])
