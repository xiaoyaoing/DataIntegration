package com.DI.flink.pojo

/**
 * @author Rikka
 * @date 2022-04-26 23:15:18
 * @description
 */
case class Djk(uid: Option[String],
               card_no: Option[String],
               tran_type: Option[String],
               tran_type_desc: Option[String],
               tran_amt: Option[Double],
               tran_amt_sign: Option[String],
               mer_type: Option[String],
               mer_code: Option[String],
               rev_ind: Option[String],
               tran_desc: Option[String],
               tran_date: Option[String],
               val_date: Option[String],
               pur_date: Option[String],
               tran_time: Option[String],
               acct_no: Option[String],
               etl_dt: Option[String])
