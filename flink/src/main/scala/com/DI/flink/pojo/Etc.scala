package com.DI.flink.pojo

/**
 * @author Rikka
 * @date 2022-04-26 23:16:04
 * @description
 */
case class Etc(uid: Option[String],
               etc_acct: Option[String],
               card_no: Option[String],
               car_no: Option[String],
               cust_name: Option[String],
               tran_date: Option[String],
               tran_time: Option[String],
               tran_amt_fen: Option[Double],
               real_amt: Option[Double],
               conces_amt: Option[Double],
               tran_place: Option[String],
               mob_phone: Option[String],
               etl_dt: Option[String])
