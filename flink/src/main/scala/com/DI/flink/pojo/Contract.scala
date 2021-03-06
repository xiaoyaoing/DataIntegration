package com.DI.flink.pojo

/**
 * @author Rikka
 * @date 2022-04-26 23:14:56
 * @description
 */
case class Contract(uid: Option[String],
                    contract_no: Option[String],
                    apply_no: Option[String],
                    artificial_no: Option[String],
                    occur_date: Option[String],
                    loan_cust_no: Option[String],
                    cust_name: Option[String],
                    buss_type: Option[String],
                    occur_type: Option[String],
                    is_credit_cyc: Option[String],
                    curr_type: Option[String],
                    buss_amt: Option[Double],
                    loan_pert: Option[Double],
                    term_year: Option[Int],
                    term_mth: Option[Int],
                    term_day: Option[Int],
                    base_rate_type: Option[String],
                    base_rate: Option[Double],
                    float_type: Option[String],
                    rate_float: Option[Double],
                    rate: Option[Double],
                    pay_times: Option[Int],
                    pay_type: Option[String],
                    direction: Option[String],
                    loan_use: Option[String],
                    pay_source: Option[String],
                    putout_date: Option[String],
                    matu_date: Option[String],
                    vouch_type: Option[String],
                    is_oth_vouch: Option[String],
                    apply_type: Option[String],
                    extend_times: Option[Int],
                    actu_out_amt: Option[Double],
                    bal: Option[Double],
                    norm_bal: Option[Double],
                    dlay_bal: Option[Double],
                    dull_bal: Option[Double],
                    owed_int_in: Option[Double],
                    owed_int_out: Option[Double],
                    fine_pr_int: Option[Double],
                    fine_intr_int: Option[Double],
                    dlay_days: Option[Int],
                    five_class: Option[String],
                    class_date: Option[String],
                    mge_org: Option[String],
                    mgr_no: Option[String],
                    operate_org: Option[String],
                    operator: Option[String],
                    operate_date: Option[String],
                    reg_org: Option[String],
                    register: Option[String],
                    reg_date: Option[String],
                    inte_settle_type: Option[String],
                    is_bad: Option[String],
                    frz_amt: Option[Double],
                    con_crl_type: Option[String],
                    shift_type: Option[String],
                    due_intr_days: Option[Int],
                    reson_type: Option[String],
                    shift_bal: Option[Double],
                    is_vc_vouch: Option[String],
                    loan_use_add: Option[String],
                    finsh_type: Option[String],
                    finsh_date: Option[String],
                    sts_flag: Option[String],
                    src_dt: Option[String],
                    etl_dt: Option[String])
