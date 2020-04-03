package bzn.dm.bclickthrough

import java.text.SimpleDateFormat
import java.util.Date

import bzn.dm.util.SparkUtil
import bzn.job.common.{ClickHouseUntil, MysqlUntil, Until}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext

/**
  * author:xiaoYuanRen
  * Date:2019/10/23
  * Time:16:51
  * describe: 每天新增的数据
  **/
object DmPolicyStreamingDetailTest extends SparkUtil with Until with ClickHouseUntil{
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName,"local[*]")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    val res = getHolderInfo(hiveContext)

    val tableName = "emp_continue_policy_all_info_detail"
    val tableName1 = "emp_risk_monitor_kri_detail"
    val url = "clickhouse.url.odsdb.test"
    val urlOfficial = "clickhouse.url"
    val user = "clickhouse.username"
    val possWord = "clickhouse.password"

    val sql = "ALTER TABLE odsdb_test.emp_continue_policy_all_info_detail DROP PARTITION '2019-12-14'"
//    exeSql(sql,url:String,user:String,possWord:String)

    sc.stop()
  }



  /**
    * 获取增量的续投数据和全量的历史数据
    * @param sqlContext 上下文
    */
  def getHolderInfo(sqlContext:HiveContext) = {
    sqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    sqlContext.udf.register("clean", (str: String) => clean(str))
    sqlContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")//设置日期格式
      val date = df.format(new Date())// new Date()为获取当前系统时间
      date + ""
    })

    import sqlContext.implicits._

    /**
      * 读取每天新增的数据
      */
    val dwPolicyStreamingDetail =
      sqlContext.sql("select * from dwdb.dw_policy_streaming_detail")
        .selectExpr(
          "proposal_no",
          "policy_code",
          "policy_no",
          "ent_id",
          "ent_name",
          "channel_id",
          "channel_name",
          "status",
          "big_policy",
          "proposal_time_preserve",//批单投保时间
          "proposal_time_policy",//保单投保时间
          "policy_start_date",//保单起期
          "policy_end_date",//投保止期
          "preserve_start_date",
          "preserve_end_date",
          "0 as refer_insured_count",
          "0 as now_insured_count",
          "insured_count as match_insured_count",
          "insured_company",//被保人企业
          "insurance_name as insure_company_name",
          "insurance_company_short_name",
          "sku_charge_type",
          "inc_dec_order_no",
          "sales_name as sale_name",
          "biz_operator"
        )

    /**
      * 读取雇主基础数据
      */
//    sqlContext.sql("select policy_code,policy_no,policy_id,holder_name,7 as status,ent_id,ent_name,channel_id,channel_name,insure_company_short_name," +
//      "big_policy,sale_name,biz_operator,proposal_time, policy_start_date,policy_end_date,insure_company_name,sku_charge_type, " +
//      "insured_subject,regexp_replace(substr(cast(now() as string),1,10),'-','') as now_day_id," +
//      " regexp_replace(date_add(last_day(now()),1),'-','') as next_month_day_id  from dwdb.dw_employer_baseinfo_detail where product_code not in ('17000001','LGB000001')")
    val dwEmployerBaseinfoDetail =
    if(getNowTime.substring (8, 10).toInt < 5){
      sqlContext.sql("select policy_code,policy_no,policy_id,holder_name,7 as status,ent_id,ent_name,channel_id,channel_name,insure_company_short_name," +
        "big_policy,sale_name,biz_operator,proposal_time, policy_start_date,policy_end_date,insure_company_name,sku_charge_type, " +
        "insured_subject,regexp_replace(last_day(add_months(now(),-1)),'-','') as refer_day_id, " +
        " regexp_replace(cast(current_date() as string),'-','') as now_day_id, " +
        " regexp_replace(date_format(current_date(),'yyyy-MM-05'),'-','') as match_day_id  from dwdb.dw_employer_baseinfo_detail where product_code not in ('17000001','LGB000001')")
    } else if(getNowTime.substring (8, 10).toInt == 5) {
      sqlContext.sql("select policy_code,policy_no,policy_id,holder_name,7 as status,ent_id,ent_name,channel_id,channel_name,insure_company_short_name," +
        "big_policy,sale_name,biz_operator,proposal_time, policy_start_date,policy_end_date,insure_company_name,sku_charge_type, " +
        "insured_subject,regexp_replace(last_day(add_months(now(),-1)),'-','') as refer_day_id," +
        " regexp_replace(date_format(current_date(),'yyyy-MM-05'),'-','') as match_day_id ," +
        " regexp_replace(cast(current_date() as string),'-','') as now_day_id, " +
        "from dwdb.dw_employer_baseinfo_detail where product_code not in ('17000001','LGB000001')")
    }else{
      sqlContext.sql("select policy_code,policy_no,policy_id,holder_name,7 as status,ent_id,ent_name,channel_id,channel_name,insure_company_short_name," +
        "big_policy,sale_name,biz_operator,proposal_time, policy_start_date,policy_end_date,insure_company_name,sku_charge_type, " +
        " regexp_replace(cast(current_date() as string),'-','') as refer_day_id, " +
        " regexp_replace(cast(current_date() as string),'-','') as now_day_id,  " +
        " regexp_replace(date_format(date_add(last_day(current_date()),1),'yyyy-MM-05'),'-','') as match_day_id  from dwdb.dw_employer_baseinfo_detail where product_code not in ('17000001','LGB000001')")
    }


    /**
      * 读取当前在保人表
      * [6-月底]：下月5号在保人数+新投+新批单/当前在保人数
      * [1-5]：本月5号在保人数+新投+新批单/上月月底在保人数
      */
//    sqlContext.sql("select policy_id as policy_id_insued,day_id,count as insured_count from dwdb.dw_policy_curr_insured_detail")
//      .where("regexp_replace(substr(cast(now() as string),1,10),'-','') = day_id or regexp_replace(date_add(last_day(now()),1),'-','') = day_id")
    val dwPolicyCurrInsuredDetail =
    if(getNowTime.substring (8, 10).toInt < 5){
      sqlContext.sql("select policy_id as policy_id_insured,day_id,count as insured_count from dwdb.dw_policy_curr_insured_detail")
        .where("regexp_replace(date_format(current_date(),'yyyy-MM-05'),'-','') = day_id or" +
          " regexp_replace(cast(current_date() as string),'-','') = day_id or regexp_replace(last_day(add_months(now(),-1)),'-','') = day_id")
//      sqlContext.sql(
//        """
//          |select regexp_replace(date_format(current_date(),'yyyy-MM-05'),'-','') as curr_month_5,regexp_replace(last_day(add_months(now(),-1)),'-','') as last_month_end
//          |from dwdb.dw_policy_streaming_detail
//        """.stripMargin)
    } else if(getNowTime.substring (8, 10).toInt == 5){
      sqlContext.sql("select policy_id as policy_id_insured,day_id,count as insured_count from dwdb.dw_policy_curr_insured_detail")
        .where("regexp_replace(date_format(current_date(),'yyyy-MM-05'),'-','') = day_id or regexp_replace(last_day(add_months(now(),-1)),'-','') = day_id")
    } else{
      sqlContext.sql("select policy_id as policy_id_insured,day_id,count as insured_count from dwdb.dw_policy_curr_insured_detail")
        .where("regexp_replace(date_format(date_add(last_day(current_date()),1),'yyyy-MM-05'),'-','') = day_id or regexp_replace(cast(current_date() as string),'-','') = day_id")
//      sqlContext.sql(
//        """
//          |select regexp_replace(date_format(date_add(last_day(current_date()),1),'yyyy-MM-05'),'-','') as next_month_5,regexp_replace(cast(current_date() as string),'-','') as curr_day
//          |from dwdb.dw_policy_streaming_detail
//        """.stripMargin)
    }


    val referDataRes = dwEmployerBaseinfoDetail.join(dwPolicyCurrInsuredDetail,'policy_id === 'policy_id_insured and 'refer_day_id === 'day_id,"leftouter")
      .selectExpr(
        "policy_id",
        "policy_no",
        "policy_code",
        "ent_id",
        "holder_name",
        "channel_id",
        "channel_name",
        "big_policy",
        "proposal_time",
        "policy_start_date",
        "policy_end_date",
        "insure_company_name",
        "insure_company_short_name as insurance_company_short_name",
        "sku_charge_type",
        "insured_subject",
        "status",
        "sale_name",
        "biz_operator",
        "insured_count as refer_insured_count",
        "now_day_id",
        "refer_day_id",
        "match_day_id"
      )

    val matchData = referDataRes.join(dwPolicyCurrInsuredDetail,'policy_id === 'policy_id_insured and 'match_day_id === 'day_id,"leftouter")
      .selectExpr(
        "policy_id",
        "'' as proposal_no",
        "policy_code",
        "policy_no",
        "ent_id",
        "holder_name as ent_name",
        "channel_id",
        "channel_name",
        "status",
        "big_policy",
        "cast('' as timestamp) as proposal_time_preserve",//批单投保时间
        "proposal_time as proposal_time_policy",//保单投保时间
        "policy_start_date",//保单起期
        "policy_end_date",//投保止期
        "cast('' as timestamp) as preserve_start_date",
        "cast('' as timestamp) as preserve_end_date",
        "refer_insured_count",
        "insured_count as match_insured_count",
        "insured_subject as insured_company",//被保人企业
        "insure_company_name",
        "insurance_company_short_name",
        "sku_charge_type",
        "'' as inc_dec_order_no",
        "sale_name",
        "biz_operator",
        "now_day_id"
      )

    val nowDataRes = matchData.join(dwPolicyCurrInsuredDetail,'policy_id === 'policy_id_insured and 'now_day_id === 'day_id,"leftouter")
      .selectExpr(
          "proposal_no",
          "policy_code",
          "policy_no",
          "ent_id",
          "ent_name",
          "channel_id",
          "channel_name",
          "status",
          "big_policy",
          "proposal_time_preserve",//批单投保时间
          "proposal_time_policy",//保单投保时间
          "policy_start_date",//保单起期
          "policy_end_date",//投保止期
          "preserve_start_date",
          "preserve_end_date",
          "refer_insured_count",
          "match_insured_count",
          "insured_count as now_insured_count",
          "insured_company",//被保人企业
          "insure_company_name",
          "insurance_company_short_name",
          "sku_charge_type",
          "inc_dec_order_no",
          "sale_name",
          "biz_operator"
      )

    val res = dwPolicyStreamingDetail.unionAll(nowDataRes)
      .selectExpr(
        "getUUID() as id",
        "clean(proposal_no) as proposal_no",
        "policy_code",
        "policy_no",
        "ent_id",
        "clean(ent_name) as ent_name",
        "channel_id",
        "channel_name",
        "status",
        "big_policy",
        "proposal_time_preserve",//批单投保时间
        "proposal_time_policy",//保单投保时间
        "policy_start_date",//保单起期
        "policy_end_date",//投保止期
        "preserve_start_date",
        "preserve_end_date",
        "refer_insured_count",
        "now_insured_count as curr_insured",
        "match_insured_count as pre_continue_person_count",
        "clean(insured_company) as insured_company",//被保人企业
        "clean(insure_company_name) as insure_company_name",
        "clean(insurance_company_short_name) as insurance_company_short_name",
        "sku_charge_type",
        "date_format(getNow(),'yyyy-MM-dd HH:mm:dd') as update_data_time",
        "clean(inc_dec_order_no) as inc_dec_order_no",
        "clean(sale_name) as sale_name",
        "clean(biz_operator) as biz_operator",
        "date_format(now(),'yyyy-MM-dd') as day_id",
        "date_format('2080-01-01','yyyy-MM-dd') as date_test",
        "date_format(now(),'yyyy-MM-dd HH:mm:ss') as create_time",
        "date_format(now(),'yyyy-MM-dd HH:mm:ss') as update_time"
      )

    res.printSchema()

  }
}
