package bzn.dm.bclickthrough

import bzn.dm.util.SparkUtil
import bzn.job.common.{MysqlUntil, Until}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext

/**
  * author:xiaoYuanRen
  * Date:2019/10/23
  * Time:16:51
  * describe: 每天新增的数据
  **/
object DmPolicyStreamingDetailTest extends SparkUtil with Until with MysqlUntil{
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName,"local[*]")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    val res = getHolderInfo(hiveContext)
   // hiveContext.sql("truncate table dwdb.dw_policy_streaming_detail")
   // res.repartition(1).write.mode(SaveMode.Append).saveAsTable("dwdb.dw_policy_streaming_detail")

    sc.stop()
  }

  def getHolderInfo(sqlContext:HiveContext) = {
    sqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    sqlContext.udf.register("clean", (str: String) => clean(str))
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
        "0 as now_insured_count",
        "insured_count as next_month_insured_count",
        "insured_company",//被保人企业
        "insurance_name as insure_company_name",
        "sku_charge_type",
        "update_data_time",
        "inc_dec_order_no",
        "sales_name",
        "biz_operator"
      )

    /**
      * 读取雇主基础数据
      */
    val dwEmployerBaseinfoDetail =
      sqlContext.sql("select policy_code,policy_no,policy_id,holder_name,7 as status,ent_id,ent_name,channel_id,channel_name," +
        "big_policy,sale_name,biz_operator,proposal_time, policy_start_date,policy_end_date,insure_company_name,sku_charge_type, " +
        "insured_subject,regexp_replace(substr(cast(now() as string),1,10),'-','') as now_day_id," +
        " regexp_replace(date_add(last_day(now()),1),'-','') as next_month_day_id  from dwdb.dw_employer_baseinfo_detail")

    /**
      * 读取当前在保人表
      */
    val dwPolicyCurrInsuredDetail = sqlContext.sql("select policy_id as policy_id_insued,day_id,count as insured_count from dwdb.dw_policy_curr_insured_detail")
      .where("regexp_replace(substr(cast(now() as string),1,10),'-','') = day_id or regexp_replace(date_add(last_day(now()),1),'-','') = day_id")

    val nowDataRes = dwEmployerBaseinfoDetail.join(dwPolicyCurrInsuredDetail,'policy_id === 'policy_id_insued and 'now_day_id === 'day_id,"leftouter")
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
        "sku_charge_type",
        "insured_subject",
        "status",
        "sale_name",
        "biz_operator",
        "insured_count as now_insured_count",
        "now_day_id",
        "next_month_day_id"
      )

    val nextMonthData = nowDataRes.join(dwPolicyCurrInsuredDetail,'policy_id === 'policy_id_insued and 'next_month_day_id === 'day_id,"leftouter")
      .selectExpr(
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
        "now_insured_count",
        "insured_count as next_month_insured_count",
        "insured_subject as insured_company",//被保人企业
        "insure_company_name",
        "sku_charge_type",
        "now() as update_data_time",
        "'' as inc_dec_order_no",
        "sale_name as sales_name",
        "biz_operator"
      )

    val res = dwPolicyStreamingDetail.unionAll(nextMonthData)
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
        "now_insured_count as curr_insured",
        "next_month_insured_count as pre_continue_person_count",
        "clean(insured_company) as insured_company",//被保人企业
        "clean(insure_company_name) as insure_company_name",
        "sku_charge_type",
        "date_format(update_data_time,'yyyy-MM-dd HH:mm:dd') as update_data_time",
        "clean(inc_dec_order_no) as inc_dec_order_no",
        "clean(sales_name) as sales_name",
        "clean(biz_operator) as biz_operator",
        "date_format(now(),'yyyy-MM-dd HH:mm:ss') as create_time",
        "date_format(now(),'yyyy-MM-dd HH:mm:ss') as update_time"
      )

    res.printSchema()


    val tableName  = "dm_b_clickthrouth_emp_continue_policy_detail"

    //    val user103 = "mysql.username.103"
    //    val pass103 = "mysql.password.103"
    //    val url103 = "mysql_url.103.dmdb"
    val driver = "mysql.driver"
    val user106 = "mysql.username.106"
    val pass106 = "mysql.password.106"
    val url106 = "mysql_url.106.dmdb"

//    // saveASMysqlTable(res: DataFrame, tableName, SaveMode.Overwrite,user103,pass103,driver,url103)
//    saveASMysqlTable(res: DataFrame, tableName, SaveMode.Overwrite,user106,pass106,driver,url106)
//    res.printSchema()

  }
}
