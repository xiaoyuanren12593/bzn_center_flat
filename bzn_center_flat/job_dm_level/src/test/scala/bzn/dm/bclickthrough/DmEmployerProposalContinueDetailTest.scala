package bzn.dm.bclickthrough

import java.text.SimpleDateFormat
import java.util.Date

import bzn.dm.util.SparkUtil
import bzn.job.common.{MysqlUntil, Until}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext

/**
  * author:xiaoYuanRen
  * Date:2019/9/24
  * Time:16:52
  * describe: 续投结果数据
  **/
object DmEmployerProposalContinueDetailTest extends SparkUtil with Until with MysqlUntil{
  def main (args: Array[String]): Unit = {
    System.setProperty ("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo (appName, "local[*]")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    val res = continueProposalDetail(hiveContext)
      .selectExpr(
        "id",
        "policy_id",
        "policy_code",
        "policy_start_date",
        "policy_end_date",
        "insure_company_name",//保险公司
        "insure_company_short_name",
        "product_code",
        "product_name",
        "continue_policy_id",
        "preserve_policy_no",
        "sku_coverage",
        "sku_charge_type",
        "sku_price",
        "now_date",
        "curr_insured_count as current_insured",
        "should_continue_policy_date  as continue_date",
        "realy_continue_policy_date",
        "realy_insured_count  as persons_continued",
        "should_continue_policy_date_is",
        "should_insured_count as persons_expired",
        "effect_month as month",
        "month as continue_month",
        "ent_id",
        "ent_name",
        "salesman",
        "team_name",
        "biz_operator",
        "consumer_category",
        "channel_id",
        "channel_name",
        "dw_create_time"
      )

    val mysqlRes = res.selectExpr(
      "id",
      "policy_code",
      "insure_company_name",//保险公司
      "insure_company_short_name",
      "ent_id",
      "ent_name",
      "channel_id",
      "channel_name",
      "salesman",
      "biz_operator",
      "sku_coverage",
      "sku_price",
      "sku_charge_type",
      "current_insured",
      "policy_start_date",
      "policy_end_date",
      "continue_date",
      "continue_month",
      "persons_expired",
      "persons_continued",
      "month",
      "dw_create_time"
    ).cache()

    val tableName  = "dm_employer_policy_continue_detail"

    val user103 = "mysql.username.103"
    val pass103 = "mysql.password.103"
    val url103 = "mysql_url.103.dmdb"
    val driver = "mysql.driver"
    val user106 = "mysql.username.106"
    val pass106 = "mysql.password.106"
    val url106 = "mysql.url.106.dmdb"

    saveASMysqlTable(mysqlRes: DataFrame, tableName, SaveMode.Overwrite,user103,pass103,driver,url103)
    saveASMysqlTable(mysqlRes: DataFrame, tableName, SaveMode.Overwrite,user106,pass106,driver,url106)
    sc.stop ()
  }

  /**
    *
    * @param sqlContext
    */
  def continueProposalDetail(sqlContext:HiveContext) ={
    sqlContext.udf.register ("getUUID", () => (java.util.UUID.randomUUID () + "").replace ("-", ""))
    sqlContext.udf.register ("getNow", () => {
      val df = new SimpleDateFormat ("yyyy-MM-dd HH:mm:ss")
      //设置日期格式
      val date = df.format (new Date ()) // new Date()为获取当前系统时间
      date + ""
    })
    /**
      * 读取续投基础数据
      */
    val dwBClickthrouthEmpProposalContinueDetail = sqlContext.sql("select * from dwdb.dw_b_clickthrouth_emp_proposal_continue_Detail")
      .selectExpr(
        "policy_id",
        "policy_code",
        "policy_start_date",
        "policy_end_date",
        "insure_company_name",//保险公司
        "insure_company_short_name",
        "product_code",
        "product_name",
        "continue_policy_id",
        "preserve_policy_no",
        "sku_coverage",
        "sku_charge_type",
        "sku_price",
        "now_date as day_id",
        "should_continue_policy_date",
        "realy_continue_policy_date",
        "should_continue_policy_date_is",
        "effect_month",
        "month",
        "ent_id",
        "ent_name",
        "salesman",
        "team_name",
        "biz_operator",
        "consumer_category",
        "channel_id",
        "channel_name"
      )

    /**
      * 读取在保人表
      */
    val dwPolicyCurrInsuredDetail = sqlContext.sql("select policy_id,policy_code as policy_code_insured ,day_id,count" +
      " from dwdb.dw_policy_curr_insured_detail")
      .cache()

    /**
      * 统计当前在保人数
      */
    val nowInsuredCountRes = dwBClickthrouthEmpProposalContinueDetail.join(dwPolicyCurrInsuredDetail,Seq("policy_id","day_id"),"leftouter")
      .selectExpr(
        "policy_id",
        "policy_code",
        "policy_start_date",
        "policy_end_date",
        "insure_company_name",//保险公司
        "insure_company_short_name",
        "product_code",
        "product_name",
        "continue_policy_id",
        "preserve_policy_no",
        "sku_coverage",
        "sku_charge_type",
        "sku_price",
        "day_id as now_date",
        "case when count is null then 0 else count end as curr_insured_count",
        "should_continue_policy_date",
        "realy_continue_policy_date",
        "should_continue_policy_date_is as day_id",
        "effect_month",
        "month",
        "ent_id",
        "ent_name",
        "salesman",
        "team_name",
        "biz_operator",
        "consumer_category",
        "channel_id",
        "channel_name"
      )

    /**
      * 统计应续人数
      */
    val shouldInsuredCountRes = nowInsuredCountRes.join(dwPolicyCurrInsuredDetail,Seq("policy_id","day_id"),"leftouter")
      .selectExpr(
        "policy_id as policy_id_master",
        "policy_code",
        "policy_start_date",
        "policy_end_date",
        "insure_company_name",//保险公司
        "insure_company_short_name",
        "product_code",
        "product_name",
        "continue_policy_id  as policy_id",
        "preserve_policy_no",
        "sku_coverage",
        "sku_charge_type",
        "sku_price",
        "now_date",
        "curr_insured_count",
        "should_continue_policy_date",
        "realy_continue_policy_date as day_id",
        "day_id as should_continue_policy_date_is",
        "case when count is null then 0 else count end as should_insured_count",
        "effect_month",
        "month",
        "ent_id",
        "ent_name",
        "salesman",
        "team_name",
        "biz_operator",
        "consumer_category",
        "channel_id",
        "channel_name"
      )

    /**
      * 统计实续在保人数
      */
    val res = shouldInsuredCountRes.join(dwPolicyCurrInsuredDetail,Seq("policy_id","day_id"),"leftouter")
      .selectExpr(
        "getUUID() as id",
        "policy_id_master as policy_id",
        "policy_code",
        "policy_start_date",
        "policy_end_date",
        "insure_company_name",//保险公司
        "insure_company_short_name",
        "product_code",
        "product_name",
        "policy_id as continue_policy_id",
        "preserve_policy_no",
        "sku_coverage",
        "sku_charge_type",
        "sku_price",
        "now_date",
        "curr_insured_count",
        "should_continue_policy_date",
        "day_id as realy_continue_policy_date",
        "case when count is null then 0 else count end as realy_insured_count",
        "should_continue_policy_date_is",
        "should_insured_count",
        "effect_month",
        "month",
        "ent_id",
        "ent_name",
        "salesman",
        "team_name",
        "biz_operator",
        "consumer_category",
        "channel_id",
        "channel_name",
        "getNow() as dw_create_time"
      )
    res
  }
}