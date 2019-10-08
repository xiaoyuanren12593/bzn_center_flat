package bzn.dm.premium

import java.text.SimpleDateFormat
import java.util.Date

import bzn.dm.util.SparkUtil
import bzn.job.common.Until
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.hive.HiveContext

/*
* @Author:liuxiang
* @Date：2019/9/29
* @Describe: 销售易每日在保人数和已赚保费
*/ object DmSaleEasyPolicyCurrInsuredAndPremiumDetailTest extends SparkUtil with Until {
  def main(args: Array[String]): Unit = {


    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "local[*]")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    val res = DmSaleEasyPolicyCurrInsuredAndPremium(hiveContext)
    res.printSchema()

    sc.stop()

  }

  /**
    *
    * @param sqlContext
    * @return
    */

  def DmSaleEasyPolicyCurrInsuredAndPremium(sqlContext: HiveContext): DataFrame = {
    import sqlContext.implicits._
    sqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    sqlContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      //设置日期格式
      val date = df.format(new Date()) // new Date()为获取当前系统时间
      date + ""
    })

    /**
      * 读取销售易的宽表
      */
    val dayIdInsure = sqlContext.sql("select policy_id,policy_code,product_code,policy_start_date,policy_end_date," +
      "insure_company_name,insurant_company_name,product_name,two_level_pdt_cate,holder_province,holder_city," +
      "sku_coverage,sku_ratio,sku_charge_type,sku_price,sku_append,profession_type,ent_id,ent_name," +
      "salesman,team_name,biz_operator,consumer_category,channel_id,channel_name,curr_insured,day_id,date_time," +
      "is_old_customer,policy_create_time,policy_update_time from dwdb.dw_saleeasy_policy_curr_insured_detail")

    /**
      * 读取每日已赚保费表
      */
    val dayIdPremium = sqlContext.sql("select policy_id as policy_id_temp,day_id as day_id_temp,premium from dwdb.dw_policy_everyDay_premium_detail")

    /**
      * 关联两个表
      */
    val resTemp = dayIdPremium.join(dayIdInsure, 'policy_id_temp === 'policy_id and 'day_id_temp === 'day_id, "leftouter")
      .selectExpr("getUUID() as id", "policy_id_temp",
        "policy_code",
        "product_code",
        "policy_start_date",
        "policy_end_date",
        "insure_company_name",
        "insurant_company_name",
        "product_name",
        "two_level_pdt_cate",
        "holder_province",
        "holder_city",
        "sku_coverage",
        "sku_ratio",
        "sku_charge_type",
        "sku_price",
        "sku_append",
        "profession_type",
        "ent_id",
        "ent_name",
        "salesman",
        "team_name",
        "biz_operator",
        "consumer_category",
        "channel_id",
        "channel_name",
        "curr_insured",
        "premium",
        "day_id_temp",
        "date_time",
        "is_old_customer",
        "policy_create_time",
        "policy_update_time",
        "getNow() as dw_create_time")

    /**
      * 读取理赔表
      */
    val policyClaim = sqlContext.sql("select policy_id,risk_date,res_pay from dwdb.dw_policy_claim_detail")

    /**
      * 将理赔表中的risk_date 转化成与day_id一样的格式
      */
    val policyClaimTemp = policyClaim.selectExpr("policy_id", "risk_date", "res_pay").map(x => {
      val policyId = x.getAs[String]("policy_id")
      val riskDate = x.getAs[String]("risk_date")
      val riskDateTemp = if (riskDate != null) {
        java.sql.Timestamp.valueOf(getFormatTime
        (getBeginTime(riskDate.replaceAll("/", "-").concat(" 00:00:00"))))
      } else {
        null
      }
      val riskDateRes = if (riskDateTemp != null) riskDateTemp.toString.substring(0, 10).replaceAll("-", "") else null
      val resPay = x.getAs[java.math.BigDecimal]("res_pay")
      (policyId, riskDateRes, resPay)

    }).toDF("policy_id_res", "risk_date", "res_pay")

    val policyClaimRes = policyClaimTemp.selectExpr("policy_id_res", "risk_date", "res_pay")

    /**
      * 按照出险日期和保单id就行分组
      */

    val policyClaimTable = policyClaimRes.registerTempTable("policyClaimTemp")
    val policySumClaim = sqlContext.sql("select policy_id_res ,sum(res_pay) as res_pay ,risk_date from policyClaimTemp group by policy_id_res,risk_date")

    /**
      * 讲每日在报人数和已赚保费作为左表 关联 理赔表
      */
    val res = resTemp.join(policySumClaim, 'policy_id_temp === 'policy_id_res and 'day_id_temp === 'risk_date, "leftouter")
      .selectExpr("getUUID() as id", "policy_id_temp",
        "policy_code",
        "product_code",
        "policy_start_date",
        "policy_end_date",
        "insure_company_name",
        "insurant_company_name",
        "product_name",
        "two_level_pdt_cate",
        "holder_province",
        "holder_city",
        "sku_coverage",
        "sku_ratio",
        "sku_charge_type",
        "sku_price",
        "sku_append",
        "profession_type",
        "ent_id",
        "ent_name",
        "salesman",
        "team_name",
        "biz_operator",
        "consumer_category",
        "channel_id",
        "channel_name",
        "curr_insured",
        "premium",
        "day_id_temp",
        "cast(res_pay as decimal(14,4)) as res_pay",
        "date_time",
        "is_old_customer",
        "policy_create_time",
        "policy_update_time",
        "getNow() as dw_create_time")

     res


  }
}
