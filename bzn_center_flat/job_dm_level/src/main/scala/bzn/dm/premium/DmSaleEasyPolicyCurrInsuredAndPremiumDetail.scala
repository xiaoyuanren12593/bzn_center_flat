package bzn.dm.premium

import java.text.SimpleDateFormat
import java.util.Date

import bzn.dm.util.SparkUtil
import bzn.job.common.Until
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext

/*
* @Author:liuxiang
* @Date：2019/9/29
* @Describe:
*/ object DmSaleEasyPolicyCurrInsuredAndPremiumDetail extends SparkUtil with Until {
  def main(args: Array[String]): Unit = {


    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    val res = DmSaleEasyPolicyCurrInsuredAndPremium(hiveContext)
    hiveContext.sql("truncate table dmdb.dm_saleeasy_policy_curr_insured_premium_detail")
    res.repartition(10).write.mode(SaveMode.Append).saveAsTable("dmdb.dm_saleeasy_policy_curr_insured_premium_detail")

    sc.stop()

  }

  /**
    * 获取相关的信息
    */

  def DmSaleEasyPolicyCurrInsuredAndPremium(sqlContext: HiveContext): DataFrame = {
    import sqlContext.implicits._
    sqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    sqlContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      //设置日期格式
      val date = df.format(new Date())
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
    val res = dayIdPremium.join(dayIdInsure, 'policy_id_temp === 'policy_id and 'day_id_temp === 'day_id, "leftouter")
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
    res


  }


}
