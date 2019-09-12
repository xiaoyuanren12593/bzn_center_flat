package bzn.dw.premium

import java.text.{NumberFormat, SimpleDateFormat}
import java.util.Date

import bzn.dw.util.SparkUtil
import bzn.job.common.Until
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * author:xiaoYuanRen
  * Date:2019/6/12
  * Time:10:04
  * describe: 保单级别每日已赚保费
  **/
object DwPolicyEveryDayPremium extends SparkUtil with Until{
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName,"")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    val res = dwPolicyEveryDayPremiumDetail(hiveContext)

    hiveContext.sql("truncate table dwdb.dw_policy_everyDay_premium_detail")
    res.repartition(200).write.mode(SaveMode.Append).saveAsTable("dwdb.dw_policy_everyDay_premium_detail")
    res.repartition(200).write.mode(SaveMode.Overwrite).parquet("/dw_data/dw_data/dw_policy_everyDay_premium_detail")

    sc.stop()
  }

  /**
    * 每个保单每日已赚保费
    * @param sqlContext
    */
  def dwPolicyEveryDayPremiumDetail(sqlContext: HiveContext) ={
    sqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    sqlContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")//设置日期格式
      val date = df.format(new Date())// new Date()为获取当前系统时间
      date + ""
    })
    import sqlContext.implicits._
    val policyEveryDayPremium = sqlContext.sql("select policy_id,day_id,sku_day_price from dwdb.dw_year_and_month_insured_premium_detail")
      .map(x => {
        var policyId = x.getAs[String]("policy_id")
        var dayId = x.getAs[String]("day_id")
        var skuDayPrice = x.getAs[java.math.BigDecimal]("sku_day_price")
        var skuDayPriceRes = java.math.BigDecimal.valueOf(0.0)
        if(skuDayPrice != null){
          skuDayPriceRes = skuDayPrice
        }
        ((policyId,dayId),skuDayPriceRes)
      })
      .reduceByKey(_.add(_))
      .map(x => {
        (x._1._1,x._1._2,x._2)
      })
      .toDF("policy_id","day_id","premium")
      .selectExpr("getUUID() as id","policy_id","day_id","cast(premium as decimal(14,4)) as premium","getNow() as dw_create_time")

    policyEveryDayPremium
  }
}
