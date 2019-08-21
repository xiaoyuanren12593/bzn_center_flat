package bzn.dw.premium

import java.text.{NumberFormat, SimpleDateFormat}
import java.util.Date

import bzn.job.common.Until
import bzn.ods.util.SparkUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

/**
  * author:xiaoYuanRen
  * Date:2019/6/12
  * Time:10:04
  * describe: 保单级别每日已赚保费
  **/
object DwPolicyEveryDayPremiumTest extends SparkUtil with Until{
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName,"local[*]")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    val res = dwPolicyEveryDayPremiumDetail(hiveContext)
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
      (date + "")
    })
    import sqlContext.implicits._
    val policyEveryDayPremium = sqlContext.sql("select policy_id,day_id,sku_day_price from dwdb.dw_year_and_month_insured_premium_detail")
      .map(x => {
        var policyId = x.getAs[String]("policy_id")
        var dayId = x.getAs[String]("day_id")
        var skuDayPrice = x.getAs[String]("sku_day_price")
        var skuDayPriceRes = 0.0
        if(skuDayPrice != null){
          skuDayPriceRes = skuDayPrice.toDouble
        }
        ((policyId,dayId),skuDayPriceRes)
      })
      .reduceByKey(_+_)
      .map(x => {
        // 创建一个数值格式化对象(对数字)
        val numberFormat = NumberFormat.getInstance
        // 设置精确到小数点后4位
        numberFormat.setMaximumFractionDigits(4)
        (x._1._1,x._1._2,numberFormat.format(x._2).replaceAll(",","").toDouble)
      })
      .toDF("policy_id","day_id","premium")
      .selectExpr("getUUID() as id","policy_id","day_id","premium","getNow() as dw_create_time")
    policyEveryDayPremium.printSchema()
    policyEveryDayPremium
  }
}
