package bzn.dw.premium

import java.sql.Timestamp
import java.text.{NumberFormat, SimpleDateFormat}
import java.util.Date

import bzn.job.common.Until
import bzn.ods.util.SparkUtil
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * author:xiaoYuanRen
  * Date:2019/6/11
  * Time:15:28
  * describe: 雇主年单月单每日保费
  **/
object DwYearAndMonthInsuredPremiumDetail extends SparkUtil with Until{
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName,"")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    val res = DwYearAndMonthInsuredPremium(hiveContext)
    res.write.mode(SaveMode.Overwrite).saveAsTable("dwdb.dw_year_and_month_insured_premium_detail")
    sc.stop()
  }

  /**
    * 计算保单与在保人的每日已赚保费，年单和月单
    * 月单取在保人的开始时间和结束时间
    * @param sqlContext
    */
  def DwYearAndMonthInsuredPremium(sqlContext:HiveContext) ={
    sqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    sqlContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")//设置日期格式
      val date = df.format(new Date())// new Date()为获取当前系统时间
      (date + "")
    })
    /**
      * 读取保单表
      */
    val odsPolicyDetail = sqlContext.sql("select policy_id,policy_code,policy_start_date,policy_end_date,policy_status,holder_name " +
      "from odsdb.ods_policy_detail")
        .where("policy_status in (1,0)")
      .cache()

    /**
      * 读取在保人表
      */
    val odsPolicyInsuredDetail =
      sqlContext.sql("select policy_id as policy_id_insured,insured_cert_no,insured_id,policy_status as insured_policy_status," +
      "start_date as insured_start_date,end_date as insured_end_date from odsdb.ods_policy_insured_detail")
      .where("length(insured_start_date)>0 and length(insured_end_date)>0")
      .cache()

    /**
      * 读取方案表
      */
    val odsPolicyProductPlan = sqlContext.sql("select policy_code as policy_code_plan,sku_charge_type,sku_price,one_level_pdt_cate" +
      " from odsdb.ods_policy_product_plan")
      .where("one_level_pdt_cate = '蓝领外包' and sku_price is not null")
      .cache()

    /**
      * 保单与被保人做关联
      */
    val odsPolicyInsuredRes =
      odsPolicyDetail.join(odsPolicyInsuredDetail,odsPolicyDetail("policy_id")===odsPolicyInsuredDetail("policy_id_insured"))
      .selectExpr("policy_id","policy_code","insured_cert_no","policy_start_date","policy_end_date","holder_name","insured_id",
       "insured_policy_status", "insured_start_date","insured_end_date")

    /**
      * 上述结构与方案表做关联
      */
    val odsPolicyInsuredPlanRes =
      odsPolicyInsuredRes.join(odsPolicyProductPlan,odsPolicyInsuredRes("policy_code")===odsPolicyProductPlan("policy_code_plan"))
      .selectExpr("policy_id","policy_code","insured_cert_no","policy_start_date","policy_end_date","holder_name","insured_id","insured_policy_status",
        "insured_start_date","insured_end_date","sku_charge_type","sku_price")
      .cache()

    val yearData: DataFrame = yearPremium(sqlContext,odsPolicyInsuredPlanRes)
    val monthData: DataFrame = monthPremium(sqlContext,odsPolicyInsuredPlanRes)

    val res = yearData.unionAll(monthData)
      .selectExpr("getUUID() as id","policy_id","sku_day_price","insured_id","insured_cert_no","insured_start_date","insured_end_date",
        "insure_policy_status","day_id","sku_price","holder_name","getNow() as dw_create_time")

    res.printSchema()
    res
  }

  /**
    * 年单保费计算方式
    * @param date
    */
  def yearPremium(sqlContext: HiveContext,date:DataFrame) = {
    import sqlContext.implicits._
    val res = date.where("sku_charge_type = '2'").mapPartitions(rdd => {
        // 创建一个数值格式化对象(对数字)
        val numberFormat = NumberFormat.getInstance
        // 设置精确到小数点后2位
        numberFormat.setMaximumFractionDigits(4)

        rdd.flatMap(x => {
          var holderName = x.getAs[String]("holder_name")

          val policyId = x.getAs[String]("policy_id")
          val skuPrice = x.getAs[Double]("sku_price")
          val insuredId = x.getAs[String]("insured_id")
          val insuredCertNo = x.getAs[String]("insured_cert_no")
          val startDate = x.getAs[Timestamp]("policy_start_date").toString.split(" ")(0).replaceAll("-", "").replaceAll("/", "")
          val endDate = x.getAs[Timestamp]("policy_end_date").toString.split(" ")(0).replaceAll("-", "").replaceAll("/", "")

          val insuredStartDate = x.getAs[String]("insured_start_date").split(" ")(0).replaceAll("-", "").replaceAll("/", "")
          val insuredEndDate = x.getAs[String]("insured_end_date").split(" ")(0).replaceAll("-", "").replaceAll("/", "")

          val insurePolicyStatus = x.getAs[String]("insured_policy_status")
          // sku_charge_type:1是月单，2是年单子
          //判断是年单还是月单
          //如果是月单，则计算的是我当月的平均保费使用的字段是:insured_start_date,insured_end_date
          //如果是年单，则计算的是我当年的平均保费使用的字段是:start_date,end_date

          //保单层面的循环天数
          val dateNumber = getBeg_End_one_two(startDate, endDate).size

          val res = getBeg_End_one_two(insuredStartDate, insuredEndDate).map(day_id => {
            val skuDayPrice = numberFormat.format(skuPrice / dateNumber)
            (policyId,skuDayPrice,insuredId,insuredCertNo,insuredStartDate,insuredEndDate,insurePolicyStatus,day_id,skuPrice,holderName)
          })
          res
        })
    }).toDF("policy_id","sku_day_price","insured_id","insured_cert_no","insured_start_date","insured_end_date","insure_policy_status",
      "day_id","sku_price","holder_name")
    res
  }

  /**
    * 月单保费计算方式
    * @param date
    */
  def monthPremium(sqlContext: HiveContext,date:DataFrame) = {
    import sqlContext.implicits._
    val res = date.where("sku_charge_type = '1'").mapPartitions(rdd => {
      // 创建一个数值格式化对象(对数字)
      val numberFormat = NumberFormat.getInstance
      // 设置精确到小数点后2位
      numberFormat.setMaximumFractionDigits(4)

      rdd.flatMap(x => {
        var holderName = x.getAs[String]("holder_name")

        val policyId = x.getAs[String]("policy_id")
        val skuPrice = x.getAs[Double]("sku_price")
        val insuredId = x.getAs[String]("insured_id")
        val insuredCertNo = x.getAs[String]("insured_cert_no")

        val insuredStartDate = x.getAs[String]("insured_start_date").split(" ")(0).replaceAll("-", "").replaceAll("/", "")
        val insuredEndDate = x.getAs[String]("insured_end_date").split(" ")(0).replaceAll("-", "").replaceAll("/", "")

        val insurePolicyStatus = x.getAs[String]("insured_policy_status")
        // sku_charge_type:1是月单，2是年单子
        //判断是年单还是月单
        //如果是月单，则计算的是我当月的平均保费使用的字段是:insured_start_date,insured_end_date
        //如果是年单，则计算的是我当年的平均保费使用的字段是:start_date,end_date

        //保单层面的循环天数
        val dateNumber = getBeg_End_one_two(insuredStartDate, insuredEndDate).size

        val res: mutable.Seq[(String, String, String, String, String, String, String, String, Double, String)] =
          getBeg_End_one_two(insuredStartDate, insuredEndDate).map(day_id => {
          val skuDayPrice = numberFormat.format(skuPrice / dateNumber)
          (policyId,skuDayPrice,insuredId,insuredCertNo,insuredStartDate,insuredEndDate,insurePolicyStatus,day_id,skuPrice,holderName)
        })
        res
      })
    }).toDF("policy_id","sku_day_price","insured_id","insured_cert_no","insured_start_date","insured_end_date","insure_policy_status",
      "day_id","sku_price","holder_name")
    res
  }
}
