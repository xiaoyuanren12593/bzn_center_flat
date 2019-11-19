package bzn.ods.policy

import java.sql.Timestamp

import bzn.job.common.MysqlUntil
import bzn.ods.policy.OdsInterAndWeddingPremiumDetailTest.clean
import bzn.ods.util.Until
import bzn.util.SparkUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext

/*
* @Author:liuxiang
* @Date：2019/11/18
* @Describe:
*/ object OdsAllBusinessPersonInfoDetailTest extends SparkUtil with Until with MysqlUntil {

  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "local[*]")

    val sc = sparkConf._2
    val hqlContext = sparkConf._4
    val sqlContext = sparkConf._3
    hqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")


    // res.registerTempTable("hiveInsuredData")
    // hqlContext.sql("insert into odsdb.ods_all_business_person_base_info_detail select * from hiveInsuredData")
    /*hqlContext.sql("truncate table odsdb.ods_all_business_person_base_info_detail")
    res.write.mode(SaveMode.Append).partitionBy("yearandmonth").saveAsTable("odsdb.ods_all_business_person_base_info_detail_test")*/
    val res1 = HiveDataPerson(hqlContext)
    val res2 = hiveExpressData(hqlContext)
    val res3 = hiveOfoData(hqlContext)
    val res4 = res1.unionAll(res2).unionAll(res3)
    res4.printSchema()
    sc.stop()

  }

  /**
    * 获取hive中核心库的数据
    *
    * @param hqlContext
    */
  def HiveDataPerson(hqlContext: HiveContext): DataFrame = {
    import hqlContext.implicits._

    //读取保单明细表
    val odsPolicyDetail = hqlContext.sql("select policy_code,policy_status from odsdb.ods_policy_detail")

    //读取被保人表

    val odsPolicyInsuredDetail = hqlContext.sql("select insured_name,insured_cert_no,insured_mobile,policy_code as policy_code_salve,start_date,end_date,create_time,update_time from odsdb.ods_policy_insured_detail")

    //读取产品方案表
    val odsProductPlanDetail = hqlContext.sql("select policy_code,product_code,sku_price from odsdb.ods_policy_product_plan_detail")

    //拿到保单在保退保终止的保单
    val odsPolicyAndInsured = odsPolicyInsuredDetail.join(odsPolicyDetail, 'policy_code_salve === 'policy_code, "leftouter")
      .selectExpr("insured_name", "insured_cert_no", "insured_mobile", "policy_code_salve", "start_date", "end_date", "policy_status", "create_time", "update_time")
      .where("policy_status in (0,1,-1)")

    //拿到产品
    val res = odsPolicyAndInsured.join(odsProductPlanDetail, 'policy_code_salve === 'policy_code, "leftouter")
      .selectExpr(
        "insured_name",
        "insured_cert_no",
        "insured_mobile",
        "policy_code_salve",
        "start_date",
        "end_date",
        "create_time",
        "update_time",
        "product_code",
        "sku_price",
        "'官网' as business_line",
        "substring(cast(if(start_date is null,if(end_date is null ,if(create_time is null,if(update_time is null,now(),update_time),create_time),end_date),start_date) as STRING),1,7) as yearandmonth")
    res

  }


  /**
    * 获取58的数据
    *
    * @param hqlContext
    * @return
    */
  def hiveExpressData(hqlContext: HiveContext): DataFrame = {
    hqlContext.udf.register("clean", (str: String) => clean(str))
    val odsExpressPolicy = hqlContext.sql("select * from odsdb_prd.open_express_policy")
      .selectExpr(
        "courier_name as insured_name",
        "courier_card_no as insured_cert_no",
        "client_mobile as insured_mobile",
        "policy_no as policy_code_salve",
        "cast(start_time as timestamp) as start_date",
        "cast(end_time as timestamp) as end_date",
        "cast(create_time as timestamp)",
        "cast(create_time as timestamp) as update_time",
        "clean('') as product_code",
        "cast(clean('') as decimal(14,4)) as sku_price",
        "'58' as business_line",
        "month_id as yearandmonth")
    odsExpressPolicy
  }

  /**
    * 获取ofo的数据
    *
    * @param hqlContext
    */
  def hiveOfoData(hqlContext: HiveContext): DataFrame = {
    import hqlContext.implicits._
    hqlContext.udf.register("clean", (str: String) => clean(str))
    //读取hive中ofo的数据

    val odsOfoPolicyTemp = hqlContext.sql("select * from  odsdb_prd.open_ofo_policy_parquet")
      .selectExpr("insured_name",
        "insured_cert_no",
        "insured_mobile",
        "policy_id as policy_code_salve",
        "cast(start_date as timestamp) as start_date",
        "cast(end_date as timestamp) as end_date",
        "cast(create_time as timestamp) as create_time",
        "cast(create_time as timestamp) as update_time",
        "product_code",
        "cast(clean('') as decimal(14,4)) as sku_price",
        "'ofo' as business_line",
        "month_id as yearandmonth")
    val odsOfoPolicy: DataFrame = odsOfoPolicyTemp.map(x => {
      val insuredName = x.getAs[String]("insured_name")
      val insuredCert = x.getAs[String]("insured_cert_no")
      val insuredMobile = x.getAs[String]("insured_mobile")
      val policyCode = x.getAs[String]("policy_code_salve")
      val startDate = x.getAs[Timestamp]("start_date")
      val endDate = x.getAs[Timestamp]("end_date")
      val createTime = x.getAs[Timestamp]("create_time")
      val updateTime = x.getAs[Timestamp]("update_time")
      val productCode = x.getAs[Timestamp]("product_code")
      val skuPrice = x.getAs[java.math.BigDecimal]("sku_price")
      val businessLine = x.getAs[String]("business_line")
      val months = x.getAs[String]("yearandmonth")
      val str = if (months.length == 7 && months.contains("-")) {
                    months
      } else  if(months.length > 7 && months.contains("-")) {
        val monthTemp = months.substring(1, 7)
        monthTemp
      } else "aaaaaaaaaa"
      val month = str
      (insuredName, insuredCert, insuredMobile, policyCode, startDate, endDate, createTime, updateTime, productCode, skuPrice, businessLine, month)
    }).toDF("insured_name", "insured_cert_no", "insured_mobile", "policy_code_salve", "start_date", "end_date", "create_time", "update_time", "product_code", "sku_price", "business_line", "yearandmonth")
    odsOfoPolicy
  }
}
