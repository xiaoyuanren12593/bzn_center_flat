package bzn.safedata

import java.text.SimpleDateFormat
import java.util.Date

import bzn.job.common.{MysqlUntil, Until}
import bzn.other.OdsOtherIncrementDetail.OfficialDataToHive
import bzn.other.OdsOtherIncrementDetailTest.{clean, dateDelect, readMysqlTable, sparkConfInfo}
import bzn.util.SparkUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.hive.HiveContext

object OdsOfficialDetailTest extends SparkUtil with Until with MysqlUntil {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName: String = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "local[*]")

    val sc: SparkContext = sparkConf._2
    val sqlContext = sparkConf._3
    val hiveContext: HiveContext = sparkConf._4


    //核心数据全量写入
    val officialData = OfficialDataToHive(hiveContext)
    /*officialData.registerTempTable("PersonBaseInfoData")
    hiveContext.sql("INSERT OVERWRITE table odsdb.ods_all_business_person_base_info_detail PARTITION(business_line = 'official',years) select * from PersonBaseInfoData")*/
    officialData.printSchema()

    /* val res5 = InterAMonthAgo(hiveContext)
     res5.printSchema()*/


    sc.stop()
  }

  /**
   * 核心库数据
   *
   * @param hqlContext
   * @return
   */
  def OfficialDataToHive(hqlContext: HiveContext): DataFrame = {

    import hqlContext.implicits._

    //读取保单明细表
    val odsPolicyDetail = hqlContext.sql("select policy_code,policy_id,policy_status from odsdb.ods_policy_detail")

    //读取被保人表
    val odsPolicyInsuredDetail = hqlContext.sql("select insured_name,insured_cert_no,insured_mobile,policy_code as policy_code_salve,start_date," +
      "end_date,create_time,update_time from odsdb.ods_policy_insured_detail")

    //读取产品方案表
    val odsProductPlanDetail = hqlContext.sql("select policy_code,product_code,sku_price from odsdb.ods_policy_product_plan_detail")

    //拿到保单在保退保终止的保单
    val odsPolicyAndInsured = odsPolicyInsuredDetail.join(odsPolicyDetail, 'policy_code_salve === 'policy_code, "leftouter")
      .selectExpr("insured_name", "policy_id", "insured_cert_no", "insured_mobile", "policy_code_salve", "start_date", "end_date", "policy_status", "create_time", "update_time")
      .where("policy_status in (0,1,-1)")

    //拿到产品
    val res = odsPolicyAndInsured.join(odsProductPlanDetail, 'policy_code_salve === 'policy_code, "leftouter")
      .selectExpr(
        "insured_name",
        "insured_cert_no",
        "insured_mobile",
        "policy_code_salve",
        "policy_id",
        "start_date",
        "end_date",
        "create_time",
        "update_time",
        "product_code",
        "sku_price",
        "'official' as business_line",
        "trim(substring(cast(if(start_date is null,if(end_date is null ,if(create_time is null," +
          "if(update_time is null,now(),update_time),create_time),end_date),start_date) as STRING),1,7)) as years")

    res

  }
}

  /*/**
   * 核心库数据
   *
   * @param hqlContext
   * @return
   */
  def OfficialDataToHive(hqlContext: HiveContext): DataFrame = {
    hqlContext.udf.register("clean", (str: String) => clean(str))
    import hqlContext.implicits._

    //读取保单明细表
    val odsPolicyDetail = hqlContext.sql("select policy_code,policy_id,policy_status from odsdb.ods_policy_detail")

    //读取被保人表 只拿距离现在7天的数据
    val odsPolicyInsuredDetail = hqlContext.sql("select id as id_salve,insured_name,insured_cert_no,insured_mobile,policy_code as policy_code_salve,start_date," +
      "end_date,create_time,update_time from odsdb.ods_policy_insured_detail " +
      "where cast(date_add(now(),-7) as string) <= cast(if(create_time is null,now(),create_time) as string)")
    println(odsPolicyInsuredDetail.count())
    //读取产品方案表
    val odsProductPlanDetail = hqlContext.sql("select policy_code,product_code,sku_price from odsdb.ods_policy_product_plan_detail")

    //拿到保单在保退保终止的保单
    val odsPolicyAndInsured = odsPolicyInsuredDetail.join(odsPolicyDetail, 'policy_code_salve === 'policy_code, "leftouter")
      .selectExpr("id_salve", "insured_name", "policy_id", "insured_cert_no", "insured_mobile", "policy_code_salve", "start_date", "end_date", "policy_status", "create_time", "update_time")
      .where("policy_status in (0,1,-1)")

    //拿到产品
    val resTemp = odsPolicyAndInsured.join(odsProductPlanDetail, 'policy_code_salve === 'policy_code, "leftouter")
      .selectExpr(
        "id_salve",
        "insured_name as insured_name_salve",
        "insured_cert_no as insured_cert_no_salve",
        "insured_mobile as insured_mobile_salve",
        "policy_code_salve",
        "policy_id as policy_id_salve",
        "start_date as start_date_salve",
        "end_date as end_date_salve",
        "create_time as create_time_salve",
        "update_time as update_time_salve",
        "product_code as product_code_salve",
        "sku_price as sku_price_salve",
        "'official' as business_line_salve",
        "trim(substring(cast(if(start_date is null,if(end_date is null ,if(create_time is null," +
          "if(update_time is null,now(),update_time),create_time),end_date),start_date) as STRING),1,7)) as years_id_salve")
    println(resTemp.count())

    //读取客户池数据(截止现在7天内)
    val insuredBaseInfo = hqlContext.sql("select id,insured_name,insured_cert_no,insured_mobile," +
      "policy_code,policy_id,start_date,end_date,create_time," +
      "update_time,product_code,sku_price,business_line,years_id from odsdb.ods_all_business_insured_base_info_detail " +
      "where business_line='official' and cast(date_add(now(),-7) as string) <= cast(if(create_time is null,now(),create_time) as string)")
    println(insuredBaseInfo.count())

    val res = resTemp.join(insuredBaseInfo, 'policy_id_salve === 'policy_id and 'id_salve === 'id, "leftouter")
      .where("policy_id is null and id is null")
      .selectExpr("id_salve",
        "insured_name_salve",
        "insured_cert_no_salve",
        "insured_mobile_salve",
        "policy_code_salve",
        "policy_id_salve",
        "start_date_salve",
        "end_date_salve",
        "create_time_salve",
        "update_time_salve",
        "product_code_salve",
        "sku_price_salve",
        "business_line_salve as business_line",
        "years_id_salve as years_id")
    res


  }*/
