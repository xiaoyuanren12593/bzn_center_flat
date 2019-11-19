package bzn.ods.policy

import bzn.job.common.{MysqlUntil, Until}
import bzn.util.SparkUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext

/*
* @Author:liuxiang
* @Date：2019/11/18
* @Describe:
*/ object OdsAllBusinessPersonInfoDetail extends SparkUtil with Until with MysqlUntil {

  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "")

    val sc = sparkConf._2
    val hqlContext = sparkConf._4
    val sqlContext = sparkConf._3
    hqlContext.setConf("hive.exec.dynamic.partition","true")
    hqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    val res = HiveDataPerson(hqlContext)
    val res1 = hiveExpressData(hqlContext)
    val res2 = res.unionAll(res1)
    hqlContext.sql("truncate table odsdb.ods_all_business_person_base_info_detail")
    res2.write.partitionBy("yearandmonth").mode(SaveMode.Append).saveAsTable("odsdb.ods_all_business_person_base_info_detail")
    sc.stop()


    //val res1 = hiveExpressData(hqlContext)
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
        "substring(cast(if(start_date is null,if(end_date is null,if(create_time is null,if(update_time is null,now(),update_time),create_time),end_date),start_date) as STRING),1,7) as yearandmonth")
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

}