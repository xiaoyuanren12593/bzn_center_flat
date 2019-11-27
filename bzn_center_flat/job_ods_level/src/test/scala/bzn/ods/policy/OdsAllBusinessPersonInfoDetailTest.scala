package bzn.ods.policy

import java.sql.Timestamp


import bzn.job.common.{MysqlUntil, Until}
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
    hqlContext.setConf("hive.exec.dynamic.partition", "true")
    hqlContext.setConf("hive.exec.dynamic.partition.mode","nonstrict")
    // hqlContext.setConf("hive.exec.max.dynamic.partitions","100000")
    //hqlContext.setConf("hive.exec.max.dynamic.partitions.pernode","100000")

    /* val data = Array(("001", "张三", 21, "112121"), ("002", "李四", 18, "877878"),("003", "李四", 18, "1212123"),
       ("004", "李四", 18, "121212"),("005", "李四", 12, "4564"),("006", "李四", 16, "45646"),("007", "李四", 19, "12155"),
       ("008", "李四", 17, "4567"),("009", "李四", 19, "2220"),("006", "李四", 16, "46545"),("006", "李四", 16, "45645"),("006", "李四", 16, "254"),("006", "李四", 16, "7512"),
       ("006", "李四", 16, "12231456"),("006", "李四", 16, "123123"),("006", "李四", 16, "1234545"),("006", "李四", 16, "1234545"),
       ("006", "李四", 16, "7895"),("006", "李四", 16, "7821"),("006", "李四", 16, "5421"),("006", "李四", 16, "1561212"),("006", "李四", 16, "745"),("006", "李四", 16, "3214"),
       ("006", "李四", 16, "2315245"),("006", "李四", 16, "78555"),("006", "李四", 16, "123123456"),("006", "李四", 16, "784568"),
       ("006", "李四", 16, "1231"),("006", "李四", 16, "4556"),("006", "李四", 16, "2321"),("006", "李四", 16, "1231212"),("006", "李四", 16, "78522278"),
       ("006", "李四", 16, "2452452"),("006", "李四", 16, "424124"),
       ("006", "李四", 16, "45645654"),("006", "李四", 16, "64565"),("006", "李四", 16, "78528728"),("006", "李四", 16, "141414"),
       ("006", "李四", 16, "0414"),("006", "李四", 16, "35"),("006", "李四", 16, "252525"),("006", "李四", 16, "141414"),("006", "李四", 16, "242424"))
     val df = hqlContext.createDataFrame(data).toDF("id", "name", "age", "years")
     hqlContext.sql("truncate table odsdb.ods_patition_test")
     df.write.mode(SaveMode.Append).partitionBy("age","years").saveAsTable("odsdb.ods_patition_test")*/
    // res.registerTempTable("hiveInsuredData")


    // hqlContext.sql("insert into odsdb.ods_all_business_person_base_info_detail select * from hiveInsuredData")
    /*hqlContext.sql("truncate table odsdb.ods_all_business_person_base_info_detail")
    res.write.mode(SaveMode.Append).partitionBy("yearandmonth").saveAsTable("odsdb.ods_all_business_person_base_info_detail_test")*/
   //val res1 = HiveDataPerson(hqlContext)

    //val res2 = hiveExpressData(hqlContext)
    val res3 = hiveOfoData(hqlContext)
    //val res4 = res1.unionAll(res2)
    res3.printSchema()
    //res4.printSchema()
   /*hqlContext.sql("truncate table odsdb.ods_all_business_person_base_info_detail")
    res4.write.mode(SaveMode.Append).format("parquet").partitionBy("business_line")
      .saveAsTable("odsdb.ods_all_business_person_base_info_detail")*/


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
        "trim(substring(cast(if(start_date is null,if(end_date is null ,if(create_time is null," +
          "if(update_time is null,now(),update_time),create_time),end_date),start_date) as STRING),1,7)) as years","'官网' as business_line")
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
        "month_id as years",
        "'58' as business_line")
    odsExpressPolicy
  }
  /**
    * 获取ofo的数据
    *
    * @param hqlContext
    */
  def hiveOfoData(hqlContext: HiveContext): DataFrame = {
    import hqlContext.implicits._
    hqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    hqlContext.udf.register("clean", (str: String) => clean(str))
    //读取hive中ofo的数据

    val odsOfoPolicyTemp =  hqlContext.sql("select insured_name,insured_cert_no,insured_mobile,policy_id as policy_code_salve,cast(start_date as timestamp) as start_date," +
      "cast(create_time as timestamp) as create_time,cast(create_time as timestamp) as update_time,product_code,cast(clean('') as decimal(14,4)) as sku_price,'ofo' as business_line,month_id as years" +
      " from odsdb_prd.open_ofo_policy_parquet where length(month_id)=7 and month_id<'2100' and  month_id<'2016-12'")
        .repartition(1000)

    odsOfoPolicyTemp
  }

}
