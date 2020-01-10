package bzn.other

import java.util.Properties

import bzn.job.common.{MysqlUntil, Until}
import bzn.other.OdsOtherToHivePolicyDetail.{getProPerties, sparkConfInfo}
import bzn.util.SparkUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext

object Ods20191201Test extends SparkUtil with Until with MysqlUntil {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName: String = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "local[*]")

    val sc: SparkContext = sparkConf._2
    val sqlContext = sparkConf._3
    val hiveContext: HiveContext = sparkConf._4

    hiveContext.setConf("hive.exec.dynamic.partition", "true")
    hiveContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

    val res = wubaData(hiveContext)

    res.write.mode(SaveMode.Append).format("PARQUET").partitionBy("business_line", "years")
      .saveAsTable("odsdb.ods_all_business_person_base_info_detail")

    sc.stop()
  }


  /**
   * 上下文
   *
   * @param sqlContext
   */
  def OdsOtherToHive(sqlContext: SQLContext): DataFrame = {

    val properties: Properties = getProPerties()
    val url = "jdbc:mysql://172.16.11.103:3306/bzn_open_all?tinyInt1isBit=false&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&allowMultiQueries=true&user=root&password=123456"



    val data: DataFrame = sqlContext.read.jdbc(url, "open_other_policy",properties)
      .where("month='2020-01-01'")
      .selectExpr("policy_id","insured_name","insured_cert_no","insured_mobile","policy_no","start_date","end_date","create_time","update_time",
        "product_code","null as sku_price","'inter' as business_line","substring(cast(month as string),1,7) as years")


    val res = data.selectExpr(
      "insured_name",
      "insured_cert_no",
      "insured_mobile",
      "policy_no",
      "policy_id",
      "start_date",
      "end_date",
      "create_time",
      "update_time",
      "product_code",
      "sku_price",
      "business_line",
      "years")
    res

  }


  def wubaData(hiveContext: HiveContext): DataFrame ={

    //读取数据
    val frame = hiveContext.sql("select insured_name,insured_cert_no,insured_mobile,null as policy_code,null as policy_id, start_date, end_date,create_time,update_time,product_code,sku_price,business_line,years from odsdb.ods_all_business_person_base_info_detail_temp")

    frame



  }


}
