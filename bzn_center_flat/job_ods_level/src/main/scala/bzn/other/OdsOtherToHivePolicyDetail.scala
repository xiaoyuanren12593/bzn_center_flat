package bzn.other

import java.util.Properties

import bzn.job.common.{MysqlUntil, Until}
import bzn.util.SparkUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext

/*
* @Author:liuxiang
* @Date：2019/12/2
* @Describe:
*/ object OdsOtherToHivePolicyDetail extends SparkUtil with Until with MysqlUntil {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName: String = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "")

    val sc: SparkContext = sparkConf._2
    val sqlContext = sparkConf._3
    val hiveContext: HiveContext = sparkConf._4
    val res = OdsOtherToHive(hiveContext)

    res.write.mode(SaveMode.Append).saveAsTable("odsdb.ods_other_detail_test")

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
      .selectExpr("policy_id","insured_name","insured_cert_no","insured_mobile","policy_no","start_date","end_date","create_time","update_time",
        "product_code","null as sku_price","'other' as business_line","month")
      .where("month>='2019-09-01' and month <'2019-12-01'")
    data

  }


}
