package bzn.other

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import bzn.job.common.{MysqlUntil, Until}
import bzn.other.OdsOtherToHivePolicyDetailTest.{getProPerties, sparkConfInfo}
import bzn.util.SparkUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.hive.HiveContext

/*
* @Author:liuxiang
* @Date：2019/12/5
* @Describe:
*/ object OdsOtherIncrementDetailTest extends SparkUtil with Until with MysqlUntil {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName: String = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "local[*]")

    val sc: SparkContext = sparkConf._2
    val sqlContext = sparkConf._3
    val hiveContext: HiveContext = sparkConf._4
    val res = OdsOtherToHive(hiveContext)

    sc.stop()
  }


  /**
    * 上下文
    *
    *
    */
  def OdsOtherToHive(hiveContext: HiveContext): DataFrame = {
    import hiveContext.implicits._
    hiveContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")//设置日期格式
      val date = df.format(new Date())// new Date()为获取当前系统时间
      (date + "")
    })

    /**
      * 获取mysql中接口的数据
      */

    val properties: Properties = getProPerties()
    val url = "jdbc:mysql://172.16.11.103:3306/bzn_open_all?tinyInt1isBit=false&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&allowMultiQueries=true&user=root&password=123456"

    //拿到当前时间所在月份的数据
    val data1: DataFrame = hiveContext.read.jdbc(url, "open_other_policy", properties)
      .where("substring(cast(case when month is null then getNow() else month end as string),1,7) = substring(cast(getNow() as string),1,7)")
      .selectExpr("policy_id","insured_name","insured_cert_no","insured_mobile","policy_no","start_date","end_date","create_time","update_time",
        "product_code","null as sku_price","'other' as business_line","substring(cast(case when month is null then getNow() else month end as string),1,7) as months")

  // 读取接口当月数据

    val data2 = hiveContext.sql("select policy_id as policy_id_salve,years,business_line as business_line_salve from odsdb.ods_other_detail_test_temp")
      .where("substring(cast(getNow() as string),1,7) = years and business_line_salve = 'other'")

    //拿到当月数据的增量

    val data3 = data1.join(data2, 'policy_id === 'policy_id_salve, "leftouter")
      .selectExpr("policy_id", "policy_id_salve", "insured_name", "insured_cert_no", "insured_mobile", "policy_no", "start_date", "end_date", "create_time", "update_time", "product_code", "sku_price", "business_line", "months")
      .where("policy_id_salve is null")

    val res = data3.selectExpr("insured_name", "insured_cert_no", "insured_mobile","policy_id", "policy_no", "start_date", "end_date", "create_time", "update_time", "product_code", "sku_price", "business_line", "months as years")
  res.printSchema()
    res

  }


  // 核心库增量数据


}