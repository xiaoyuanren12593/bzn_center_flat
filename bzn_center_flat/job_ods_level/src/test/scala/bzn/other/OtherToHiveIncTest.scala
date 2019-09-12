package bzn.other

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import bzn.job.common.Until
import bzn.ods.util.SparkUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

import scala.io.Source

object OtherToHiveIncTest extends SparkUtil with Until {

  def main(args: Array[String]): Unit = {

    //    初始化设置
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName: String = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "local[*]")

    val sc: SparkContext = sparkConf._2
    val hiveContext: HiveContext = sparkConf._4

    //    数据迁移
    val otherDetail: DataFrame = readMysqlOtherTable(hiveContext)
//    otherDetail.write.mode(SaveMode.Append).partitionBy("month").saveAsTable("odsdb.ods_open_other_policy_detail")
    otherDetail.printSchema()

    sc.stop()

  }

  /**
    * 获取 Mysql 表的数据
    * @param sqlContext
    * @return 返回 Mysql 表的 DataFrame
    */
  def readMysqlOtherTable(sqlContext: SQLContext): DataFrame = {
    val properties: Properties = getProPerties()

    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    //    计算当前时间和七日前时间
    val currDate = sdf.format(new Date()).split(" ")(0) + " 00:00:00"
    val sevenDate = currTimeFuction(currDate, -7)
    //    计算当前月份和上个月份
    val currMonth = currDate.substring(0, 8) + "01"
    val lastMonth = currMonth.substring(0, 6) + (currMonth.substring(6, 7).toInt - 1).toString + currMonth.substring(7)

    val table = "(select * from open_other_policy where month = '" + lastMonth + "' or month = '" + currMonth + "') as T"
    val condition = "create_time >= '" + sevenDate + "' and create_time < '" + currDate + "'"

    val otherTemp: DataFrame = sqlContext
      .read
      .format("jdbc")
      .option("url", properties.getProperty("mysql.url.103"))
      .option("driver", properties.getProperty("mysql.driver"))
      .option("user", properties.getProperty("mysql.username.103"))
      .option("password", properties.getProperty("mysql.password.103"))
      .option("dbtable", table)
      .load()

    val otherResult: DataFrame = otherTemp
      .where(condition)
      .selectExpr("policy_id", "proposal_no", "policy_no", "batch_id", "user_id", "product_code", "order_id", "start_date",
        "end_date", "holder_name", "holder_ename", "holder_first_name", "holder_last_name", "holder_mobile", "holder_cert_type",
        "holder_cert_no", "holder_email", "cast(holder_birth_day as string) as holder_birth_day", "holder_sex", "holder_industry", "insured_name", "insured_ename",
        "insured_first_name", "insured_last_name", "insured_mobile", "insured_cert_type", "insured_cert_no", "insured_holder_relation",
        "insured_email", "cast(insured_birth_day  as string) as insured_birth_day", "insured_sex", "insured_industry", "status", "export_status", "extend_key1",
        "extend_key2", "extend_key3", "extend_key4", "extend_key5", "create_time", "update_time", "cast(month as string) as month", "province_code",
        "city_code", "area_code", "address", "cast(amount as decimal(14,4)) as amount", "cast(premium as decimal(14,4)) as premium", "insured_time")

    otherResult

  }

  /**
    * 获取配置文件
    *
    * @return
    */
  def getProPerties() = {
    val lines_source = Source.fromURL(getClass.getResource("/config_scala.properties")).getLines.toSeq
    var properties: Properties = new Properties()
    for (elem <- lines_source) {
      val split = elem.split("==")
      val key = split(0)
      val value = split(1)
      properties.setProperty(key,value)
    }
    properties
  }

}
