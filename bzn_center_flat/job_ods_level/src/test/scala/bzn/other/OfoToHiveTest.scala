package bzn.other

import java.util.Properties

import bzn.job.common.Until
import bzn.ods.util.SparkUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.hive.HiveContext

import scala.io.Source

object OfoToHiveTest extends SparkUtil with Until {

  def main(args: Array[String]): Unit = {

    //    初始化设置
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName: String = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "local[*]")

    val sc: SparkContext = sparkConf._2
    val hiveContext: HiveContext = sparkConf._4

    val ofoDetail: DataFrame = readMysqlOtherTable(hiveContext)
    ofoDetail.printSchema()

    sc.stop()

  }

  /**
    * 获取 Mysql 表的数据
    * @param sqlContext
    * @return 返回 Mysql 表的 DataFrame
    */
  def readMysqlOtherTable(sqlContext: SQLContext): DataFrame = {
    sqlContext.udf.register("dropEmptys", (line: String) => dropEmpty(line))
    sqlContext.udf.register("dropSpecial", (line: String) => dropSpecial(line))
    sqlContext.udf.register("getEmpty", () => "")
    val properties: Properties = getProPerties()

    //    201711-201810
    val url1: String = "jdbc:mysql://172.16.11.103:3306/bzn_open_201711?tinyInt1isBit=false&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&allowMultiQueries=true&user=root&password=123456"

    //    mysql 201711-201810
    val result: DataFrame = sqlContext.read.jdbc(url1, "open_ofo_policy", properties)
      .selectExpr("policy_id", "proposal_no", "policy_no", "batch_id", "user_id", "product_code", "order_id", "start_date",
        "end_date", "holder_name", "holder_ename", "holder_first_name", "holder_last_name", "holder_mobile", "holder_cert_type",
        "holder_cert_no", "holder_email", "cast(holder_birth_day as string) as holder_birth_day", "holder_sex", "holder_industry", "insured_name", "insured_ename",
        "insured_first_name", "insured_last_name", "insured_mobile", "insured_cert_type", "insured_cert_no", "insured_holder_relation",
        "insured_email", "cast(insured_birth_day  as string) as insured_birth_day", "insured_sex", "insured_industry", "status", "export_status", "extend_key1",
        "extend_key2", "extend_key3", "extend_key4", "extend_key5", "create_time", "update_time", "cast(month as string) as month")

    result

  }

  /**
    * 获取配置文件
    *
    * @return
    */
  def getProPerties(): Properties = {
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
