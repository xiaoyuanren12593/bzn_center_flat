package bzn.other

import java.util.Properties

import bzn.job.common.Until
import bzn.ods.util.SparkUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.hive.HiveContext

import scala.io.Source

object UserAndMobile extends SparkUtil with Until{

  def main(args: Array[String]): Unit = {

    //    初始化设置
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName: String = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "")

    val sc: SparkContext = sparkConf._2
    val hiveContext: HiveContext = sparkConf._4

//    清晰标签

    /**
      * 读取other和product数据
      */
    val otherInfo: DataFrame = readMysqlOtherTable(hiveContext).cache()
    val proInfo: DataFrame = readMysqlProTable(hiveContext).cache()

    val result: DataFrame = getDataFrame(hiveContext, otherInfo, proInfo)
    result.rdd.repartition(1).saveAsTextFile("hdfs://172.16.11.106:8020/xing/data/userAndMobile")

    sc.stop()

  }

  def getDataFrame(hiveContext: HiveContext, otherInfo: DataFrame, proInfo: DataFrame): DataFrame = {
    import hiveContext.implicits._

    /**
      * 获得去重的产品列表
      */
    val proInfoDistinct: DataFrame = proInfo
      .selectExpr("product_name")
      .dropDuplicates(Array("product_name"))

    /**
      * 获得去重身份证号数量
      */
    val certInfoDistinct: DataFrame = otherInfo
      .join(proInfo, otherInfo("product_code") === proInfo("product_codes"), "leftouter")
      .dropDuplicates(Array("insured_cert_no", "product_name"))
      .selectExpr("product_name", "insured_cert_no")
      .map(line => {
        val productName: String = line.getAs[String]("product_name")
        (productName, 1)
      })
      .reduceByKey(_ + _)
      .toDF("product_name_temp1", "cert_distinct_count")

    /**
      * 获得去重手机号数量
      */
    val mobileInfoDistinct: DataFrame = otherInfo
      .join(proInfo, otherInfo("product_code") === proInfo("product_codes"), "leftouter")
      .dropDuplicates(Array("insured_mobile", "product_name"))
      .selectExpr("product_name", "insured_mobile")
      .map(line => {
        val productName: String = line.getAs[String]("product_name")
        (productName, 1)
      })
      .reduceByKey(_ + _)
      .toDF("product_name_temp2", "mobile_distinct_count")

    /**
      * 合并表格
      */
    val res: DataFrame = proInfoDistinct
      .join(certInfoDistinct, proInfoDistinct("product_name") === certInfoDistinct("product_name_temp1"), "leftouter")
      .join(mobileInfoDistinct, proInfoDistinct("product_name") === mobileInfoDistinct("product_name_temp2"), "leftouter")
      .selectExpr("product_name", "cert_distinct_count", "mobile_distinct_count")

    res

  }

  /**
    * 获取 Mysql 表的数据
    * @param sqlContext
    * @return 返回 Mysql 表的 DataFrame
    */
  def readMysqlOtherTable(sqlContext: SQLContext): DataFrame = {
    import sqlContext.implicits._
    sqlContext.udf.register("dropEmptys", (line: String) => dropEmpty(line))
    sqlContext.udf.register("dropSpecial", (line: String) => dropSpecial(line))
    val properties: Properties = getProPerties()

    //    201711-201810
    val url1: String = "jdbc:mysql://172.16.11.103:3306/bzn_open_201711?tinyInt1isBit=false&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&allowMultiQueries=true&user=root&password=123456"
    val url2: String = "jdbc:mysql://172.16.11.103:3306/bzn_open_201712?tinyInt1isBit=false&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&allowMultiQueries=true&user=root&password=123456"
    val url3: String = "jdbc:mysql://172.16.11.103:3306/bzn_open_201801?tinyInt1isBit=false&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&allowMultiQueries=true&user=root&password=123456"
    val url4: String = "jdbc:mysql://172.16.11.103:3306/bzn_open_201802?tinyInt1isBit=false&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&allowMultiQueries=true&user=root&password=123456"
    val url5: String = "jdbc:mysql://172.16.11.103:3306/bzn_open_201803?tinyInt1isBit=false&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&allowMultiQueries=true&user=root&password=123456"
    val url6: String = "jdbc:mysql://172.16.11.103:3306/bzn_open_201804?tinyInt1isBit=false&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&allowMultiQueries=true&user=root&password=123456"
    val url7: String = "jdbc:mysql://172.16.11.103:3306/bzn_open_201805?tinyInt1isBit=false&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&allowMultiQueries=true&user=root&password=123456"
    val url8: String = "jdbc:mysql://172.16.11.103:3306/bzn_open_201806?tinyInt1isBit=false&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&allowMultiQueries=true&user=root&password=123456"
    val url9: String = "jdbc:mysql://172.16.11.103:3306/bzn_open_201807?tinyInt1isBit=false&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&allowMultiQueries=true&user=root&password=123456"
    val url10: String = "jdbc:mysql://172.16.11.103:3306/bzn_open_201808?tinyInt1isBit=false&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&allowMultiQueries=true&user=root&password=123456"
    val url11: String = "jdbc:mysql://172.16.11.103:3306/bzn_open_201809?tinyInt1isBit=false&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&allowMultiQueries=true&user=root&password=123456"
    val url12: String = "jdbc:mysql://172.16.11.103:3306/bzn_open_201810?tinyInt1isBit=false&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&allowMultiQueries=true&user=root&password=123456"

    //    201811-
    val url = "jdbc:mysql://172.16.11.103:3306/bzn_open_all?tinyInt1isBit=false&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&allowMultiQueries=true&user=root&password=123456"
    val predicates = Array[String]("month <= '2018-12-01'",
      "month > '2018-12-01' and month <= '2019-01-01'",
      "month > '2019-01-01' and month <= '2019-02-01'",
      "month > '2019-02-01' and month <= '2019-03-01'",
      "month > '2019-03-01' and month <= '2019-04-01'",
      "month > '2019-04-01' and month <= '2019-05-01'",
      "month > '2019-05-01' and month <= '2019-06-01'",
      "month > '2019-06-01' and month <= '2019-07-01'",
      "month > '2019-07-01' and month <= '2019-08-01'",
      "month > '2019-08-01'"
    )

    //    mysql 201711-201810
    val data01: DataFrame = sqlContext.read.jdbc(url1, "open_other_policy", properties)
      .selectExpr("insured_cert_no", "insured_cert_type", "insured_mobile", "product_code")
    val data02: DataFrame = sqlContext.read.jdbc(url2, "open_other_policy", properties)
      .selectExpr("insured_cert_no", "insured_cert_type", "insured_mobile", "product_code")
    val data03: DataFrame = sqlContext.read.jdbc(url3, "open_other_policy", properties)
      .selectExpr("insured_cert_no", "insured_cert_type", "insured_mobile", "product_code")
    val data04: DataFrame = sqlContext.read.jdbc(url4, "open_other_policy", properties)
      .selectExpr("insured_cert_no", "insured_cert_type", "insured_mobile", "product_code")
    val data05: DataFrame = sqlContext.read.jdbc(url5, "open_other_policy", properties)
      .selectExpr("insured_cert_no", "insured_cert_type", "insured_mobile", "product_code")
    val data06: DataFrame = sqlContext.read.jdbc(url6, "open_other_policy", properties)
      .selectExpr("insured_cert_no", "insured_cert_type", "insured_mobile", "product_code")
    val data07: DataFrame = sqlContext.read.jdbc(url7, "open_other_policy", properties)
      .selectExpr("insured_cert_no", "insured_cert_type", "insured_mobile", "product_code")
    val data08: DataFrame = sqlContext.read.jdbc(url8, "open_other_policy", properties)
      .selectExpr("insured_cert_no", "insured_cert_type", "insured_mobile", "product_code")
    val data09: DataFrame = sqlContext.read.jdbc(url9, "open_other_policy", properties)
      .selectExpr("insured_cert_no", "insured_cert_type", "insured_mobile", "product_code")
    val data10: DataFrame = sqlContext.read.jdbc(url10, "open_other_policy", properties)
      .selectExpr("insured_cert_no", "insured_cert_type", "insured_mobile", "product_code")
    val data11: DataFrame = sqlContext.read.jdbc(url11, "open_other_policy", properties)
      .selectExpr("insured_cert_no", "insured_cert_type", "insured_mobile", "product_code")
    val data12: DataFrame = sqlContext.read.jdbc(url12, "open_other_policy", properties)
      .selectExpr("insured_cert_no", "insured_cert_type", "insured_mobile", "product_code")

    //    mysql 201811-
    val data13: DataFrame = sqlContext
      .read
      .jdbc(url, "open_other_policy", predicates, properties)
      .selectExpr("insured_cert_no", "insured_cert_type", "insured_mobile", "product_code")

    //      合并
    val result: DataFrame = data13
      .unionAll(data12)
      .unionAll(data11)
      .unionAll(data10)
      .unionAll(data09)
      .unionAll(data08)
      .unionAll(data07)
      .unionAll(data06)
      .unionAll(data05)
      .unionAll(data04)
      .unionAll(data03)
      .unionAll(data02)
      .unionAll(data01)
      .where("insured_cert_type = 2 and length(insured_cert_no) = 18")
      .filter("dropSpecial(insured_cert_no) as insured_cert_no")
      .selectExpr("insured_cert_no", "insured_mobile", "product_code")

    result

  }

  /**
    * 获取 Mysql 表的数据
    * @param sqlContext
    * @return 返回 Mysql 表的 DataFrame
    */
  def readMysqlProTable(sqlContext: SQLContext): DataFrame = {
    import sqlContext.implicits._
    sqlContext.udf.register("dropEmptys", (line: String) => dropEmpty(line))
    sqlContext.udf.register("dropSpecial", (line: String) => dropSpecial(line))

    val url = "jdbc:mysql://172.16.11.105:3306/dwdb?tinyInt1isBit=false&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&allowMultiQueries=true&user=root&password=bzn@cdh123!"
    val properties: Properties = getProPerties()

    val result: DataFrame = sqlContext
      .read
      .jdbc(url, "dim_product", properties)
      .selectExpr("product_code as product_codes", "product_new_1 as product_name")

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
