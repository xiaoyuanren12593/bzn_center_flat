package bzn.other

import java.util.Properties

import bzn.job.common.Until
import bzn.ods.util.SparkUtil
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

object UserOfoTest extends SparkUtil with Until {

  def main(args: Array[String]): Unit = {

    //    初始化设置
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName: String = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "local[*]")

    val sc: SparkContext = sparkConf._2
    val hiveContext: HiveContext = sparkConf._4

    //    清晰标签
    val res: DataFrame = readMysqlOtherTable(hiveContext)
    res.printSchema()
//    res.rdd.repartition(1).saveAsTextFile("hdfs://172.16.11.106:8020/xing/data/monthOfo")

    sc.stop()

  }

  /**
    * 获取 Mysql 表的数据
    * @param sqlContext
    * @return 返回 Mysql 表的 DataFrame
    */
  def readMysqlOtherTable(sqlContext: SQLContext): DataFrame = {
    import sqlContext.implicits._
    sqlContext.udf.register("dropEmptys", (line: String) => dropSpecial(line))
    sqlContext.udf.register("dropSpecial", (line: String) => dropSpecial(line))
    val properties: Properties = getProPerties()

    //    201801-201810
    val url1: String = "jdbc:mysql://172.16.11.103:3306/bzn_open_201801?tinyInt1isBit=false&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&allowMultiQueries=true&user=root&password=123456"
    val url2: String = "jdbc:mysql://172.16.11.103:3306/bzn_open_201802?tinyInt1isBit=false&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&allowMultiQueries=true&user=root&password=123456"
    val url3: String = "jdbc:mysql://172.16.11.103:3306/bzn_open_201803?tinyInt1isBit=false&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&allowMultiQueries=true&user=root&password=123456"
    val url4: String = "jdbc:mysql://172.16.11.103:3306/bzn_open_201804?tinyInt1isBit=false&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&allowMultiQueries=true&user=root&password=123456"
    val url5: String = "jdbc:mysql://172.16.11.103:3306/bzn_open_201805?tinyInt1isBit=false&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&allowMultiQueries=true&user=root&password=123456"
    val url6: String = "jdbc:mysql://172.16.11.103:3306/bzn_open_201806?tinyInt1isBit=false&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&allowMultiQueries=true&user=root&password=123456"

    //    mysql 201711-201810
    val data01: DataFrame = sqlContext.read.jdbc(url1, "open_ofo_policy", properties)
      .where("insured_cert_type = 2 and length(insured_cert_no) = 18")
      .dropDuplicates(Array("insured_cert_type"))
      .selectExpr("month")
      .map(line => (line, 1))
      .reduceByKey(_ + _)
      .toDF("month", "num")

    val data02: DataFrame = sqlContext.read.jdbc(url2, "open_ofo_policy", properties)
      .where("insured_cert_type = 2 and length(insured_cert_no) = 18")
      .dropDuplicates(Array("insured_cert_type"))
      .selectExpr("month")
      .map(line => (line, 1))
      .reduceByKey(_ + _)
      .toDF("month", "num")

    val data03: DataFrame = sqlContext.read.jdbc(url3, "open_ofo_policy", properties)
      .where("insured_cert_type = 2 and length(insured_cert_no) = 18")
      .dropDuplicates(Array("insured_cert_type"))
      .selectExpr("month")
      .map(line => (line, 1))
      .reduceByKey(_ + _)
      .toDF("month", "num")

    val data04: DataFrame = sqlContext.read.jdbc(url4, "open_ofo_policy", properties)
      .where("insured_cert_type = 2 and length(insured_cert_no) = 18")
      .dropDuplicates(Array("insured_cert_type"))
      .selectExpr("month")
      .map(line => (line, 1))
      .reduceByKey(_ + _)
      .toDF("month", "num")

    val data05: DataFrame = sqlContext.read.jdbc(url5, "open_ofo_policy", properties)
      .where("insured_cert_type = 2 and length(insured_cert_no) = 18")
      .dropDuplicates(Array("insured_cert_type"))
      .selectExpr("month")
      .map(line => (line, 1))
      .reduceByKey(_ + _)
      .toDF("month", "num")

    val data06: DataFrame = sqlContext.read.jdbc(url6, "open_ofo_policy", properties)
      .where("insured_cert_type = 2 and length(insured_cert_no) = 18")
      .dropDuplicates(Array("insured_cert_type"))
      .selectExpr("month")
      .map(line => (line, 1))
      .reduceByKey(_ + _)
      .toDF("month", "num")

    val result: DataFrame = data01
      .unionAll(data02)
      .unionAll(data03)
      .unionAll(data04)
      .unionAll(data05)
      .unionAll(data06)

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
