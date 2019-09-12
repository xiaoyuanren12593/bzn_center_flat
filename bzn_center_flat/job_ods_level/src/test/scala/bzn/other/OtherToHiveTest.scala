package bzn.other

import java.util.Properties

import bzn.job.common.Until
import bzn.ods.util.SparkUtil
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

object OtherToHiveTest extends SparkUtil with Until {

  def main(args: Array[String]): Unit = {

//    初始化设置
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName: String = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "local[*]")

    val sc: SparkContext = sparkConf._2
    val hiveContext: HiveContext = sparkConf._4

//    数据迁移
    val otherDetail: DataFrame = readMysqlOtherTable(hiveContext)
//    otherDetail.write.mode(SaveMode.Append).partitionBy("month").saveAsTable("ods_open_other_policy_detail")
    otherDetail.printSchema()

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
      "month > '2019-08-01' and month <= '2019-09-01'",
      "month > '2019-09-01'"
    )

    //    mysql 201711-201810
    val data01: DataFrame = sqlContext.read.jdbc(url1, "open_other_policy", properties)
    val data02: DataFrame = sqlContext.read.jdbc(url2, "open_other_policy", properties)
    val data03: DataFrame = sqlContext.read.jdbc(url3, "open_other_policy", properties)
    val data04: DataFrame = sqlContext.read.jdbc(url4, "open_other_policy", properties)
    val data05: DataFrame = sqlContext.read.jdbc(url5, "open_other_policy", properties)
    val data06: DataFrame = sqlContext.read.jdbc(url6, "open_other_policy", properties)
    val data07: DataFrame = sqlContext.read.jdbc(url7, "open_other_policy", properties)
    val data08: DataFrame = sqlContext.read.jdbc(url8, "open_other_policy", properties)
    val data09: DataFrame = sqlContext.read.jdbc(url9, "open_other_policy", properties)
    val data10: DataFrame = sqlContext.read.jdbc(url10, "open_other_policy", properties)
    val data11: DataFrame = sqlContext.read.jdbc(url11, "open_other_policy", properties)
    val data12: DataFrame = sqlContext.read.jdbc(url12, "open_other_policy", properties)

    //    mysql 201811-
    val data13: DataFrame = sqlContext
      .read
      .jdbc(url, "open_other_policy", predicates, properties)

    //      合并
    val result: DataFrame = data01
      .unionAll(data02)
      .unionAll(data03)
      .unionAll(data04)
      .unionAll(data05)
      .unionAll(data06)
      .unionAll(data07)
      .selectExpr("policy_id", "proposal_no", "policy_no", "batch_id", "user_id", "product_code", "order_id", "start_date",
        "end_date", "holder_name", "holder_ename", "holder_first_name", "holder_last_name", "holder_mobile", "holder_cert_type",
        "holder_cert_no", "holder_email", "holder_birth_day", "holder_sex", "holder_industry", "insured_name", "insured_ename",
        "insured_first_name", "insured_last_name", "insured_mobile", "insured_cert_type", "insured_cert_no", "insured_holder_relation",
        "insured_email", "insured_birth_day", "insured_sex", "insured_industry", "status", "export_status", "extend_key1",
        "extend_key2", "extend_key3", "extend_key4", "extend_key5", "create_time", "update_time", "month", "dropEmptys(getEmpty()) as province_code",
        "dropEmptys(getEmpty()) as city_code", "dropEmptys(getEmpty()) as area_code", "dropEmptys(getEmpty()) as address",
        "dropEmptys(getEmpty()) as amount", "dropEmptys(getEmpty()) as premium", "dropEmptys(getEmpty()) as insured_time")
      .unionAll(data08)
      .unionAll(data09)
      .unionAll(data10)
      .unionAll(data11)
      .unionAll(data12)
      .unionAll(data13)
      .selectExpr("policy_id", "proposal_no", "policy_no", "batch_id", "user_id", "product_code", "order_id", "start_date",
        "end_date", "holder_name", "holder_ename", "holder_first_name", "holder_last_name", "holder_mobile", "holder_cert_type",
        "holder_cert_no", "holder_email", "cast(holder_birth_day as string) as holder_birth_day", "holder_sex", "holder_industry", "insured_name", "insured_ename",
        "insured_first_name", "insured_last_name", "insured_mobile", "insured_cert_type", "insured_cert_no", "insured_holder_relation",
        "insured_email", "cast(insured_birth_day  as string) as insured_birth_day", "insured_sex", "insured_industry", "status", "export_status", "extend_key1",
        "extend_key2", "extend_key3", "extend_key4", "extend_key5", "create_time", "update_time", "cast(month as string) as month", "province_code",
        "city_code", "area_code", "address", "cast(amount as decimal(14,4)) as amount", "cast(premium as decimal(14,4)) as premium", "insured_time")

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
