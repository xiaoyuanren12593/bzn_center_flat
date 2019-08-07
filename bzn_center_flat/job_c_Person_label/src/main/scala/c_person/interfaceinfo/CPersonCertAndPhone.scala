package c_person.interfaceinfo

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import bzn.job.common.Until
import c_person.util.SparkUtil
import com.alibaba.fastjson.JSONObject
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable
import scala.io.Source

object CPersonCertAndPhone extends SparkUtil with Until {

  def main(args: Array[String]): Unit = {

    //    初始化设置
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf = sparkConfInfo(appName, "")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4

//    清洗
    val website: DataFrame = websiteInfo(hiveContext)
    val inter: DataFrame = interInfo(hiveContext)
    val other: DataFrame = otherInfo(hiveContext)

    val result: DataFrame = rinseData(hiveContext, website, inter, other)

//    保存到hive
    result.repartition(50).write.mode(SaveMode.Overwrite).saveAsTable("dwdb.dw_all_cert_no_and_mobile_detail_temp")

    sc.stop()

  }

  /**
    * 合并数据集
    * @param hiveContext
    * @param website
    * @param inter
    * @param other
    * @return
    */
  def rinseData(hiveContext: HiveContext, website: DataFrame, inter: DataFrame, other: DataFrame): DataFrame = {
    import hiveContext.implicits._
    hiveContext.udf.register("dropEmptys", (line: String) => dropEmpty(line))
    hiveContext.udf.register("dropSpecial", (line: String) => dropSpecial(line))
    hiveContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    hiveContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")//设置日期格式
      val date = df.format(new Date())// new Date()为获取当前系统时间
      (date + "")
    })

    val tempData: DataFrame = website
      .unionAll(inter)
      .unionAll(other)
      .filter("dropSpecial(cert_no) as cert_no")
      .selectExpr("cert_no", "mobile", "start_date", "end_date", "source_type")

    val resultInfo = tempData
      .map(line => {
        val certNo: String = line.getAs[String]("cert_no")
        val mobile: String = line.getAs[String]("mobile")
        val startDate: String = line.getAs[String]("start_date")
        val endDate: String = line.getAs[String]("end_date")
        val sourceType: String = line.getAs[String]("source_type")
//        更改时间
        val date: String = if (startDate != null) startDate else endDate
//        结果
        (certNo, (date, mobile, sourceType))
      })
      .aggregateByKey(mutable.ListBuffer[(String, String, String)]())(
        (List, value) => List += value,
        (List1, List2) => List1 ++= List2
      )
      .map(line => {
        val certNo: String = line._1
        val mobile: String = getMobile(line._2)
        val sourceType: String = getSource(line._2)
//        结果
        (certNo, mobile, sourceType)
      })
      .toDF("cert_no", "mobile", "source_type")
      .selectExpr("getUUID() as id", "cert_no", "mobile", "getNow() as dw_create_time", "source_type")

//      结果
    resultInfo

  }

  /**
    * 获取官网数据
    * @param hiveContext
    * @return
    */
  def websiteInfo(hiveContext: HiveContext): DataFrame = {
    import hiveContext.implicits._
    hiveContext.udf.register("getEmptyString", () => "")
    hiveContext.udf.register("dropNull", (line: String) => dropEmpty(line))
    hiveContext.udf.register("system", () => "system")

    /**
      * 读取holder的hive表
      */
    val holderInfoTemp: DataFrame = hiveContext.sql("select holder_cert_no, holder_cert_type, mobile, policy_id from odsdb.ods_holder_detail")
      .where("holder_cert_type = 1 and length(holder_cert_no) = 18")
      .selectExpr("holder_cert_no as cert_no", "mobile", "policy_id")

    /**
      * 读取policy的hive表
      */
    val policyInfo: DataFrame = hiveContext.sql("select policy_id, policy_start_date, policy_end_date from odsdb.ods_policy_detail")
      .selectExpr("policy_id as policy_id_temp", "policy_start_date as start_date", "policy_end_date as end_date")

//    获得投保人数据
    val holderInfo: DataFrame = holderInfoTemp
      .join(policyInfo, holderInfoTemp("policy_id") === policyInfo("policy_id_temp"))
      .selectExpr("cert_no", "mobile", "start_date", "end_date")
      .map(line => {
        val certNo: String = line.getAs[String]("cert_no")
        val mobile: String = line.getAs[String]("mobile")
        val startDate: Timestamp = line.getAs[java.sql.Timestamp]("start_date")
        val endDate: Timestamp = line.getAs[java.sql.Timestamp]("end_date")
//        更改时间数据格式
        val startDates: String = if (startDate != null) timeSubstring(startDate.toString) else null
        val endDates: String = if (endDate != null) timeSubstring(endDate.toString) else null
//        结果
        (certNo, mobile, startDates, endDates)
      })
      .toDF("cert_no", "mobile", "start_date", "end_date")

    /**
      * 读取insured的hive表
      */
    val insuredInfo: DataFrame = hiveContext.sql("select insured_cert_no, insured_cert_type, insured_mobile, start_date, end_date from odsdb.ods_policy_insured_detail")
      .where("insured_cert_type = '1' and length(insured_cert_no) = 18")
      .selectExpr("insured_cert_no as cert_no", "insured_mobile as mobile", "start_date", "end_date")

    /**
      * 读取slave的hive表
      */
    val slaveInfo: DataFrame = hiveContext.sql("select slave_cert_no, slave_cert_type, getEmptyString() as mobile, start_date, end_date from odsdb.ods_policy_insured_slave_detail")
      .where("slave_cert_type = '1' and length(slave_cert_no) = 18")
      .selectExpr("slave_cert_no as cert_no", "mobile", "start_date", "end_date")

//    合并表格并处理空
    val website: DataFrame = holderInfo
      .unionAll(insuredInfo)
      .unionAll(slaveInfo)
      .selectExpr("dropNull(cert_no) as cert_no", "dropNull(mobile) as mobile", "dropNull(start_date) as start_date",
        "dropNull(end_date) as end_date", "system() as source_type")

//    结果
    website

  }

  /**
    * 获得ofo和45数据
    * @param hiveContext
    * @return
    */
  def interInfo(hiveContext: HiveContext): DataFrame = {
    hiveContext.udf.register("dropEmptys", (line: String) => dropEmpty(line))
    hiveContext.udf.register("dropSpecial", (line: String) => dropSpecial(line))
    hiveContext.udf.register("ofo", () => "ofo")
    hiveContext.udf.register("express", () => "express")

    /**
      * 读取ofo接口的手机号信息
      */
    val ofoTelInfo: DataFrame = hiveContext.sql("select product_code, insured_cert_no, insured_mobile, start_date, end_date from odsdb_prd.open_ofo_policy_parquet")
      .where("product_code = 'OFO00002' and length(insured_cert_no) = 18")
      .selectExpr("insured_cert_no as cert_no", "insured_mobile as mobile", "start_date", "end_date", "ofo() as source_type")

    /**
      * 读取58速运接口的手机号信息
      */
    val suyunTelInfo: DataFrame = hiveContext.sql("select courier_card_no, courier_mobile, start_time, end_time from odsdb_prd.open_express_policy")
      .where("length(courier_card_no) = 18")
      .selectExpr("courier_card_no as cert_no", "courier_mobile as mobile", "start_time as start_date", "end_time as end_date", "express() as source_type")

//    合并表格
    val inter: DataFrame = ofoTelInfo
      .unionAll(suyunTelInfo)
      .selectExpr("dropNull(cert_no) as cert_no", "dropNull(mobile) as mobile", "dropNull(start_date) as start_date",
        "dropNull(end_date) as end_date", "source_type")

//    结果
    inter

  }

  /**
    * 获得other数据
    * @param hiveContext
    * @return
    */
  def otherInfo(hiveContext: HiveContext): DataFrame = {
    import hiveContext.implicits._
    hiveContext.udf.register("dropEmptys", (line: String) => dropEmpty(line))
    hiveContext.udf.register("dropSpecial", (line: String) => dropSpecial(line))
    hiveContext.udf.register("other", () => "other")
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

    //    分区201811-
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
    val data01: DataFrame = hiveContext.read.jdbc(url1, "open_other_policy", properties)
      .selectExpr("insured_cert_no", "insured_cert_type", "insured_mobile", "start_date", "end_date")
    val data02: DataFrame = hiveContext.read.jdbc(url2, "open_other_policy", properties)
      .selectExpr("insured_cert_no", "insured_cert_type", "insured_mobile", "start_date", "end_date")
    val data03: DataFrame = hiveContext.read.jdbc(url3, "open_other_policy", properties)
      .selectExpr("insured_cert_no", "insured_cert_type", "insured_mobile", "start_date", "end_date")
    val data04: DataFrame = hiveContext.read.jdbc(url4, "open_other_policy", properties)
      .selectExpr("insured_cert_no", "insured_cert_type", "insured_mobile", "start_date", "end_date")
    val data05: DataFrame = hiveContext.read.jdbc(url5, "open_other_policy", properties)
      .selectExpr("insured_cert_no", "insured_cert_type", "insured_mobile", "start_date", "end_date")
    val data06: DataFrame = hiveContext.read.jdbc(url6, "open_other_policy", properties)
      .selectExpr("insured_cert_no", "insured_cert_type", "insured_mobile", "start_date", "end_date")
    val data07: DataFrame = hiveContext.read.jdbc(url7, "open_other_policy", properties)
      .selectExpr("insured_cert_no", "insured_cert_type", "insured_mobile", "start_date", "end_date")
    val data08: DataFrame = hiveContext.read.jdbc(url8, "open_other_policy", properties)
      .selectExpr("insured_cert_no", "insured_cert_type", "insured_mobile", "start_date", "end_date")
    val data09: DataFrame = hiveContext.read.jdbc(url9, "open_other_policy", properties)
      .selectExpr("insured_cert_no", "insured_cert_type", "insured_mobile", "start_date", "end_date")
    val data10: DataFrame = hiveContext.read.jdbc(url10, "open_other_policy", properties)
      .selectExpr("insured_cert_no", "insured_cert_type", "insured_mobile", "start_date", "end_date")
    val data11: DataFrame = hiveContext.read.jdbc(url11, "open_other_policy", properties)
      .selectExpr("insured_cert_no", "insured_cert_type", "insured_mobile", "start_date", "end_date")
    val data12: DataFrame = hiveContext.read.jdbc(url12, "open_other_policy", properties)
      .selectExpr("insured_cert_no", "insured_cert_type", "insured_mobile", "start_date", "end_date")

    //    mysql 201811-
    val data13: DataFrame = hiveContext
      .read
      .jdbc(url, "open_other_policy", predicates, properties)
      .selectExpr("insured_cert_no", "insured_cert_type", "insured_mobile", "start_date", "end_date")

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
      .map(line => {
        val certNo: String = line.getAs[String]("insured_cert_no")
        val mobile: String = line.getAs[String]("insured_mobile")
        val startDate: Timestamp = line.getAs[java.sql.Timestamp]("start_date")
        val endDate: Timestamp = line.getAs[java.sql.Timestamp]("end_date")
        //        更改时间数据格式
        val startDates: String = if (startDate != null) timeSubstring(startDate.toString) else null
        val endDates: String = if (endDate != null) timeSubstring(endDate.toString) else null
        //        结果
        (certNo, mobile, startDates, endDates)
      })
      .toDF("cert_no", "mobile", "start_date", "end_date")
      .selectExpr("dropNull(cert_no) as cert_no", "dropNull(mobile) as mobile", "dropNull(start_date) as start_date",
        "dropNull(end_date) as end_date", "other() as source_type")

//    结果
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

  /**
    * 合并手机号
    * @param list
    * @return
    */
  def getMobile(list: mutable.ListBuffer[(String, String, String)]): String = {
    val mobileJSON = new JSONObject(true)
//    去除重复值，留下时间最大的,然后排序
    val result: Seq[(String, String)] = list
      .filter(value => (value._1 != null && value._2 != null))
      .map(value => (value._2, value._1))
      .groupBy(_._1)
      .map(value => value._2.max)
      .toSeq
      .sortBy(_._2)
      .reverse

    //    循环放入JSON
    if (!result.isEmpty) for (r <- result) mobileJSON.put(r._1, r._2)

    //    如果集合为空存null，否则存JSON
    if (!result.isEmpty) mobileJSON.toString else null

  }

  /**
    * 合并来源
    * @param list
    * @return
    */
  def getSource(list: mutable.ListBuffer[(String, String, String)]): String = {
    //    去空、去重
    val set: Seq[String] = list
      .map(line => line._3)
      .toSet
      .toSeq
      .sorted

    //    循环遍历
    set.mkString(":")
  }

}
