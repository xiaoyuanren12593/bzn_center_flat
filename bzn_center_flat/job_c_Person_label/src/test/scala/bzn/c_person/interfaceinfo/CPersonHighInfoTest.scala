package bzn.c_person.interfaceinfo

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Date, Properties}
import java.util.regex.Pattern

import bzn.c_person.util.SparkUtil
import bzn.job.common.{HbaseUtil, Until}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source

object CPersonHighInfoTest extends SparkUtil with Until with HbaseUtil{

  def main(args: Array[String]): Unit = {

    //    初始化设置
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName: String = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "local[*]")

    val sc: SparkContext = sparkConf._2
    val hiveContext: HiveContext = sparkConf._4

    //    清洗标签
    val peopleInfo: DataFrame = readMysqlOtherTable(hiveContext)
    val productInfo: DataFrame = readMysqlProTable(hiveContext)

    peopleInfo.cache()
    productInfo.cache()

    val certInfo: DataFrame = getCertInfo(hiveContext, peopleInfo)
    val coxcombry: DataFrame = getCoxcombry(hiveContext, peopleInfo, productInfo)
    val wedding: DataFrame = getWedding(hiveContext, peopleInfo, productInfo)
    val ride: DataFrame = getRideInfo(hiveContext, peopleInfo, productInfo)
    val onlineCar: DataFrame = getOnlineCar(hiveContext, peopleInfo, productInfo)
    val partTimeNums: DataFrame = getPartNums(hiveContext, peopleInfo, productInfo)
    val house: DataFrame = getHouse(hiveContext, peopleInfo, productInfo)
    val amplitudeFrequenter = getamplitudeFrequenter(hiveContext, peopleInfo, productInfo)

    val allData: DataFrame = unionAll(hiveContext, certInfo, coxcombry, wedding, ride, onlineCar, partTimeNums, house, amplitudeFrequenter)

    peopleInfo.show()

    sc.stop()

  }

  /**
    * 获得接口全部身份证信息
    * @param hiveContext
    * @param peopleInfo
    * @return 身份证DataFrame
    */
  def getCertInfo(hiveContext: HiveContext, peopleInfo: DataFrame): DataFrame = {
    import hiveContext.implicits._
    hiveContext.udf.register("dropEmptys", (line: String) => dropEmpty(line))
    hiveContext.udf.register("dropSpecial", (line: String) => dropSpecial(line))

    /**
      * 读取MySQL中的other表
      */
    val certNo: DataFrame = peopleInfo
      .selectExpr("high_cert_no")
      .dropDuplicates(Array("high_cert_no"))

//    结果
    certNo

  }

  /**
    * 获得isCoxcombry标签
    * @param hiveContext
    * @param peopleInfo
    * @param productInfo
    * @return
    */
  def getCoxcombry(hiveContext: HiveContext, peopleInfo: DataFrame, productInfo: DataFrame): DataFrame = {
    import hiveContext.implicits._
    hiveContext.udf.register("dropEmptys", (line: String) => dropEmpty(line))
    hiveContext.udf.register("dropSpecial", (line: String) => dropSpecial(line))

    /**
      * 读取MySQL中的other表
      */
    val peopleInfos: DataFrame = peopleInfo
      .selectExpr("high_cert_no", "product_code")
      .dropDuplicates(Array("high_cert_no", "product_code"))

    //    清洗is_coxcombry标签
    val result: DataFrame = peopleInfos
      .join(productInfo, peopleInfos("product_code") === productInfo("product_codes"), "leftouter")
      .selectExpr("high_cert_no", "product_desc")
      .map(line => {
        val highCertNo: String = line.getAs[String]("high_cert_no")
        val productDesc: String = line.getAs[String]("product_desc")
//        结果
        (highCertNo, productDesc)
      })
      .aggregateByKey(mutable.ListBuffer[String]())(
        seqOp = (List: ListBuffer[String], value: String) => List += value,
        combOp = (List1: mutable.ListBuffer[String], List2: ListBuffer[String]) => List1 ++= List2
      )
      .map(line => {
        val highCertNo: String = line._1
        val isCoxcombry: String = if (line._2.contains("新氧医美")) "是" else null
//        结果
        (highCertNo, isCoxcombry)
      })
      .filter(line => line._2 != null)
      .toDF("high_cert_no", "is_coxcombry")

//    结果
    result

  }

  /**
    * 获得wedding_month标签
    * @param hiveContext
    * @param peopleInfo
    * @param productInfo
    * @return
    */
  def getWedding(hiveContext: HiveContext, peopleInfo: DataFrame, productInfo: DataFrame): DataFrame = {
    import hiveContext.implicits._
    hiveContext.udf.register("dropEmptys", (line: String) => dropEmpty(line))
    hiveContext.udf.register("dropSpecial", (line: String) => dropSpecial(line))

    /**
      * 读取MySQL中的other表
      */
    val peopleInfos: DataFrame = peopleInfo
      .selectExpr("high_cert_no", "product_code", "start_date")

//    清洗wedding_month标签
    val result: DataFrame = peopleInfos
      .join(productInfo, peopleInfos("product_code") === productInfo("product_codes"), "leftouter")
      .selectExpr("high_cert_no", "product_desc", "start_date")
      .map(f = line => {
        val highCertNo: String = line.getAs[String]("high_cert_no")
        val productDesc: String = line.getAs[String]("product_desc")
        val startDate: Timestamp = line.getAs[java.sql.Timestamp]("start_date")
//        结果
        (highCertNo, (productDesc, startDate))
      })
      .filter(line => line._2._1 == "婚礼纪-婚礼保")
      .reduceByKey((x, y) => if (x._2.compareTo(y._2) <= 0) x else y)
      .map(line => {
        val highCertNo: String = line._1
        val weddingMonth: String = line._2._2.toString.split(" ")(0).split("-")(1)
//        结果
        (highCertNo, weddingMonth)
      })
      .toDF("high_cert_no", "wedding_month")

//    结果
    result

  }

  /**
    * 获得出行信息
    * @param hiveContext
    * @param peopleInfo
    * @param productInfo
    * @return
    */
  def getRideInfo(hiveContext: HiveContext, peopleInfo: DataFrame, productInfo: DataFrame): DataFrame = {
    import hiveContext.implicits._
    hiveContext.udf.register("dropEmptys", (line: String) => dropEmpty(line))
    hiveContext.udf.register("dropSpecial", (line: String) => dropSpecial(line))

    /**
      * 读取MySQL中的other表
      */
    val peopleInfos: DataFrame = peopleInfo
      .selectExpr("high_cert_no", "product_code", "start_date")

    //    清洗ride标签
    val result: DataFrame = peopleInfos
      .join(productInfo, peopleInfos("product_code") === productInfo("product_codes"), "leftouter")
      .selectExpr("high_cert_no", "product_desc", "start_date")
      .map(f = line => {
        val highCertNo: String = line.getAs[String]("high_cert_no")
        val productDesc: String = line.getAs[String]("product_desc")
        val startDate: Timestamp = line.getAs[java.sql.Timestamp]("start_date")
//        清洗中间字段
        val date: String = startDate.toString.split(" ")(0)
        val hour: String = startDate.toString.split(" ")(1).split(":")(0)
        val week: String = getWeekOfDate(date).toString
//        获取当前时间
        val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val dateStr: String = sdf.format(new Date())
        val nintyDay: String = dateDelNintyDay(dateStr).split(" ")(0)
        //        结果
        (highCertNo, (productDesc, date, hour, week, nintyDay))
      })
      .filter(line => {
        line._2._1 == "星驾单车" || line._2._1 == "七彩单车" || line._2._1 == "闪骑电单车" || line._2._1 == "DDbike" ||
          line._2._1 == "酷骑单车" || line._2._1 == "小鹿单车" || line._2._1 == "骑迹单车" || line._2._1 == "OFO" ||
          line._2._1 == "便利蜂单车" || line._2._1 == "飞鸽出行" || line._2._1 == "骑迹单车" || line._2._1 == "西游单车" ||
          line._2._1 == "景智单车"
      })
      .aggregateByKey(mutable.ListBuffer[(String, String, String, String, String)]())(
        (List: ListBuffer[(String, String, String, String, String)], value: (String, String, String, String, String)) => List += value,
        (List1: ListBuffer[(String, String, String, String, String)], List2: ListBuffer[(String, String, String, String, String)]) => List1 ++= List2
      )
      .map(line => {
        val highCertNo: String = line._1
        val rideDay: String = rideDays(line._2)
        val maxRideTimeStep: String = rideTimeStep(line._2)
        val maxRideBrand: String = rideBrand(line._2)
        val maxRideDate: String = rideDate(line._2)
        val tripRates: String = tripRate(line._2)
        val internalClocks: String = internalClock(line._2)
        val allRideTimeSteps: String = allRideTimeStep(line._2)
        val allRideBrands: String = allRideBrand(line._2)
        val allRideDates: String = allRideDate(line._2)
//        结果
        (highCertNo, rideDay, maxRideTimeStep, maxRideBrand, maxRideDate, tripRates, internalClocks, allRideTimeSteps,
        allRideBrands, allRideDates)
      })
      .toDF("high_cert_no", "ride_days", "max_ride_time_step", "max_ride_brand", "max_ride_date", "trip_rate",
        "internal_clock", "all_ride_time_step", "all_ride_brand", "all_ride_date")

//    结果
    result

  }

  /**
    * 获得is_online_car标签
    * @param hiveContext
    * @param peopleInfo
    * @param productInfo
    * @return
    */
  def getOnlineCar(hiveContext: HiveContext, peopleInfo: DataFrame, productInfo: DataFrame): DataFrame = {
    import hiveContext.implicits._
    hiveContext.udf.register("dropEmptys", (line: String) => dropEmpty(line))
    hiveContext.udf.register("dropSpecial", (line: String) => dropSpecial(line))

    /**
      * 读取MySQL中的other表
      */
    val peopleInfos: DataFrame = peopleInfo
      .selectExpr("high_cert_no", "product_code")
      .dropDuplicates(Array("high_cert_no", "product_code"))

    //    清洗is_online_car标签
    val result: DataFrame = peopleInfos
      .join(productInfo, peopleInfos("product_code") === productInfo("product_codes"), "leftouter")
      .selectExpr("high_cert_no", "product_desc")
      .map(line => {
        val highCertNo: String = line.getAs[String]("high_cert_no")
        val productDesc: String = line.getAs[String]("product_desc")
        //        结果
        (highCertNo, productDesc)
      })
      .aggregateByKey(mutable.ListBuffer[String]())(
        (List: ListBuffer[String], value: String) => List += value,
        (List1: ListBuffer[String], List2: ListBuffer[String]) => List1 ++= List2
      )
      .map(line => {
        val highCertNo: String = line._1
        val isOnlineCar: String = if (line._2.contains("曹操意外险")) "是" else null
        //        结果
        (highCertNo, isOnlineCar)
      })
      .toDF("high_cert_no", "is_online_car")

    //    结果
    result

  }

  /**
    * 获得part_time_nums标签
    * @param hiveContext
    * @param peopleInfo
    * @param productInfo
    * @return
    */
  def getPartNums(hiveContext: HiveContext, peopleInfo: DataFrame, productInfo: DataFrame): DataFrame = {
    import hiveContext.implicits._
    hiveContext.udf.register("dropEmptys", (line: String) => dropEmpty(line))
    hiveContext.udf.register("dropSpecial", (line: String) => dropSpecial(line))

    /**
      * 读取MySQL中的other表
      */
    val peopleInfos: DataFrame = peopleInfo
      .selectExpr("high_cert_no", "product_code")
      .dropDuplicates(Array("high_cert_no", "product_code"))

    //    清洗part_time_nums标签
    val result: DataFrame = peopleInfos
      .join(productInfo, peopleInfos("product_code") === productInfo("product_codes"), "leftouter")
      .selectExpr("high_cert_no", "product_desc")
      .map(line => {
        val highCertNo: String = line.getAs[String]("high_cert_no")
        val productDesc: String = line.getAs[String]("product_desc")
        //        结果
        (highCertNo, productDesc)
      })
      .filter(line => {
        (line._2 == "斗米兼职" || line._2 == "万能小哥" || line._2 == "弧聚网络" || line._2 == "找活儿兼职" || line._2 == "第二职场" ||
          line._2 == "蒲公英" || line._2 == "独立日")
      })
      .aggregateByKey(mutable.ListBuffer[String]())(
        (List: ListBuffer[String], value: String) => List += value,
        (List1: ListBuffer[String], List2: ListBuffer[String]) => List1 ++= List2
      )
      .map(line => {
        val highCertNo: String = line._1
        val partTimeNums: String = line._2.size.toString
        //        结果
        (highCertNo, partTimeNums)
      })
      .toDF("high_cert_no", "part_time_nums")

    //    结果
    result

  }

  /**
    * 获得is_h_house标签
    * @param hiveContext
    * @param peopleInfo
    * @param productInfo
    * @return
    */
  def getHouse(hiveContext: HiveContext, peopleInfo: DataFrame, productInfo: DataFrame): DataFrame = {
    import hiveContext.implicits._
    hiveContext.udf.register("dropEmptys", (line: String) => dropEmpty(line))
    hiveContext.udf.register("dropSpecial", (line: String) => dropSpecial(line))

    /**
      * 读取MySQL中的other表
      */
    val peopleInfos: DataFrame = peopleInfo
      .selectExpr("high_cert_no", "product_code")
      .dropDuplicates(Array("high_cert_no", "product_code"))

    //    清洗is_h_house标签
    val result: DataFrame = peopleInfos
      .join(productInfo, peopleInfos("product_code") === productInfo("product_codes"), "leftouter")
      .selectExpr("high_cert_no", "product_desc")
      .map(line => {
        val highCertNo: String = line.getAs[String]("high_cert_no")
        val productDesc: String = line.getAs[String]("product_desc")
        //        结果
        (highCertNo, productDesc)
      })
      .aggregateByKey(mutable.ListBuffer[String]())(
        (List: ListBuffer[String], value: String) => List += value,
        (List1: ListBuffer[String], List2: ListBuffer[String]) => List1 ++= List2
      )
      .map(line => {
        val highCertNo: String = line._1
        val ishHouse: String = if (line._2.contains("侃家")) "是" else null
        //        结果
        (highCertNo, ishHouse)
      })
      .toDF("high_cert_no", "is_h_house")

    //    结果
    result

  }

  /**
    * 获得amplitude_frequenter 标签
    * @param hiveContext
    * @param peopleInfo
    * @param productInfo
    * @return
    */
  def getamplitudeFrequenter (hiveContext: HiveContext, peopleInfo: DataFrame, productInfo: DataFrame): DataFrame = {
    import hiveContext.implicits._
    hiveContext.udf.register("dropEmptys", (line: String) => dropEmpty(line))
    hiveContext.udf.register("dropSpecial", (line: String) => dropSpecial(line))

    /**
      * 读取MySQL中的other表
      */
    val peopleInfos: DataFrame = peopleInfo
      .selectExpr("high_cert_no", "product_code")

    //    清洗is_h_house标签
    val result: DataFrame = peopleInfos
      .join(productInfo, peopleInfos("product_code") === productInfo("product_codes"), "leftouter")
      .selectExpr("high_cert_no", "product_desc")
      .map(line => {
        val highCertNo: String = line.getAs[String]("high_cert_no")
        val productDesc: String = line.getAs[String]("product_desc")
        //        结果
        (highCertNo, productDesc)
      })
      .filter(line => {
        if (line._2 != null) {
          if (line._2.contains("青芒果方案1")) true else false
        } else false
      })
      .aggregateByKey(mutable.ListBuffer[String]())(
        (List: ListBuffer[String], value: String) => List += value,
        (List1: ListBuffer[String], List2: ListBuffer[String]) => List1 ++= List2
      )
      .map(line => {
        val highCertNo: String = line._1
        val amplitudeFrequenter: String = if (line._2.size >= 12) "是" else null
        //        结果
        (highCertNo, amplitudeFrequenter)
      })
      .toDF("high_cert_no", "amplitude_frequenter")

    //    结果
    result

  }

  def unionAll(hiveContext: HiveContext, certInfo: DataFrame, coxcombry: DataFrame, wedding: DataFrame, ride: DataFrame, onlineCar: DataFrame,
               partTimeNums: DataFrame, house: DataFrame, amplitudeFrequenter: DataFrame): DataFrame = {
    import hiveContext.implicits._
    hiveContext.udf.register("dropEmptys", (line: String) => dropEmpty(line))
    hiveContext.udf.register("dropSpecial", (line: String) => dropSpecial(line))

    val coxcombrys: DataFrame = coxcombry.withColumnRenamed("high_cert_no", "co_cert_no")
    val weddings: DataFrame = wedding.withColumnRenamed("high_cert_no", "we_cert_no")
    val rides: DataFrame = ride.withColumnRenamed("high_cert_no", "ri_cert_no")
    val onlineCars: DataFrame = onlineCar.withColumnRenamed("high_cert_no", "on_cert_no")
    val partTimeNumss: DataFrame = partTimeNums.withColumnRenamed("high_cert_no", "pa_cert_no")
    val houses: DataFrame = house.withColumnRenamed("high_cert_no", "ho_cert_no")
    val amplitudeFrequenters = amplitudeFrequenter.withColumnRenamed("high_cert_no", "am_cert_no")

//    unionAll
    val result: DataFrame = certInfo
      .join(coxcombrys, certInfo("high_cert_no") === coxcombrys("co_cert_no"), "leftouter")
      .join(weddings, certInfo("high_cert_no") === weddings("we_cert_no"), "leftouter")
      .join(rides, certInfo("high_cert_no") === rides("ri_cert_no"), "leftouter")
      .join(onlineCars, certInfo("high_cert_no") === onlineCars("on_cert_no"), "leftouter")
      .join(partTimeNumss, certInfo("high_cert_no") === partTimeNumss("pa_cert_no"), "leftouter")
      .join(houses, certInfo("high_cert_no") === houses("ho_cert_no"), "leftouter")
      .join(amplitudeFrequenters, certInfo("high_cert_no") === amplitudeFrequenters("am_cert_no"), "leftouter")
      .selectExpr("high_cert_no", "dropEmptys(is_coxcombry) as is_coxcombry", "dropEmptys(wedding_month) as wedding_month",
      "dropEmptys(ride_days) as ride_days", "dropEmptys(max_ride_time_step) as max_ride_time_step",
      "dropEmptys(max_ride_brand) as max_ride_brand", "dropEmptys(max_ride_date) as max_ride_date",
      "dropEmptys(trip_rate) as trip_rate", "dropEmptys(internal_clock) as internal_clock",
      "dropEmptys(all_ride_time_step) as all_ride_time_step", "dropEmptys(all_ride_brand) as all_ride_brand",
      "dropEmptys(all_ride_date) as all_ride_date", "dropEmptys(is_online_car) as is_online_car",
      "dropEmptys(part_time_nums) as part_time_nums", "dropEmptys(is_h_house) as is_h_house", "dropEmptys(amplitude_frequenter) as amplitude_frequenter")

//    结果
    result

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
      "month > '2019-07-01'"
    )

    //    mysql 201711-201810
    val data01: DataFrame = sqlContext.read.jdbc(url1, "open_other_policy", properties)
      .selectExpr("insured_cert_no", "insured_cert_type", "product_code", "start_date")
    val data02: DataFrame = sqlContext.read.jdbc(url2, "open_other_policy", properties)
      .selectExpr("insured_cert_no", "insured_cert_type", "product_code", "start_date")
    val data03: DataFrame = sqlContext.read.jdbc(url3, "open_other_policy", properties)
      .selectExpr("insured_cert_no", "insured_cert_type", "product_code", "start_date")
    val data04: DataFrame = sqlContext.read.jdbc(url4, "open_other_policy", properties)
      .selectExpr("insured_cert_no", "insured_cert_type", "product_code", "start_date")
    val data05: DataFrame = sqlContext.read.jdbc(url5, "open_other_policy", properties)
      .selectExpr("insured_cert_no", "insured_cert_type", "product_code", "start_date")
    val data06: DataFrame = sqlContext.read.jdbc(url6, "open_other_policy", properties)
      .selectExpr("insured_cert_no", "insured_cert_type", "product_code", "start_date")
    val data07: DataFrame = sqlContext.read.jdbc(url7, "open_other_policy", properties)
      .selectExpr("insured_cert_no", "insured_cert_type", "product_code", "start_date")
    val data08: DataFrame = sqlContext.read.jdbc(url8, "open_other_policy", properties)
      .selectExpr("insured_cert_no", "insured_cert_type", "product_code", "start_date")
    val data09: DataFrame = sqlContext.read.jdbc(url9, "open_other_policy", properties)
      .selectExpr("insured_cert_no", "insured_cert_type", "product_code", "start_date")
    val data10: DataFrame = sqlContext.read.jdbc(url10, "open_other_policy", properties)
      .selectExpr("insured_cert_no", "insured_cert_type", "product_code", "start_date")
    val data11: DataFrame = sqlContext.read.jdbc(url11, "open_other_policy", properties)
      .selectExpr("insured_cert_no", "insured_cert_type", "product_code", "start_date")
    val data12: DataFrame = sqlContext.read.jdbc(url12, "open_other_policy", properties)
      .selectExpr("insured_cert_no", "insured_cert_type", "product_code", "start_date")

    //    mysql 201811-
    val data13: DataFrame = sqlContext
      .read
      .jdbc(url, "open_other_policy", predicates, properties)
      .selectExpr("insured_cert_no", "insured_cert_type", "product_code", "start_date")

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
      .selectExpr("insured_cert_no as high_cert_no", "insured_cert_type", "product_code", "start_date")
      .where("insured_cert_type = 2 and length(high_cert_no) = 18")
      .map(line => {
        val highCertNo: String = line.getAs[String]("high_cert_no")
        val productCode: String = line.getAs[String]("product_code")
        val startDate: Timestamp = line.getAs[java.sql.Timestamp]("start_date")
        //        结果
        (highCertNo, productCode, startDate)
      })
      .filter(line => {dropSpecial(line._1) && line._3 != null})
      .toDF("high_cert_no", "product_code", "start_date")

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
      .selectExpr("product_code as product_codes", "product_desc")

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
