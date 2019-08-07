package bzn.c_person.interfaceinfo

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import bzn.c_person.util.SparkUtil
import bzn.job.common.{HbaseUtil, Until}
import com.alibaba.fastjson
import com.alibaba.fastjson.JSON
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.parsing.json.JSONObject

object CPersonHighInfoIncrementTest extends SparkUtil with Until with HbaseUtil{

  def main(args: Array[String]): Unit = {

    //    初始化设置
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName: String = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "local[*]")

    val sc: SparkContext = sparkConf._2
    val hiveContext: HiveContext = sparkConf._4

  }

  /**
    * 更新is_coxcombry标签
    * @param hiveContext
    * @param peopleInfo
    * @param productInfo
    * @param hbaseInfo
    */
  def updateCoxcombry(hiveContext: HiveContext, peopleInfo: DataFrame, productInfo: DataFrame, hbaseInfo: DataFrame): Unit = {
    import hiveContext.implicits._
    hiveContext.udf.register("dropEmptys", (line: String) => dropEmpty(line))
    hiveContext.udf.register("dropSpecial", (line: String) => dropSpecial(line))
    hiveContext.udf.register("getNew", (line: String) => {
      if (line == null) true else false
    })

    /**
      * 读取MySQL中的other表
      */
    val peopleInfos: DataFrame = peopleInfo
      .selectExpr("high_cert_no", "product_code")
      .dropDuplicates(Array("high_cert_no", "product_code"))

    //    清洗is_coxcombry标签
    val newResult: DataFrame = peopleInfos
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
      .toDF("high_cert_no", "new_coxcombry")

    /**
      * hbase中的标签
      */
    val oldResult: DataFrame = hbaseInfo
      .selectExpr("cert_no", "is_coxcombry as old_coxcombry")

//    整理新旧标签
    val result: DataFrame = newResult
      .join(oldResult, newResult("high_cert_no") === oldResult("cert_no"), "leftouter")
      .filter("getNew(cert_no) as cert_no")
      .selectExpr("high_cert_no", "new_coxcombry as is_coxcombry")

//      结果
    toHBase(result, "label_person", "high_info", "high_cert_no")

  }

  /**
    * 更新wedding_month信息
    * @param hiveContext
    * @param peopleInfo
    * @param productInfo
    * @param hbaseInfo
    */
  def updateWeddingMongth(hiveContext: HiveContext, peopleInfo: DataFrame, productInfo: DataFrame, hbaseInfo: DataFrame): Unit = {
    import hiveContext.implicits._
    hiveContext.udf.register("dropEmptys", (line: String) => dropEmpty(line))
    hiveContext.udf.register("dropSpecial", (line: String) => dropSpecial(line))
    hiveContext.udf.register("getNew", (line: String) => {
      if (line == null) true else false
    })

    /**
      * 读取MySQL中的other表
      */
    val peopleInfos: DataFrame = peopleInfo
      .selectExpr("high_cert_no", "product_code", "start_date")

    //    清洗wedding_month标签
    val newResult: DataFrame = peopleInfos
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
      .toDF("high_cert_no", "new_wedding_month")

    /**
      * hbase中标签
      */
    val oldResult: DataFrame = hbaseInfo
      .selectExpr("cert_no", "wedding_month as old_wedding_month")

//    整理新旧标签
    val result: DataFrame = newResult
      .join(oldResult, newResult("high_cert_no") === oldResult("cert_no"), "leftouter")
      .filter("getNew(cert_no) as cert_no")
      .selectExpr("high_cert_no", "new_wedding_month as wedding_month")

//    结果
    toHBase(result, "label_person", "high_info", "high_cert_no")

  }

  def updateRideInfo(hiveContext: HiveContext, peopleInfo: DataFrame, productInfo: DataFrame, hbaseInfo: DataFrame): Unit = {
    import hiveContext.implicits._
    hiveContext.udf.register("dropEmptys", (line: String) => dropEmpty(line))
    hiveContext.udf.register("dropSpecial", (line: String) => dropSpecial(line))
    hiveContext.udf.register("getNew", (line: String) => {
      if (line == null) true else false
    })

    /**
      * 读取MySQL中的other表
      */
    val peopleInfos: DataFrame = peopleInfo
      .selectExpr("high_cert_no", "product_code", "start_date")

    //    清洗ride标签
    val newResult: DataFrame = peopleInfos
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
        val allRideTimeSteps: String = allRideTimeStep(line._2)
        val allRideBrands: String = allRideBrand(line._2)
        val allRideDates: String = allRideDate(line._2)

        //        结果
        (highCertNo, rideDay, allRideTimeSteps, allRideBrands, allRideDates)
      })
      .toDF("high_cert_no", "new_ride_days", "new_all_ride_time_step", "new_all_ride_brand", "new_all_ride_date")

    /**
      * hbase标签
      */
    val oldResult: DataFrame = hbaseInfo
      .selectExpr("cert_no", "ride_days as old_ride_days", "all_ride_time_step as old_all_ride_time_step",
        "all_ride_brand as old_all_ride_brand", "all_ride_date as old_all_ride_date")

//    整理标签
    val result: DataFrame = newResult
      .join(oldResult, newResult("high_cert_no") === oldResult("cert_no"))
      .map(line => {
        val highCertNo: String = line.getAs[String]("high_cert_no")
        val newRideDays: String = line.getAs[String]("new_ride_days")
        val newAllRideTimeStep: String = line.getAs[String]("new_all_ride_time_step")
        val newAllRideBrand: String = line.getAs[String]("new_all_ride_brand")
        val newAllRideDate: String = line.getAs[String]("new_all_ride_date")
        val oldRideDays: String = line.getAs[String]("old_ride_days")
        val oldAllRideTimeStep: String = line.getAs[String]("old_all_ride_time_step")
        val oldAllRideBrand: String = line.getAs[String]("old_all_ride_brand")
        val oldAllRideDate: String = line.getAs[String]("old_all_ride_date")
//        创建list
        val allRideTimeStep: ListBuffer[String] = new ListBuffer[String]
        val allRideBrand: ListBuffer[String] = new ListBuffer[String]
        val allRideDate: ListBuffer[String] = new ListBuffer[String]
//        循环放入数据
        for (a <- JSON.parseObject(oldAllRideTimeStep).keySet()) allRideTimeStep ++= flat(a, JSON.parseObject(oldAllRideTimeStep).get(a).toString)
        for (a <- JSON.parseObject(newAllRideTimeStep).keySet()) allRideTimeStep ++= flat(a, JSON.parseObject(newAllRideTimeStep).get(a).toString)
        for (a <- JSON.parseObject(oldAllRideBrand).keySet()) allRideBrand ++= flat(a, JSON.parseObject(oldAllRideBrand).get(a).toString)
        for (a <- JSON.parseObject(newAllRideBrand).keySet()) allRideBrand ++= flat(a, JSON.parseObject(newAllRideBrand).get(a).toString)
        for (a <- JSON.parseObject(oldAllRideDate).keySet()) allRideDate ++= flat(a, JSON.parseObject(oldAllRideDate).get(a).toString)
        for (a <- JSON.parseObject(newAllRideDate).keySet()) allRideDate ++= flat(a, JSON.parseObject(newAllRideDate).get(a).toString)
//        系标签
        val ride_days: String = (newRideDays.toInt + oldRideDays.toInt).toString
        val max_ride_time_step: String = rideTimeStep(allRideTimeStep)


      })



  }

  /**
    * 更新is_online_car信息
    * @param hiveContext
    * @param peopleInfo
    * @param productInfo
    * @param hbaseInfo
    */
  def updateIsOnlineCar(hiveContext: HiveContext, peopleInfo: DataFrame, productInfo: DataFrame, hbaseInfo: DataFrame): Unit = {
    import hiveContext.implicits._
    hiveContext.udf.register("dropEmptys", (line: String) => dropEmpty(line))
    hiveContext.udf.register("dropSpecial", (line: String) => dropSpecial(line))
    hiveContext.udf.register("getNew", (line: String) => {
      if (line == null) true else false
    })

    /**
      * 读取MySQL中的other表
      */
    val peopleInfos: DataFrame = peopleInfo
      .selectExpr("high_cert_no", "product_code")
      .dropDuplicates(Array("high_cert_no", "product_code"))

    //    清洗is_online_car标签
    val newResult: DataFrame = peopleInfos
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
      .toDF("high_cert_no", "new_online_car")

    /**
      * hbase信息
      */
    val oldResult: DataFrame = hbaseInfo
      .selectExpr("cert_no", "is_online_car as old_online_car")

//    整理新旧标签
    val result: DataFrame = newResult
      .join(oldResult, newResult("high_cert_no") === oldResult("cert_no"), "leftouter")
      .filter("getNew(cert_no) as cert_no")
      .selectExpr("high_cert_no", "new_online_car as is_online_car")

//    结果
    toHBase(result, "label_person", "high_info", "high_cert_no")

  }

  /**
    * 更新part_time_nums标签
    * @param hiveContext
    * @param peopleInfo
    * @param productInfo
    * @param hbaseInfo
    */
  def updatePartTimeNums(hiveContext: HiveContext, peopleInfo: DataFrame, productInfo: DataFrame, hbaseInfo: DataFrame): Unit = {
    import hiveContext.implicits._
    hiveContext.udf.register("dropEmptys", (line: String) => dropEmpty(line))
    hiveContext.udf.register("dropSpecial", (line: String) => dropSpecial(line))
    hiveContext.udf.register("getNew", (line: String) => {
      if (line == null) true else false
    })

    /**
      * 读取MySQL中的other表
      */
    val peopleInfos: DataFrame = peopleInfo
      .selectExpr("high_cert_no", "product_code")
      .dropDuplicates(Array("high_cert_no", "product_code"))

    //    清洗part_time_nums标签
    val newResult: DataFrame = peopleInfos
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
      .toDF("high_cert_no", "new_part_time_nums")

    /**
      * hbase标签
      */
    val oldResult: DataFrame = hbaseInfo
      .selectExpr("cert_no", "part_time_nums as old_part_time_nums")

//    整理标签
    val result: DataFrame = newResult
      .join(oldResult, newResult("high_cert_no") === oldResult("cert_no"), "leftouter")
      .map(line => {
        val highCertNo: String = line.getAs[String]("high_cert_no")
        val newPTN: String = line.getAs[String]("new_part_time_nums")
        val oldPTN: String = line.getAs[String]("old_part_time_nums")
        var PTN: String = null
//        合并逻辑
        if (oldPTN == null) {
          PTN = newPTN
        } else {
          PTN = (oldPTN.toInt + newPTN.toInt).toString
        }
//        结果
        (highCertNo, PTN)
      })
      .toDF("high_cert_no", "part_time_nums")

//    结果
    toHBase(result, "label_person", "high_info", "high_cert_no")

  }

  /**
    * 更新is_h_house标签
    * @param hiveContext
    * @param peopleInfo
    * @param productInfo
    * @param hbaseInfo
    */
  def updateIsHHouse(hiveContext: HiveContext, peopleInfo: DataFrame, productInfo: DataFrame, hbaseInfo: DataFrame): Unit = {
    import hiveContext.implicits._
    hiveContext.udf.register("dropEmptys", (line: String) => dropEmpty(line))
    hiveContext.udf.register("dropSpecial", (line: String) => dropSpecial(line))
    hiveContext.udf.register("getNew", (line: String) => {
      if (line == null) true else false
    })

    /**
      * 读取MySQL中的other表
      */
    val peopleInfos: DataFrame = peopleInfo
      .selectExpr("high_cert_no", "product_code")
      .dropDuplicates(Array("high_cert_no", "product_code"))

    //    清洗is_h_house标签
    val newResult: DataFrame = peopleInfos
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
      .toDF("high_cert_no", "new_h_house")

    /**
      * hbase标签
      */
    val oldResult: DataFrame = hbaseInfo
      .selectExpr("cert_no", "is_h_house as old_h_house")

//    整理标签
    val result: DataFrame = newResult
      .join(oldResult, newResult("high_cert_no") === oldResult("cert_no"), "leftouter")
      .filter("getNew(cert_no) as cert_no")
      .selectExpr("high_cert_no", "new_h_house as is_h_house")

//    结果
    toHBase(result, "label_person", "high_info", "high_cert_no")

  }

  def updateAmplitudeFrequenter(hiveContext: HiveContext, peopleInfo: DataFrame, productInfo: DataFrame, hbaseInfo: DataFrame): Unit = {
    import hiveContext.implicits._
    hiveContext.udf.register("dropEmptys", (line: String) => dropEmpty(line))
    hiveContext.udf.register("dropSpecial", (line: String) => dropSpecial(line))
    hiveContext.udf.register("getNew", (line: String) => {
      if (line == null) true else false
    })


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
    val currDate = sdf.format(new Date()).split(" ")(0) + "00:00:00"
    val sevenDate = currTimeFuction(currDate, -7)
    //    计算当前月份和上个月份
    val currMonth = currDate.substring(0, 8) + "01"
    val lastMonth = currMonth.substring(0, 6) + (currMonth.substring(6, 7).toInt - 1).toString + currMonth.substring(7)

    val table = "select * from open_other_policy where month = '" + lastMonth + "' or month = '" + currMonth + "'"
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
      .where("insured_cert_type = 2 and length(insured_cert_no) = 18")
      .selectExpr("insured_cert_no as high_cert_no", "product_code", "start_date")

    otherResult

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

  def hbaseInfos(sc: SparkContext, hiveContext: HiveContext): DataFrame = {
    import hiveContext.implicits._

    /**
      * 读取hbase中的部分标签
      */
    val hbaseData: DataFrame = getHbaseBussValue(sc, "label_person")
      .map(line => {
        val key: String = Bytes.toString(line._2.getRow)
        val isCoxcombry: String = Bytes.toString(line._2.getValue("high_info".getBytes, "is_coxcombry".getBytes))
        val weddingMonth: String = Bytes.toString(line._2.getValue("high_info".getBytes, "wedding_month".getBytes))
        val rideDays: String = Bytes.toString(line._2.getValue("high_info".getBytes, "ride_days".getBytes))
        val allRideTimeStep: String = Bytes.toString(line._2.getValue("high_info".getBytes, "all_ride_time_step".getBytes))
        val allRideBrand: String = Bytes.toString(line._2.getValue("high_info".getBytes, "all_ride_brand".getBytes))
        val allRideDate: String = Bytes.toString(line._2.getValue("high_info".getBytes, "all_ride_date".getBytes))
        val tripRate: String = Bytes.toString(line._2.getValue("high_info".getBytes, "trip_rate".getBytes))
        val internalClock: String = Bytes.toString(line._2.getValue("high_info".getBytes, "internal_clock".getBytes))
        val isOnlineCar: String = Bytes.toString(line._2.getValue("high_info".getBytes, "is_online_car".getBytes))
        val partTimeNums: String = Bytes.toString(line._2.getValue("high_info".getBytes, "part_time_nums".getBytes))
        val isHHouse: String = Bytes.toString(line._2.getValue("high_info".getBytes, "is_h_house".getBytes))
        val amplitudeFrequenter: String = Bytes.toString(line._2.getValue("high_info".getBytes, "amplitude_frequenter".getBytes))

        //        结果
        (key, isCoxcombry, weddingMonth, rideDays, allRideTimeStep, allRideBrand, allRideDate, tripRate, internalClock,
        isOnlineCar, partTimeNums, isHHouse, amplitudeFrequenter)
      })
      .toDF("cert_no", "is_coxcombry", "wedding_month", "ride_days", "all_ride_time_step", "all_ride_brand", "all_ride_date",
      "trip_rate", "internal_clock", "is_online_car", "part_time_nums", "is_h_house", "amplitude_frequenter")

    //    结果
    hbaseData

  }

  /**
    * 拆分list
    * @param str1
    * @param str2
    * @return
    */
  def flat(str1: String, str2: String): ListBuffer[String] = {
    var list: ListBuffer[String] = new ListBuffer[String]
    for (s <- 1 to str2.toInt) {
      list += str1
    }
    list
  }

}
