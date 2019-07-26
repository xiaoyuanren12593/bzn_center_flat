package bzn.c_person.interfaceinfo

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util
import java.util.{Date, Properties}
import java.util.regex.Pattern

import bzn.c_person.util.SparkUtil
import bzn.job.common.{HbaseUtil, Until}
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.io.Source

object CPersonHighInfoTest extends SparkUtil with Until with HbaseUtil{

  def main(args: Array[String]): Unit = {

    //    初始化设置
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName: String = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "")

    val sc: SparkContext = sparkConf._2
    val hiveContext: HiveContext = sparkConf._4

    //    清洗标签
    val peopleInfo: DataFrame = readMysqlOtherTable(hiveContext)
    val productInfo: DataFrame = readMysqlProTable(hiveContext)

    val certInfo: DataFrame = getCertInfo(hiveContext, peopleInfo)
    val coxcombry: DataFrame = getCoxcombry(hiveContext, peopleInfo, productInfo)
    val wedding: DataFrame = getWedding(hiveContext, peopleInfo, productInfo)
    val ride: DataFrame = getRideInfo(hiveContext, peopleInfo, productInfo)
    val onlineCar: DataFrame = getOnlineCar(hiveContext, peopleInfo, productInfo)
    val partTimeNums: DataFrame = getPartNums(hiveContext, peopleInfo, productInfo)
    val house: DataFrame = getHouse(hiveContext, peopleInfo, productInfo)
    val amplitudeFrequenter = getamplitudeFrequenter(hiveContext, peopleInfo, productInfo)

    val allData: DataFrame = unionAll(certInfo, coxcombry, wedding, ride, onlineCar, partTimeNums, house, amplitudeFrequenter)

    allData.show()

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
      .aggregateByKey(List[String]())(
        (List: List[String], value: String) => List:+value,
        (List1: List[String], List2: List[String]) => List1:::List2
      )
      .map(line => {
        val highCertNo: String = line._1
        val isCoxcombry: String = if (line._2.contains("新氧医美")) "是" else "否"
//        结果
        (highCertNo, isCoxcombry)
      })
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

    //    清洗wedding_month标签
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
        (line._2._1 == "星驾单车" || line._2._1 == "七彩单车" || line._2._1 == "闪骑电单车" || line._2._1 == "DDbike" ||
          line._2._1 == "酷骑单车" || line._2._1 == "小鹿单车" || line._2._1 == "骑迹单车" || line._2._1 == "OFO" ||
          line._2._1 == "便利蜂单车" || line._2._1 == "飞鸽出行" || line._2._1 == "骑迹单车" || line._2._1 == "西游单车" ||
          line._2._1 == "景智单车")
      })
      .aggregateByKey(List[(String, String, String, String, String)]())(
        (List: List[(String, String, String, String, String)], value: (String, String, String, String, String)) => List:+value,
        (List1: List[(String, String, String, String, String)], List2: List[(String, String, String, String, String)]) => List1:::List2
      )
      .map(line => {
        val rideDays: String = rideDate(line._2)
        val maxRideTimeStep: String = rideTimeStep(line._2)
        val maxRideBrand: String = rideBrand(line._2)
        val maxRideDate: String = rideDate(line._2)
        val tripRates: String = tripRate(line._2)
        val internalClocks: String = internalClock(line._2)
//        结果
        (rideDays, maxRideTimeStep, maxRideBrand, maxRideDate, tripRates, internalClocks)
      })
      .toDF("ride_days", "max_ride_time_step", "max_ride_brand", "max_ride_date", "trip_rate", "internal_clock")

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
      .aggregateByKey(List[String]())(
        (List: List[String], value: String) => List:+value,
        (List1: List[String], List2: List[String]) => List1:::List2
      )
      .map(line => {
        val highCertNo: String = line._1
        val isOnlineCar: String = if (line._2.contains("曹操意外险")) "是" else "否"
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
      .aggregateByKey(List[String]())(
        seqOp = (List: List[String], value: String) => List :+ value,
        combOp = (List1: List[String], List2: List[String]) => List1 ::: List2
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
      .aggregateByKey(List[String]())(
        (List: List[String], value: String) => List:+value,
        (List1: List[String], List2: List[String]) => List1:::List2
      )
      .map(line => {
        val highCertNo: String = line._1
        val ishHouse: String = if (line._2.contains("侃家")) "是" else "否"
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
      .filter(line => line._2.contains("青芒果方案1"))
      .aggregateByKey(List[String]())(
        (List: List[String], value: String) => List:+value,
        (List1: List[String], List2: List[String]) => List1:::List2
      )
      .map(line => {
        val highCertNo: String = line._1
        val amplitudeFrequenter: String = line._2.size.toString
        //        结果
        (highCertNo, amplitudeFrequenter)
      })
      .toDF("high_cert_no", "amplitude_frequenter")

    //    结果
    result

  }

  def unionAll(certInfo: DataFrame, coxcombry: DataFrame, wedding: DataFrame, ride: DataFrame, onlineCar: DataFrame,
               partTimeNums: DataFrame, house: DataFrame, amplitudeFrequenter: DataFrame): DataFrame = {

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

//    结果
    result

  }


  /**
    * 获取 Mysql 表的数据
    * @param sqlContext
    * @return 返回 Mysql 表的 DataFrame
    */
  def readMysqlOtherTable(sqlContext: SQLContext): DataFrame = {
    val url = "jdbc:mysql://172.16.11.103:3306/bzn_open_all?tinyInt1isBit=false&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&allowMultiQueries=true&user=root&password=123456"
    val properties: Properties = getProPerties()
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

    val result: DataFrame = sqlContext
      .read
      .jdbc(url, "open_other_policy", predicates, properties)
      .selectExpr("insured_cert_no", "insured_cert_type", "product_code", "start_date")
      .where("insured_cert_type = 2 and length(insured_cert_no) = 18 and length(start_date) > 0")
      .filter("dropSpecial(insured_cert_no) as high_cert_no")
      .cache()

    result

  }

  /**
    * 获取 Mysql 表的数据
    * @param sqlContext
    * @return 返回 Mysql 表的 DataFrame
    */
  def readMysqlProTable(sqlContext: SQLContext): DataFrame = {
    val url = "jdbc:mysql://172.16.11.105:3306/dwdb?tinyInt1isBit=false&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&allowMultiQueries=true&user=root&password=123456"
    val properties: Properties = getProPerties()

    val result: DataFrame = sqlContext
      .read
      .jdbc(url, "dim_product", properties)
      .selectExpr("product_code as product_codes", "product_desc")
      .cache()

    result

  }

  /**
    * 将空字符串、空值转换为NULL
    * @param Temp
    * @return
    */
  def dropEmpty(Temp: String): String = {
    if (Temp == "" || Temp == "NULL" || Temp == null) null else Temp
  }

  /**
    * 身份证匹配
    * @param Temp
    * @return
    */
  def dropSpecial(Temp: String): Boolean = {
    if (Temp != null) {
      val pattern = Pattern.compile("^[\\d]{17}[\\dxX]{1}$")
      pattern.matcher(Temp).matches
    } else false
  }

  /**
    * 根据生日月日获取星座id
    * @param month
    * @param day
    * @return
    */
  def getConstellation(month: String, day: String): String = {
    val dayArr = Array[Int](20, 19, 21, 20, 21, 22, 23, 23, 23, 24, 23, 22)
    val constellationArr = Array[Int](10, 11, 12, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    return if (day.toInt < dayArr(month.toInt - 1)) {
      constellationArr(month.toInt - 1).toString
    } else {
      constellationArr(month.toInt).toString
    }
  }

  /**
    * 自定义拼接Json
    * @param tuples
    * @return
    */
  def udfJson(tuples: Iterator[(String, String, String, String)]): String = {
    //    创建手机号信息List
    val list: util.List[util.HashMap[String, String]] = new util.ArrayList[util.HashMap[String, String]]()
    //    遍历迭代器
    while (tuples.hasNext) {
      val tuple: (String, String, String, String) = tuples.next()
      //      当手机信息都不为空时全部塞进去，只手机号不为空时就把手机号塞进去，都为空时什么都不塞进去
      if (tuple._1 != null && tuple._2 != null && tuple._3 != null && tuple._4 != null) {
        val map: util.HashMap[String, String] = new util.HashMap[String, String]()
        map.put("base_tel_name", tuple._1)
        map.put("base_tel_province", tuple._2)
        map.put("base_tel_city", tuple._3)
        map.put("base_tel_operator", tuple._4)
        list.add(map)
      } else if (tuple._1 != null && tuple._2 == null && tuple._3 == null && tuple._4 == null) {
        val map: util.HashMap[String, String] = new util.HashMap[String, String]()
        map.put("base_tel_name", tuple._1)
        list.add(map)
      }
    }
    //    将嵌套数组改为Json格式字符串
    val jsonString: String = JSON.toJSONString(list, SerializerFeature.BeanToArray)
    //    如果Json格式字符串为[]，则转为null
    if (jsonString == "[]") null else jsonString

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
    * 将DataFrame写入HBase
    * @param dataFrame
    * @param tableName
    * @param columnFamily
    */
  def toHBase2(dataFrame: DataFrame, tableName: String, columnFamily: String): Unit = {
    //    获取conf
    val con: (Configuration, Configuration) = HbaseConf(tableName)
    val conf_fs: Configuration = con._2
    val conf: Configuration = con._1
    //    获取列
    val cols: Array[String] = dataFrame.columns
    //    取不等于key的列循环

    cols.filter(x => x != "base_cert_no").map(x => {
      val hbaseRDD: RDD[(String, String, String)] = dataFrame.map(rdd => {
        val certNo = rdd.getAs[String]("base_cert_no")
        val clo: Any = rdd.getAs[Any](x)
        //证件号，列值 列名
        (certNo,clo,x)
      })
        .filter(x => x._2 != null && x._2 != "")
        .map(x => (x._1,x._2.toString,x._3))

      saveToHbase(hbaseRDD, columnFamily, conf_fs, tableName, conf)
    })
  }

}
