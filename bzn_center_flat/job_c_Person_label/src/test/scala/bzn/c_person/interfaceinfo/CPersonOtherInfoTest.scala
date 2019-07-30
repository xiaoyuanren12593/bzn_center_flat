package bzn.c_person.interfaceinfo

import java.text.SimpleDateFormat
import java.util
import java.util.{Date, Properties}
import java.util.regex.Pattern

import bzn.job.common.{HbaseUtil, Until}
import c_person.util.SparkUtil
import com.alibaba.fastjson.{JSON, JSONObject}
import com.alibaba.fastjson.serializer.SerializerFeature
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

object CPersonOtherInfoTest extends SparkUtil with Until with HbaseUtil {

  def main(args: Array[String]): Unit = {

    //    初始化设置
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName: String = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "local[*]")

    val sc: SparkContext = sparkConf._2
    val hiveContext: HiveContext = sparkConf._4

    //    清洗标签
    val peopleInfo: DataFrame = readMysqlOtherTable(hiveContext)
    peopleInfo.cache()

    val certInfo: DataFrame = getCertInfo(hiveContext, peopleInfo)
    val telInfo: DataFrame = getTelInfo(hiveContext, peopleInfo)

    //    合并
    val result: DataFrame = unionTable(certInfo, telInfo)

    //    写到hive中
//    result.write.mode(SaveMode.Overwrite).saveAsTable("label.interface_label")
    result.show()

    sc.stop()

  }

  /**
    * 清洗cert信息
    * @param hiveContext
    * @return cert的dataframe
    */
  def getCertInfo(hiveContext: HiveContext, peopleInfo: DataFrame): DataFrame = {
    import hiveContext.implicits._
    hiveContext.udf.register("dropEmptys", (line: String) => dropEmpty(line))
    hiveContext.udf.register("dropSpecial", (line: String) => dropSpecial(line))

    /**
      * 读取other接口的mysql表
      */
    val otherInfo: DataFrame = peopleInfo
      .selectExpr("insured_cert_no as base_cert_no", "insured_name as base_name")
      .filter("dropSpecial(base_cert_no) as base_cert_no")
      .dropDuplicates(Array("base_cert_no"))

    //    清洗身份证标签
    val certInfoTemp: DataFrame = otherInfo
      .map(line => {
        //        身份证号
        val baseCertNo: String = line.getAs[String]("base_cert_no")
        //        姓名
        val baseNameTemp: String = line.getAs[String]("base_name")
        val baseName: String = dropEmpty(baseNameTemp)
        //        性别
        val baseGender: String = if (baseCertNo.length == 18) {
          val genderNo: Int = baseCertNo.substring(16, 17).toInt
          if (genderNo % 2 == 1) "1" else if (genderNo % 2 == 0) "2" else null
        } else null
        //        生日
        val baseBirthday: String = if (baseCertNo.length == 18) baseCertNo.substring(6, 14) else null
        //        年龄
        val baseAge: String = if (baseCertNo.length == 18) {
          val time: Date = new Date()
          val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          getAgeFromBirthTime(baseCertNo, sdf.format(time)).toString
        } else null
        //        年龄所处年代
        val baseAgeTime: String = if (baseCertNo.length == 18) {
          val ageTime: Int = baseCertNo.substring(8, 10).toInt
          ageTime match {
            case _ if (ageTime >= 0 && ageTime < 10) => "00后"
            case _ if (ageTime >= 10 && ageTime < 20) => "10后"
            case _ if (ageTime >= 20 && ageTime < 30) => "20后"
            case _ if (ageTime >= 30 && ageTime < 40) => "30后"
            case _ if (ageTime >= 40 && ageTime < 50) => "40后"
            case _ if (ageTime >= 50 && ageTime < 60) => "50后"
            case _ if (ageTime >= 60 && ageTime < 70) => "60后"
            case _ if (ageTime >= 70 && ageTime < 80) => "70后"
            case _ if (ageTime >= 80 && ageTime < 90) => "80后"
            case _ if (ageTime >= 90 && ageTime < 100) => "90后"
            case _  => null
          }
        } else null
        //        年龄所属区间
        val baseAgeSection: String = if (baseAge != null) {
          val baseAgeTemp = baseAge.toInt
          baseAgeTemp match {
            case _ if (baseAgeTemp < 12) => "儿童"
            case _ if (baseAgeTemp >= 12 && baseAgeTemp < 18) => "少年"
            case _ if (baseAgeTemp >= 18 && baseAgeTemp < 28) => "青年"
            case _ if (baseAgeTemp >= 28 && baseAgeTemp < 45) => "壮年"
            case _ if (baseAgeTemp >= 45 && baseAgeTemp < 60) => "中年"
            case _ if (baseAgeTemp >= 60) => "老年"
            case _ => null
          }
        } else null
        //        是否退休
        val baseIsRetire: String = if (baseAge != null) {
          val baseAgeTemp = baseAge.toString.toInt
          if (baseAgeTemp <= 60) "未退休" else if (baseAgeTemp > 60) "退休" else null
        } else null
        //        籍贯码表id
        val nativePlaceId: String = if (baseCertNo.length == 18) {
          baseCertNo.substring(0, 4) + "00"
        } else null
        //        星座码表id
        val constellatoryId: String = if (
          baseCertNo.length == 18 &&
            (baseCertNo.substring(6, 10).toInt >= 1919) &&
            (baseCertNo.substring(10, 12).toInt >= 1 && baseCertNo.substring(10, 12).toInt <= 12) &&
            (baseCertNo.substring(12, 14).toInt >= 1 && baseCertNo.substring(12, 14).toInt <= 31)
        ) {
          getConstellation(baseCertNo.substring(10, 12), baseCertNo.substring(12, 14))
        } else null

        //        结果
        (baseCertNo, baseName, baseGender, baseBirthday, baseAge, baseAgeTime, baseAgeSection, baseIsRetire,
          nativePlaceId, constellatoryId)
      })
      .toDF("base_cert_no", "base_name", "base_gender", "base_birthday", "base_age", "base_age_time", "base_age_section",
        "base_is_retire", "native_place_id", "constellatory_id")

    /**
      * 读取地区码表
      */
    val areaInfoDimension: DataFrame = hiveContext.sql("select * from odsdb.ods_area_info_dimension")
      .selectExpr("code", "province", "short_name", "city_region", "case when is_coastal = '' then null else is_coastal end as is_coastal",
        "case when city_type = '' then null else city_type end as city_type", "weather_feature", "weather_type", "city_deit")

    //    个人信息关联区域码表
    val certInfoJoin: DataFrame = certInfoTemp
      .join(areaInfoDimension, certInfoTemp("native_place_id") === areaInfoDimension("code"), "leftouter")
      .selectExpr("base_cert_no", "base_name", "base_gender", "base_birthday", "base_age", "base_age_time", "base_age_section",
        "base_is_retire",  "province as base_province", "short_name as base_city", "city_region as base_area",
        "is_coastal as base_coastal", "city_type as base_city_type", "weather_feature as base_weather_feature",
        "weather_type as base_city_weather", "city_deit as base_city_deit", "constellatory_id")

    /**
      * 读取星座码表
      */
    val constellationDimension: DataFrame = hiveContext.sql("select * from odsdb.ods_constellation_dimension")
      .map(line => {
        val id: String = line.getAs[String]("id")
        val baseConsName: String = line.getAs[String]("constellation_name")
        val baseConsType: String = line.getAs[String]("constellation_quadrant")
        val baseConsCharacter1: String = line.getAs[String]("constellation_type1")
        val baseConsCharacter2: String = line.getAs[String]("constellation_type2")
        val baseConsCharacter3: String = line.getAs[String]("constellation_type3")
        //        创建Json
        val json_value = new JSONObject()
        json_value.put("Character1", baseConsCharacter1)
        json_value.put("Chatacter2", baseConsCharacter2)
        json_value.put("Chatacter3", baseConsCharacter3)
        //        结果
        (id, baseConsName, baseConsType, json_value.toString)
      })
      .toDF("id", "base_cons_name", "base_cons_type", "base_cons_character")

    //    个人信息关联星座码表
    val certInfoRes: DataFrame = certInfoJoin
      .join(constellationDimension, certInfoJoin("constellatory_id") === constellationDimension("id"), "leftouter")
      .selectExpr("base_cert_no", "base_name", "base_gender", "base_birthday", "base_age", "base_age_time", "base_age_section",
        "base_is_retire", "base_province", "base_city", "base_area", "base_coastal", "base_city_type", "base_weather_feature",
        "base_city_weather", "base_city_deit", "base_cons_name", "base_cons_type", "base_cons_character")

    //    结果
    certInfoRes

  }

  /**
    * 获得手机号信息
    * @param hiveContext
    * @return
    */
  def getTelInfo(hiveContext: HiveContext, peopleInfo: DataFrame): DataFrame = {
    import hiveContext.implicits._
    hiveContext.udf.register("dropEmptys", (line: String) => dropEmpty(line))
    hiveContext.udf.register("dropSpecial", (line: String) => dropSpecial(line))

    /**
      * 读取other的mysql表
      */
    val otherInfo: DataFrame = peopleInfo
      .selectExpr("insured_cert_no as base_cert_no", "dropEmptys(insured_mobile) as base_mobile")
      .filter("dropSpecial(base_cert_no) as base_cert_no")
      .dropDuplicates(Array("base_cert_no", "base_mobile"))

    //    读取手机信息表
    val mobileInfo: DataFrame = readMysqlTelTable(hiveContext)
      .selectExpr("mobile", "province", "city", "operator")

    //    与手机信息表连接
    val telInfoAll: DataFrame = otherInfo
      .join(mobileInfo, otherInfo("base_mobile") === mobileInfo("mobile"), "leftouter")
      .selectExpr("base_cert_no", "base_mobile as base_tel_name", "province as base_tel_province", "city as base_tel_city",
        "operator as base_tel_operator")

    //    手机号结果
    val telInfoRes: DataFrame = telInfoAll
      .map(line => {
        val baseCertNo: String = line.getAs[String]("base_cert_no")
        val baseTelMobile: String = line.getAs[String]("base_tel_name")
        val baseTelProvince: String = line.getAs[String]("base_tel_province")
        val baseTelCity: String = line.getAs[String]("base_tel_city")
        val baseTelOperator: String = line.getAs[String]("base_tel_operator")
        //        结果
        (baseCertNo, (baseTelMobile, baseTelProvince, baseTelCity, baseTelOperator))
      })
      .aggregateByKey(List[(String, String, String, String)]())(
        (list: List[(String, String, String, String)], value: (String, String, String, String)) => list:+value,
        (list1: List[(String, String, String, String)], list2: List[(String, String, String, String)]) => list1:::list2
      )
      .map(line => {
        val baseCertNo: String = line._1
        val value: Iterator[(String, String, String, String)] = line._2.iterator
        val baseTel: String = udfJson(value)
        //        结果
        (baseCertNo, baseTel)
      })
      .toDF("base_cert_no", "base_tel")

    //    结果
    telInfoRes

  }

  /**
    * 合并表格
    * @param certInfo
    * @param telInfo
    * @return 返回最终结果
    */
  def unionTable(certInfo: DataFrame, telInfo: DataFrame): DataFrame = {

    val telInfos: DataFrame = telInfo.withColumnRenamed("base_cert_no", "tel_cert_no")

    //    关联表格
    val result: DataFrame = certInfo
      .join(telInfos, certInfo("base_cert_no") === telInfos("tel_cert_no"), "leftouter")
      .selectExpr("base_cert_no", "base_name", "base_gender", "base_birthday", "base_age", "base_age_time", "base_age_section",
        "base_is_retire", "base_province", "base_city", "base_area", "base_coastal", "base_city_type", "base_weather_feature",
        "base_city_weather", "base_city_deit", "base_cons_name", "base_cons_type", "base_cons_character", "base_tel")

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
      .selectExpr("insured_cert_no", "insured_cert_type", "insured_name", "insured_mobile")
    val data02: DataFrame = sqlContext.read.jdbc(url2, "open_other_policy", properties)
      .selectExpr("insured_cert_no", "insured_cert_type", "insured_name", "insured_mobile")
    val data03: DataFrame = sqlContext.read.jdbc(url3, "open_other_policy", properties)
      .selectExpr("insured_cert_no", "insured_cert_type", "insured_name", "insured_mobile")
    val data04: DataFrame = sqlContext.read.jdbc(url4, "open_other_policy", properties)
      .selectExpr("insured_cert_no", "insured_cert_type", "insured_name", "insured_mobile")
    val data05: DataFrame = sqlContext.read.jdbc(url5, "open_other_policy", properties)
      .selectExpr("insured_cert_no", "insured_cert_type", "insured_name", "insured_mobile")
    val data06: DataFrame = sqlContext.read.jdbc(url6, "open_other_policy", properties)
      .selectExpr("insured_cert_no", "insured_cert_type", "insured_name", "insured_mobile")
    val data07: DataFrame = sqlContext.read.jdbc(url7, "open_other_policy", properties)
      .selectExpr("insured_cert_no", "insured_cert_type", "insured_name", "insured_mobile")
    val data08: DataFrame = sqlContext.read.jdbc(url8, "open_other_policy", properties)
      .selectExpr("insured_cert_no", "insured_cert_type", "insured_name", "insured_mobile")
    val data09: DataFrame = sqlContext.read.jdbc(url9, "open_other_policy", properties)
      .selectExpr("insured_cert_no", "insured_cert_type", "insured_name", "insured_mobile")
    val data10: DataFrame = sqlContext.read.jdbc(url10, "open_other_policy", properties)
      .selectExpr("insured_cert_no", "insured_cert_type", "insured_name", "insured_mobile")
    val data11: DataFrame = sqlContext.read.jdbc(url11, "open_other_policy", properties)
      .selectExpr("insured_cert_no", "insured_cert_type", "insured_name", "insured_mobile")
    val data12: DataFrame = sqlContext.read.jdbc(url12, "open_other_policy", properties)
      .selectExpr("insured_cert_no", "insured_cert_type", "insured_name", "insured_mobile")

    //    mysql 201811-
    val data13: DataFrame = sqlContext
      .read
      .jdbc(url, "open_other_policy", predicates, properties)
      .selectExpr("insured_cert_no", "insured_cert_type", "insured_name", "insured_mobile")

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

    result

  }

  /**
    * 获取 Mysql 表的数据
    * @param sqlContext
    * @return 返回 Mysql 表的 DataFrame
    */
  def readMysqlTelTable(sqlContext: SQLContext): DataFrame = {
    val properties: Properties = getProPerties()
    sqlContext
      .read
      .format("jdbc")
      .option("url", properties.getProperty("mysql.url"))
      .option("driver", properties.getProperty("mysql.driver"))
      .option("user", properties.getProperty("mysql.username"))
      .option("password", properties.getProperty("mysql.password"))
      .option("numPartitions","20")
      .option("partitionColumn","id")
      .option("lowerBound", "126787")
      .option("upperBound","64944871")
      .option("dbtable", "t_mobile_location")
      .load()

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
