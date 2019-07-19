package bzn.c_person.interfaceinfo

import java.text.SimpleDateFormat
import java.util
import java.util.regex.Pattern
import java.util.{Date, Properties}

import bzn.job.common.{HbaseUtil, Until}
import c_person.util.SparkUtil
import com.alibaba.fastjson.{JSON, JSONObject}
import com.alibaba.fastjson.serializer.SerializerFeature
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.hive.HiveContext

import scala.io.Source

object CPersonInterfaceInfoTest extends SparkUtil with Until with HbaseUtil{

  def main(args: Array[String]): Unit = {

    //    初始化设置
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf = sparkConfInfo(appName, "local[*]")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4

    //    清洗标签
//    val certInfo: DataFrame = getCertInfo(hiveContext)
    val telInfo: DataFrame = getTelInfo(hiveContext)

//    certInfo.show()
    telInfo.show()

//    合并
//    val result: DataFrame = unionTable(certInfo, telInfo)
//    result.show()

    sc.stop()

  }

  /**
    * 获取身份信息
    * @param hiveContext
    * @return
    */
  def getCertInfo(hiveContext: HiveContext): DataFrame = {
    import hiveContext.implicits._
    hiveContext.udf.register("dropEmptys", (line: String) => dropEmpty(line))
    hiveContext.udf.register("dropSpecial", (line: String) => dropSpecial(line))

    /**
      * 读取ofo接口的hive表
      */
    val ofoInfo: DataFrame = hiveContext.sql("select product_code, insured_name, insured_cert_no from odsdb_prd.open_ofo_policy_parquet_temp")
      .where("product_code = 'OFO00002' and length(insured_cert_no) = 18")
      .selectExpr("insured_cert_no as base_cert_no", "insured_name as base_name")

    /**
      * 读取58速运接口的hive表
      */
    val suyunInfo: DataFrame = hiveContext.sql("select courier_card_no, courier_name from odsdb_prd.open_express_policy")
      .where("length(courier_card_no) = 18")
      .selectExpr("courier_card_no as base_cert_no", "courier_name as base_name")
      .limit(10000)

//    合并数据
    val certInfo: DataFrame = ofoInfo
      .unionAll(suyunInfo)
      .filter("dropSpecial(base_cert_no) as base_cert_no")
      .dropDuplicates(Array("base_cert_no"))

//    清洗身份证标签
    val certInfoTemp: DataFrame = certInfo
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
        val constellatoryId: String = if (baseCertNo.length == 18) {
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
  def getTelInfo(hiveContext: HiveContext): DataFrame = {
    import hiveContext.implicits._
    hiveContext.udf.register("dropEmptys", (line: String) => dropEmpty(line))
    hiveContext.udf.register("dropSpecial", (line: String) => dropSpecial(line))

    /**
      * 读取ofo接口的手机号信息
      */
    val ofoTelInfo: DataFrame = hiveContext.sql("select product_code, insured_cert_no, insured_mobile from odsdb_prd.open_ofo_policy_parquet_temp")
      .where("product_code = 'OFO00002' and length(insured_cert_no) = 18")
      .selectExpr("insured_cert_no as base_cert_no", "insured_mobile as base_mobile")

    println(ofoTelInfo.rdd.getNumPartitions)

    /**
      * 读取58速运接口的手机号信息
      */
    val suyunTelInfo: DataFrame = hiveContext.sql("select courier_card_no, courier_mobile from odsdb_prd.open_express_policy")
      .where("length(courier_card_no) = 18")
      .selectExpr("courier_card_no as base_cert_no", "courier_mobile as base_mobile")
      .limit(100000)

    println(suyunTelInfo.rdd.getNumPartitions)

//    合并数据
    val telInfo: DataFrame = ofoTelInfo
      .unionAll(suyunTelInfo)
      .selectExpr("base_cert_no", "dropEmptys(base_mobile) as base_mobile")
      .filter("dropSpecial(base_cert_no) as base_cert_no")
      .dropDuplicates(Array("base_cert_no", "base_mobile"))

    //    读取手机信息表
    val mobileInfo: DataFrame = readMysqlTable(hiveContext, "t_mobile_location")
      .selectExpr("mobile", "province", "city", "operator")
      .limit(100000)

    println(mobileInfo.rdd.getNumPartitions)

    //    与手机信息表连接
    val telInfoAll: DataFrame = telInfo
      .join(mobileInfo, telInfo("base_mobile") === mobileInfo("mobile"), "leftouter")
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
        (baseCertNo, (baseTelMobile, baseTelProvince, baseTelCity, baseTelOperator))
      })
      .groupByKey()
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
      pattern.matcher(Temp).matches()
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
    * 获取 Mysql 表的数据
    * @param sqlContext
    * @param tableName 读取Mysql表的名字
    * @return 返回 Mysql 表的 DataFrame
    */
  def readMysqlTable(sqlContext: SQLContext, tableName: String): DataFrame = {
    val properties: Properties = getProPerties()
    sqlContext
      .read
      .format("jdbc")
      .option("url", properties.getProperty("mysql.url"))
      .option("driver", properties.getProperty("mysql.driver"))
      .option("user", properties.getProperty("mysql.username"))
      .option("password", properties.getProperty("mysql.password"))
      .option("numPartitions","10")
      .option("partitionColumn","id")
      .option("lowerBound", "0")
      .option("upperBound","200")
      .option("dbtable", tableName)
      .load()

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
