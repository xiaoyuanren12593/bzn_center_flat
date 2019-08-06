package bzn.c_person.interfaceinfo

import java.text.SimpleDateFormat
import java.util
import java.util.{Date, Properties}

import bzn.c_person.util.SparkUtil
import bzn.job.common.{HbaseUtil, Until}
import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source

object CPersonOtherInfoIncrementTest extends SparkUtil with Until with HbaseUtil {

  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName: String = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "local[*]")

    val sc: SparkContext = sparkConf._2
    val hiveContext: HiveContext = sparkConf._4

    //    读取数据
    val other: DataFrame = readMysqlOtherTable(hiveContext)
    val hbase: DataFrame = hbaseInfos(sc, hiveContext)

    other.cache()
    hbase.cache()

//    更新数据
    updateCertNo(hiveContext, other, hbase)
    updateTel(hiveContext, other, hbase)

    sc.stop()

  }

  /**
    * 更新身份信息
    * @param hiveContext
    * @param other
    * @param hbase
    */
  def updateCertNo(hiveContext: HiveContext, other: DataFrame, hbase: DataFrame): Unit = {
    import hiveContext.implicits._
    hiveContext.udf.register("getEmptyString", () => "")
    hiveContext.udf.register("dropEmptys", (line: String) => dropEmpty(line))
    hiveContext.udf.register("dropSpecial", (line: String) => dropSpecial(line))
    hiveContext.udf.register("getNew", (line: String) => {
      if (line == null) true else false
    })

    /**
      * 读取other接口的mysql表
      */
    val otherInfo: DataFrame = other
      .selectExpr("insured_cert_no as base_cert_no", "insured_name as base_name")
      .filter("dropSpecial(base_cert_no) as base_cert_no")
      .dropDuplicates(Array("base_cert_no"))

    //    获取全部新人员的身份信息
    val peopleInfo: DataFrame = otherInfo
      .join(hbase, otherInfo("base_cert_no") === hbase("cert_no"), "leftouter")
      .filter("getNew(cert_no) as cert_no")
      .selectExpr("base_cert_no", "base_name")

    /**
      * 读取地区码表
      */
    val areaInfoDimension: DataFrame = hiveContext.sql("select * from odsdb.ods_area_info_dimension")
      .selectExpr("code", "province", "short_name", "city_region", "case when is_coastal = '' then null else is_coastal end as is_coastal",
        "case when city_type = '' then null else city_type end as city_type", "weather_feature", "weather_type", "city_deit")

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

    //    清洗身份证标签
    val certInfoTemp: DataFrame = peopleInfo
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

    //    结果表
    val resultInfo: DataFrame = certInfoTemp
      .join(areaInfoDimension, certInfoTemp("native_place_id") === areaInfoDimension("code"), "leftouter")
      .join(constellationDimension, certInfoTemp("constellatory_id") === constellationDimension("id"), "leftouter")
      .selectExpr("base_cert_no", "base_name", "base_gender", "base_birthday", "base_age", "base_age_time", "base_age_section",
        "base_is_retire", "base_province", "base_city", "base_area", "base_coastal", "base_city_type", "base_weather_feature",
        "base_city_weather", "base_city_deit", "base_cons_name", "base_cons_type", "base_cons_character")

    //    结果
//    toHBase(resultInfo, "label_person", "base_info", "base_cert_no")
    resultInfo.show()
  }

  def updateTel(hiveContext: HiveContext, other: DataFrame, hbase: DataFrame): Unit = {
    import hiveContext.implicits._
    hiveContext.udf.register("dropEmptys", (line: String) => dropEmpty(line))
    hiveContext.udf.register("dropSpecial", (line: String) => dropSpecial(line))

    /**
      * 读取other的mysql表
      */
    val otherInfo: DataFrame = other
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
      .map(line => {
        val baseCertNo: String = line.getAs[String]("base_cert_no")
        val baseTelMobile: String = line.getAs[String]("base_tel_name")
        val baseTelProvince: String = line.getAs[String]("base_tel_province")
        val baseTelCity: String = line.getAs[String]("base_tel_city")
        val baseTelOperator: String = line.getAs[String]("base_tel_operator")
        (baseCertNo, (baseTelMobile, baseTelProvince, baseTelCity, baseTelOperator))
      })
      .aggregateByKey(mutable.ListBuffer[(String, String, String, String)]())(
        (List: ListBuffer[(String, String, String, String)], value: (String, String, String, String)) => List += value,
        (List1: ListBuffer[(String, String, String, String)], List2: ListBuffer[(String, String, String, String)]) => List1 ++= List2
      )
      .map(line => {
        val baseCertNo: String = line._1
        val baseTel: String = udfJson(line._2)
        //        结果
        (baseCertNo, baseTel)
      })
      .filter(line => line._2 != null)
      .toDF("base_cert_no", "base_tel")

    /**
      * 获取hbase的手机号信息
      */
    val oldTelInfo: DataFrame = hbase
      .selectExpr("cert_no as old_cert_no", "base_tel as old_base_tel")

    //    获取全部新保单手机号信息
    val result = telInfoAll
      .join(oldTelInfo, telInfoAll("base_cert_no") === oldTelInfo("old_cert_no"), "leftouter")
      .map(line => {
        val oldCertNo: String = line.getAs[String]("old_cert_no")
        val newCertNo: String = line.getAs[String]("base_cert_no")
        val oldTel: String = line.getAs[String]("old_base_tel")
        val newTel: String = line.getAs[String]("base_tel")
        //        更改手机号
        val oldTelArr: Array[JSONObject] = if (oldTel == null) null else parse(oldTel)
        val newTelArr: Array[JSONObject] = if (newTel == null) null else parse(newTel)
        val telInfo: ListBuffer[(String, String, String, String)] = ListBuffer[(String, String, String, String)]()
        val ifInfo: mutable.Set[String] = mutable.Set()

        //        重构的逻辑
        if (oldTelArr == null) {
          //          如果旧的手机号为空，则将新的手机号放入临时变量
          for (newTel <- newTelArr) {
            telInfo += ((parseJSONObject(newTel, "base_tel_name"), parseJSONObject(newTel, "base_tel_province"), parseJSONObject(newTel, "base_tel_city"), parseJSONObject(newTel, "base_tel_operator")))
          }
        } else {
          //          如果旧的手机号不为空，则先将旧的手机号放入，然后将不重复的新手机号放入
          for (oldTel <- oldTelArr) {
            telInfo += ((parseJSONObject(oldTel, "base_tel_name"), parseJSONObject(oldTel, "base_tel_province"), parseJSONObject(oldTel, "base_tel_city"), parseJSONObject(oldTel, "base_tel_operator")))
            ifInfo.add(parseJSONObject(oldTel, "base_tel_name"))
          }
          for (newTel <- newTelArr) {
            if (!ifInfo.contains(newTel.get("base_tel_name").toString)) telInfo += ((parseJSONObject(newTel, "base_tel_name"), parseJSONObject(newTel, "base_tel_province"), parseJSONObject(newTel, "base_tel_city"), parseJSONObject(newTel, "base_tel_operator")))
          }
        }

        //        创建最新的JSON
        val baseTel: String = udfJson(telInfo).toString
        //        结果
        (newCertNo, baseTel)
      })
      .toDF("base_cert_no", "base_tel")

    //    结果
//    toHBase(result, "label_person", "base_info", "base_cert_no")
    result.show()
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
      .selectExpr("insured_cert_no", "insured_name", "insured_mobile")

    otherResult

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
    * 读取hbase中的标签
    * @param sc
    * @param hiveContext
    * @return
    */
  def hbaseInfos(sc: SparkContext, hiveContext: HiveContext): DataFrame = {
    import hiveContext.implicits._

    /**
      * 读取hbase中的部分标签
      */
    val hbaseData: DataFrame = getHbaseBussValue(sc, "label_person")
      .map(line => {
        val key: String = Bytes.toString(line._2.getRow)
        val baseTel: String = Bytes.toString(line._2.getValue("base_info".getBytes, "base_tel".getBytes))
        //        结果
        (key, baseTel)
      })
      .toDF("cert_no", "base_tel")

    //    结果
    hbaseData

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

  /**
    * 自定义拼接Json
    * @param tuples
    * @return
    */
  def udfJson(tuples: ListBuffer[(String, String, String, String)]): String = {
    //    创建手机号信息List
    val list: util.List[util.HashMap[String, String]] = new util.ArrayList[util.HashMap[String, String]]()
    //    遍历迭代器
    for (t <- tuples) {
      //      当手机信息都不为空时全部塞进去，只手机号不为空时就把手机号塞进去，都为空时什么都不塞进去
      if (t._1 != null && t._2 != null && t._3 != null && t._4 != null) {
        val map: util.HashMap[String, String] = new util.HashMap[String, String]()
        map.put("base_tel_name", t._1)
        map.put("base_tel_province", t._2)
        map.put("base_tel_city", t._3)
        map.put("base_tel_operator", t._4)
        list.add(map)
      } else if (t._1 != null && t._2 == null && t._3 == null && t._4 == null) {
        val map: util.HashMap[String, String] = new util.HashMap[String, String]()
        map.put("base_tel_name", t._1)
        list.add(map)
      }
    }
    //    将嵌套数组改为Json格式字符串
    val jsonString: String = JSON.toJSONString(list, SerializerFeature.BeanToArray)
    //    如果Json格式字符串为[]，则转为null
    if (jsonString == "[]") null else jsonString

  }

  /**
    * 解析JSON
    * @param string
    * @return
    */
  def parse(string: String): Array[JSONObject] = {
    val parseRes = JSON.parseArray(string)
    parseRes.toArray().map(x => JSON.parseObject(x.toString))
  }

  /**
    * 解析JSON字符串防止NullPointException
    * @param js
    * @param str
    * @return
    */
  def parseJSONObject(js: JSONObject, str: String): String = {
    if (js.get(str) == null) null else js.get(str).toString
  }

}
