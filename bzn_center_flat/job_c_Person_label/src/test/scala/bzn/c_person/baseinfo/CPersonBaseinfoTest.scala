package bzn.c_person.baseinfo

import java.text.SimpleDateFormat
import java.util.Date
import java.util.regex.Pattern

import bzn.c_person.util.SparkUtil
import bzn.job.common.Until
<<<<<<< HEAD
import com.alibaba.fastjson.JSONObject
import org.apache.spark.sql.{DataFrame, SaveMode}
=======
import org.apache.spark.sql.DataFrame
>>>>>>> aaa4183d4f0e91940f36c0398a15fe7b4a98e896
import org.apache.spark.sql.hive.HiveContext

/**
  * author:xiaoYuanRen
  * Date:2019/7/10
  * Time:9:27
  * describe: c端标签基础信息类
  **/
object CPersonBaseinfoTest extends SparkUtil with Until {

  def main(args: Array[String]): Unit = {

    //    初始化设置
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf = sparkConfInfo(appName, "local[*]")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4

    //    标签信息整理
    val certInfo: DataFrame = getAllCertInfo(hiveContext)
    val telInfo: DataFrame = getAllTelInfo(hiveContext)
    val habitInfo: DataFrame = getHabitInfo(hiveContext)
    val childInfo: DataFrame = getChildInfo(hiveContext)

    certInfo.show()
    telInfo.show()
    habitInfo.show()
    childInfo.show()

    //    标签信息合并
    val result: DataFrame = unionAllTable(certInfo, telInfo, habitInfo, childInfo)
//    result.write.mode(SaveMode.Overwrite).saveAsTable("label.base_label")
    result.show()
    sc.stop()

  }

  /**
    * 获取官网（投保人、被保人）所有的证件号等信息
    * @param hiveContext
    * @return 证件号DataFrame
    */
  def getAllCertInfo(hiveContext: HiveContext): DataFrame = {
    import hiveContext.implicits._
    hiveContext.udf.register("getEmptyString", () => "")
    hiveContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")//设置日期格式
      val data = df.format(new Date())// new Date()为获取当前系统时间
      (data + "")
    })

    /**
      * 读取被保险人Master的hive表
      */
    val insuredInfo: DataFrame = hiveContext.sql("select insured_cert_no, insured_cert_type, insured_name, is_married, email, " +
      " getEmptyString() as bank_cert_no, getEmptyString() as bank_name from odsdb.ods_policy_insured_detail")
      .where("insured_cert_type = '1' and length(insured_cert_no) > 0")
      .selectExpr("insured_cert_no as base_cert_no", "insured_name as base_name", "is_married as base_married", "email as base_email",
        "bank_cert_no as base_bank_code", "bank_name as base_bank_name")
      .limit(10)

    /**
      * 读取被保人Slave的hive表
      */
    val slaveInfo: DataFrame = hiveContext.sql("select slave_cert_no, slave_cert_type, slave_name, is_married, email, " +
      " getEmptyString() as bank_cert_no, getEmptyString() as bank_name from odsdb.ods_policy_insured_slave_detail")
      .where("slave_cert_type = '1' and length(slave_cert_no) > 0")
      .selectExpr("slave_cert_no as base_cert_no", "slave_name as base_name", "is_married as base_married", "email as base_email",
        "bank_cert_no as base_bank_code", "bank_name as base_bank_name")
      .limit(10)

    /**
      * 读取投保人的hive表
      */
    val holderInfo: DataFrame = hiveContext.sql("select holder_cert_no, holder_cert_type, holder_name, getEmptyString() as base_married," +
      "email, bank_card_no, bank_name from odsdb.ods_holder_detail")
      .where("holder_cert_type = 1 and length(holder_cert_no) > 0")
      .selectExpr("holder_cert_no as base_cert_no", "holder_name as base_name", "base_married", "email as base_email",
        "bank_card_no as base_bank_code", "bank_name as base_bank_name")
      .limit(10)

    //    获得全部身份信息
    val peopleInfo: DataFrame = insuredInfo
      .unionAll(slaveInfo)
      .unionAll(holderInfo)
      .dropDuplicates(Array("base_cert_no"))
      .filter(!$"base_cert_no".contains("*"))
//      .where("base_cert_no <> '11**************15'")

    val peopleInfoTemp = peopleInfo
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
        val baseAgeTime: String = if (baseCertNo == 18) {
          val ageTime: Int = baseCertNo.substring(8, 10).toInt
          ageTime match {
            case _ if (ageTime >= 30 && ageTime < 40) => "30后"
            case _ if (ageTime >= 40 && ageTime < 50) => "40后"
            case _ if (ageTime >= 50 && ageTime < 60) => "50后"
            case _ if (ageTime >= 60 && ageTime < 70) => "60后"
            case _ if (ageTime >= 70 && ageTime < 80) => "70后"
            case _ if (ageTime >= 80 && ageTime < 90) => "80后"
            case _ if (ageTime >= 90 && ageTime < 100) => "90后"
            case _ if (ageTime >= 0 && ageTime < 10) => "00后"
            case _ if (ageTime >= 10 && ageTime < 20) => "10后"
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
          if (baseAgeTemp <= 60) "退休" else if (baseAgeTemp > 60) "未退休" else null
        } else null
        //        邮箱
        val baseEmailTemp: String = line.getAs[String]("base_email")
        val baseEmail: String = dropEmpty(baseEmailTemp)
        //        是否结婚
        val baseMarriedTemp: String = line.getAs[String]("base_married")
        val baseMarried: String = dropEmpty(baseMarriedTemp)
        //        银行卡卡号
        val baseBankCodeTemp: String = line.getAs[String]("base_bank_code")
        val baseBankCode: String = dropEmpty(baseBankCodeTemp)
        //        开户行
        val baseBankNameTemp: String = line.getAs[String]("base_bank_name")
        val baseBankName: String = dropEmpty(baseBankNameTemp)
        //        籍贯码表id
        val nativePlaceId: String = if (baseCertNo.length == 18) {
          baseCertNo.substring(0, 6)
        } else null
        //        星座码表id
        val constellatoryId: String = if (baseCertNo.length == 18) {
          getConstellation(baseCertNo.substring(10, 12), baseCertNo.substring(12, 14))
        } else null

        //        结果
        (baseCertNo, baseName, baseGender, baseBirthday, baseAge, baseAgeTime, baseAgeSection, baseIsRetire, baseEmail,
          baseMarried, baseBankCode, baseBankName, nativePlaceId, constellatoryId)

      })
      .toDF("base_cert_no", "base_name", "base_gender", "base_birthday", "base_age", "base_age_time", "base_age_section",
        "base_is_retire", "base_email", "base_married", "base_bank_code", "base_bank_name", "native_place_id", "constellatory_id")

    /**
      * 读取地区码表
      */
    val areaInfoDimension: DataFrame = hiveContext.sql("select * from odsdb.ods_area_info_dimension")

    //    个人信息关联区域码表
    val peopleInfoJoin: DataFrame = peopleInfoTemp
      .join(areaInfoDimension, peopleInfoTemp("native_place_id") === areaInfoDimension("code"), "leftouter")
      .selectExpr("base_cert_no", "base_name", "base_gender", "base_birthday", "base_age", "base_age_time", "base_age_section",
        "base_is_retire", "base_email", "base_married", "base_bank_code", "base_bank_name", "province as base_province",
        "short_name as base_city", "city_region as base_area", "is_coastal as base_coastal", "city_type as base_city_type",
        "weather_feature as base_weather_feature", "weather_type as base_city_weather", "city_deit as base_city_deit", "constellatory_id")

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
    val peopleInfoRes: DataFrame = peopleInfoJoin
      .join(constellationDimension, peopleInfoJoin("constellatory_id") === constellationDimension("id"), "leftouter")
      .selectExpr("base_cert_no", "base_name", "base_gender", "base_birthday", "base_age", "base_age_time", "base_age_section",
        "base_is_retire", "base_email", "base_married", "base_bank_code", "base_bank_name", "base_province", "base_city",
        "base_area", "base_coastal", "base_city_type", "base_weather_feature", "base_city_weather", "base_city_deit",
        "base_cons_name", "base_cons_type", "base_cons_character")

    //    结果
    peopleInfoRes

  }

  /**
    * 获取全部手机号信息
    * @param hiveContext
    * @return
    */
  def getAllTelInfo(hiveContext: HiveContext): DataFrame = {
    import hiveContext.implicits._

    /**
      * 从被保险人读取hive表
      */
    val insuredTel: DataFrame = hiveContext.sql("select insured_cert_no, insured_cert_type, insured_mobile from odsdb.ods_policy_insured_detail")
      .where("insured_cert_type = '1' and length(insured_cert_no) > 0")
      .selectExpr("insured_cert_no as base_cert_no", "insured_mobile as base_mobile")
      .limit(10)

    /**
      * 从投保人读取hive表
      */
    val holderTel: DataFrame = hiveContext.sql("select holder_cert_no, holder_cert_type, mobile from odsdb.ods_holder_detail")
      .where("holder_cert_type = 1 and length(holder_cert_no) > 0")
      .selectExpr("holder_cert_no as base_cert_no", "mobile as base_mobile")
      .limit(10)

    //    获得全部手机号信息
    val TelInfoTemp: DataFrame = insuredTel
      .unionAll(holderTel)
      .dropDuplicates(Array("base_cert_no", "base_mobile"))

    TelInfoTemp

  }

  /**
    * 获得证件号与爱好信息
    * @param hiveContext
    * @return DataFrame
    */
  def getHabitInfo(hiveContext: HiveContext): DataFrame = {
    import hiveContext.implicits._

    /**
      * 从被保人主表获取身份证件号与保单号
      */
    val insuredInfo: DataFrame = hiveContext.sql("select insured_cert_no, insured_cert_type, policy_id from odsdb.ods_policy_insured_detail")
      .where("insured_cert_type = '1' and length(insured_cert_no) > 0")
      .selectExpr("insured_cert_type as base_cert_no", "policy_id")
      .limit(10)

    /**
      * 从保单表获取保单号与产品信息
      */
    val productInfo: DataFrame = hiveContext.sql("select policy_id, product_name from odsdb.ods_policy_detail")
      .where("length(policy_id) > 0")
      .selectExpr("policy_id as policy_id_temp", "product_name")
      .limit(10)

    //    将产品表与被保险人表关联
    val habitJoin: DataFrame = productInfo
      .join(insuredInfo, productInfo("policy_id_temp") === insuredInfo("policy_id"), "leftouter")
      .selectExpr("base_cert_no", "product_name")
      .limit(10)

    //    计算每个被保险人的爱好
    val habitRes: DataFrame = habitJoin
      .map(line => {
        val baseCertNo: String = line.getAs[String]("base_cert_no")
        val productName: String = line.getAs[String]("product_name")
        //        获取爱好字段
        val habitName: String = if (productName == null) "无"
        else if (productName.contains("骑行")) "骑行"
        else if (productName.contains("足球")) "足球"
        else if (productName.contains("游泳")) "游泳"
        else if (productName.contains("篮球")) "篮球"
        else if (productName.contains("滑雪")) "滑雪"
        else if (productName.contains("滑冰")) "滑冰"
        else if (productName.contains("铁人三项")) "铁人三项"
        else if (productName.contains("马拉松")) "马拉松"
        else if (productName.contains("羽毛球")) "羽毛球"
        else if (productName.contains("登山")) "登山"
        else "无"
        //        结果
        ((baseCertNo, habitName), 1)
      })
      .reduceByKey(_ + _) //计算购买特定产品次数
      .filter(x => x._1._2 != "无" && x._2 >= 3)  //获取特定产品购买三次以上的
      .map(x => (x._1._1, x._1._2))
      .groupByKey()   //获取购买三次的作为爱好
      .map(line => {
      val baseCertNo: String = line._1
      //      创建Json装箱爱好
      val json_value: JSONObject = new JSONObject()
      //        获取迭代器
      val it: Iterator[String] = line._2.iterator
      while (it.hasNext) json_value.put("habit", it.next())
      //        结果
      (baseCertNo, json_value.toString)
    })
      .toDF("base_cert_no", "base_habit")

    //    返回身份证与爱好Json
    habitRes

  }

  /**
    * 获取全部子女的信息
    * @param hiveContext
    * @return dataframe
    */
  def getChildInfo(hiveContext: HiveContext): DataFrame = {
    import hiveContext.implicits._

    /**
      * 读取主被保险人hive表
      */
    val insuredInfo: DataFrame = hiveContext.sql("select insured_cert_no, insured_cert_type, insured_id from odsdb.ods_policy_insured_detail")
      .where("insured_cert_type = '1' and length(insured_cert_no) > 0")
      .selectExpr("insured_cert_no", "insured_id")
      .limit(10)

    /**
      * 读取从被保险人hive表
      */
    val slaveInfo: DataFrame = hiveContext.sql("select slave_cert_no, slave_cert_type, master_id from odsdb.ods_policy_insured_slave_detail")
      .where("slave_cert_type = '1' and length(slave_cert_no) > 0")
      .selectExpr("slave_cert_no", "master_id")
      .limit(10)

    //    通过从属关系获取子女信息
    val childrenInfoOne: DataFrame = insuredInfo
      .join(slaveInfo, insuredInfo("insured_id") === slaveInfo("master_id"))
      .map(line => {
        val insuredCertNo: String = line.getAs[String]("insured_cert_no")
        val slaveCertNo: String = line.getAs[String]("slave_cert_no")
        //        结果
        (insuredCertNo, slaveCertNo)
      })
      .groupByKey()
      .map((value: (String, Iterable[String])) => {
        val insuredCertNo: String = value._1
        //        定义计数器
        var count: Int = 0
        //        定义Json
        var childAge: JSONObject = new JSONObject()
        var childAttendSch: JSONObject = new JSONObject()
        //        读取并计算数据
        val childernCertNo = value._2.toArray.distinct
        count = childernCertNo.size
        for (childCertNo <- childernCertNo) {
          val time: Date = new Date()
          val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          //          计算两个字段
          val age: String = getAgeFromBirthTime(childCertNo, sdf.format(time)).toString
          val isAttendSch: String = isAttendSchool(age)
          //          写入Json
          childAge.put(childCertNo, age)
          childAttendSch.put(childCertNo, isAttendSch)
        }
        //        结果
        (insuredCertNo, count.toString, childAge.toString, childAttendSch.toString)

      })
      .toDF("base_cert_no", "base_child_cun", "base_child_age", "base_child_attend_sch")

    /**
      * 读取投保人hive表
      */
    val holderInfos: DataFrame = hiveContext.sql("select holder_cert_no, holder_cert_type, policy_id from odsdb.ods_holder_detail")
      .where("holder_cert_type = 1 and length(holder_cert_no) > 0")
      .selectExpr("holder_cert_no", "policy_id as holder_policy_id")
      .limit(10)

    /**
      * 读取保单明细hive表
      */
    val policyDetail: DataFrame = hiveContext.sql("select policy_id, product_name from odsdb.ods_policy_detail")
      .where("product_name = '学幼险' or product_name = '信美相互爱我宝贝少儿白血病保险'")
      .selectExpr("policy_id as policy_detail_id")
      .limit(10)

    /**
      * 读取被保险人hive表
      */
    val insuredInfos: DataFrame = hiveContext.sql("select insured_cert_no, insured_cert_type, policy_id from odsdb.ods_policy_insured_detail")
      .where("insured_cert_type = '1' and length(insured_cert_no) > 0")
      .selectExpr("insured_cert_no", "policy_id as insured_policy_id")
      .limit(10)

    //    通过投被保人关系确定子女关系
    val childrenInfoTemp: DataFrame = insuredInfos
      .join(policyDetail, insuredInfos("insured_policy_id") === policyDetail("policy_detail_id"))
      .selectExpr("insured_cert_no", "policy_detail_id")

    val childrenInfoTwo: DataFrame = childrenInfoTemp
      .join(holderInfos, childrenInfoTemp("policy_detail_id") === holderInfos("holder_policy_id"))
      .map(line => {
        val holderCertNo: String = line.getAs[String]("holder_cert_no")
        val insuredCertNo: String = line.getAs[String]("insured_cert_no")
        //        结果
        (holderCertNo, insuredCertNo)
      })
      .groupByKey()
      .map((value: (String, Iterable[String])) => {
        val holderCertNo: String = value._1
        //        定义计数器
        var count: Int = 0
        //        定义Json
        var childAge: JSONObject = new JSONObject()
        var childAttendSch: JSONObject = new JSONObject()
        //        读取并计算数据
        val childernCertNo = value._2.toArray.distinct
        count = childernCertNo.size
        for (childCertNo <- childernCertNo) {
          val time: Date = new Date()
          val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          //          计算两个字段
          val age: String = getAgeFromBirthTime(childCertNo, sdf.format(time)).toString
          val isAttendSch: String = isAttendSchool(age)
          //          写入Json
          childAge.put(childCertNo, age)
          childAttendSch.put(childCertNo, isAttendSch)
        }
        //        结果
        (holderCertNo, count.toString, childAge.toString, childAttendSch.toString)

      })
      .toDF("base_cert_no", "base_child_cun", "base_child_age", "base_child_attend_sch")

    val childernInfoRes: DataFrame = childrenInfoOne
      .unionAll(childrenInfoTwo)
      .map(line => {
        //        身份证号
        val baseCertNoTemp: String = line.getAs[String]("base_cert_no")
        val baseCertNo: String = dropEmpty(baseCertNoTemp)
        //        子女数量
        val baseChildCunTemp: String = line.getAs[String]("base_child_cun")
        val baseChildCun: String = dropEmpty(baseChildCunTemp)
        //        子女年龄
        val baseChildAgeTemp: String = line.getAs[String]("base_child_age")
        val baseChildAge: String = dropEmpty(baseChildAgeTemp)
        //        子女是否上学
        val baseChildAttendSchTemp: String = line.getAs[String]("base_child_attend_sch")
        val baseChildAttendSch: String = dropEmpty(baseChildAttendSchTemp)
        //        结果
        (baseCertNo, baseChildCun, baseChildAge, baseChildAttendSch)
      })
      .toDF("base_cert_no", "base_child_cun", "base_child_age", "base_child_attend_sch")

    //    结果
    childernInfoRes

  }

  /**
    * 合并所有表
    * @param certInfo
    * @param telInfo
    * @param habitInfo
    * @param childInfo
    * @return DataFrame
    */
  def unionAllTable(certInfo: DataFrame, telInfo: DataFrame, habitInfo: DataFrame, childInfo: DataFrame): DataFrame = {

    val telInfos: DataFrame = telInfo.withColumnRenamed("base_cert_no", "tel_cert_no")

    val habitInfos: DataFrame = habitInfo.withColumnRenamed("base_cert_no", "habit_cert_no")

    val childInfos: DataFrame = childInfo.withColumnRenamed("base_cert_no", "child_cert_no")

    //    多表关联
    val result: DataFrame = certInfo
      .join(telInfos, certInfo("base_cert_no") === telInfos("tel_cert_no"), "leftouter")
      .join(habitInfos, certInfo("base_cert_no") === habitInfos("habit_cert_no"), "leftouter")
      .join(childInfos, certInfo("base_cert_no") === childInfos("child_cert_no"), "leftouter")
      .selectExpr("base_cert_no", "base_name", "base_gender", "base_birthday", "base_age", "base_age_time", "base_age_section",
        "base_is_retire", "base_email", "base_married", "base_bank_code", "base_bank_name as base_bank_deposit", "base_province", "base_city",
        "base_area", "base_coastal", "base_city_type", "base_weather_feature", "base_city_weather", "base_city_deit",
        "base_cons_name", "base_cons_type", "base_cons_character", "base_mobile", "base_habit", "base_child_cun",
        "base_child_age", "base_child_attend_sch")

    //    结果
    result

  }

  /**
    * 将空字符串、空值转换为NULL
    * @param Temp
    * @return
    */
  def dropEmpty(Temp: String): String = {
    if (Temp == "" && Temp == "NULL" && Temp == null) null else Temp
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

  def isAttendSchool(age: String): String = {
    val ageInt: Int = age.toInt
    var isAttendSch: String = null
    if (ageInt >= 6 && ageInt <= 15) {
      isAttendSch =  "上学"
    } else {
      isAttendSch =  "未上学"
    }
    isAttendSch
  }

}
