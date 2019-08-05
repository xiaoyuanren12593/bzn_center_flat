package bzn.c_person.baseinfo

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import bzn.c_person.util.SparkUtil
import bzn.job.common.{HbaseUtil, Until}
import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * author:sangJiaQI
  * Date:2019/7/30
  * describe: 基础标签增量
  */
object CPersonBaseInfoIncrementTest extends SparkUtil with Until with HbaseUtil{

  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName: String = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "local[*]")

    val sc: SparkContext = sparkConf._2
    val hiveContext: HiveContext = sparkConf._4


    sc.stop()

  }


  /**
    * 更新新人员的基础信息
    * @param hiveContext
    * @param policyId
    * @param hbaseInfo
    */
  def updateCertInfo(hiveContext: HiveContext, policyId: DataFrame, hbaseInfo: DataFrame): Unit = {
    import hiveContext.implicits._
    hiveContext.udf.register("getEmptyString", () => "")
    hiveContext.udf.register("dropEmptys", (line: String) => dropEmpty(line))
    hiveContext.udf.register("dropSpecial", (line: String) => dropSpecial(line))
    hiveContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")//设置日期格式
      val data = df.format(new Date())// new Date()为获取当前系统时间
      (data + "")
    })
    hiveContext.udf.register("getNew", (line: String) => {
      if (line == null) true else false
    })

    /**
      * 读取被保人Master的hive表
      */
    val insuredInfo: DataFrame = hiveContext.sql("select insured_cert_no, insured_cert_type, insured_name, is_married, email, " +
      " getEmptyString() as bank_cert_no, getEmptyString() as bank_deposit, policy_id, insured_id from odsdb.ods_policy_insured_detail")

    val newInsuredInfo: DataFrame = insuredInfo
      .join(policyId, insuredInfo("policy_id") === policyId("policy_id_temp"))
      .where("insured_cert_type = '1' and length(insured_cert_no) = 18")
      .selectExpr("insured_cert_no as base_cert_no", "insured_name as base_name", "is_married as base_married", "email as base_email",
        "bank_cert_no as base_bank_code", "bank_deposit as base_bank_deposit", "insured_id")

    /**
      * 读取被保人Slave的hive表
      */
    val slaveInfo: DataFrame = hiveContext.sql("select slave_cert_no, slave_cert_type, slave_name, is_married, email, " +
      " getEmptyString() as bank_cert_no, getEmptyString() as bank_deposit, master_id from odsdb.ods_policy_insured_slave_detail")

    val newSlaveInfo: DataFrame = slaveInfo
      .join(newInsuredInfo, slaveInfo("master_id") === newInsuredInfo("insured_id"))
      .where("slave_cert_type = '1' and length(slave_cert_no) = 18")
      .selectExpr("slave_cert_no as base_cert_no", "slave_name as base_name", "is_married as base_married", "email as base_email",
        "bank_cert_no as base_bank_code", "bank_deposit as base_bank_deposit")

    /**
      * 读取投保人的hive表
      */
    val holderInfo: DataFrame = hiveContext.sql("select holder_cert_no, holder_cert_type, holder_name, getEmptyString() as base_married," +
      "email, bank_card_no, bank_deposit, policy_id from odsdb.ods_holder_detail")

    val newHolderInfo: DataFrame = holderInfo
      .join(policyId, holderInfo("policy_id") === policyId("policy_id_temp"))
      .where("holder_cert_type = 1 and length(holder_cert_no) = 18")
      .selectExpr("holder_cert_no as base_cert_no", "holder_name as base_name", "base_married", "email as base_email",
        "bank_card_no as base_bank_code", "bank_deposit as base_bank_deposit")

//    获得全部新保单的身份信息
    val newInsuredPeople: DataFrame = newInsuredInfo
      .drop("insured_id")
      .unionAll(newSlaveInfo)
      .unionAll(newHolderInfo)
      .filter("dropSpecial(base_cert_no) as base_cert_no")
      .dropDuplicates(Array("base_cert_no"))

//    获取全部新人员的身份信息
    val peopleInfo: DataFrame = newInsuredPeople
      .join(hbaseInfo, newInsuredPeople("base_cert_no") === hbaseInfo("cert_no"), "leftouter")
      .filter("getNew(cert_no) as cert_no")
      .selectExpr("base_cert_no", "base_name", "base_married", "base_email", "base_bank_code", "base_bank_deposit")

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

//    获得新人员全部信息
    val resultTempInfo: DataFrame = peopleInfo
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
        val baseBankDepositTemp: String = line.getAs[String]("base_bank_deposit")
        val baseBankDeposit: String = dropEmpty(baseBankDepositTemp)
        //        籍贯码表id
        val nativePlaceId: String = if (baseCertNo.length == 18) {
          baseCertNo.substring(0, 4) + "00"
        } else null
        //        星座码表id
        val constellatoryId: String = if (baseCertNo.length == 18) {
          getConstellation(baseCertNo.substring(10, 12), baseCertNo.substring(12, 14))
        } else null

        //        结果
        (baseCertNo, baseName, baseGender, baseBirthday, baseAge, baseAgeTime, baseAgeSection, baseIsRetire, baseEmail,
          baseMarried, baseBankCode, baseBankDeposit, nativePlaceId, constellatoryId)

      })
      .toDF("base_cert_no", "base_name", "base_gender", "base_birthday", "base_age", "base_age_time", "base_age_section",
        "base_is_retire", "base_email", "base_married", "base_bank_code", "base_bank_deposit", "native_place_id", "constellatory_id")

//    结果表
    val resultInfo: DataFrame = resultTempInfo
      .join(areaInfoDimension, resultTempInfo("native_place_id") === areaInfoDimension("code"), "leftouter")
      .join(constellationDimension, resultTempInfo("constellatory_id") === constellationDimension("id"), "leftouter")
      .selectExpr("base_cert_no", "base_name", "base_gender", "base_birthday", "base_age", "base_age_time", "base_age_section",
        "base_is_retire", "base_email", "base_married", "base_bank_code", "base_bank_deposit", "base_province", "base_city",
        "base_area", "base_coastal", "base_city_type", "base_weather_feature", "base_city_weather", "base_city_deit",
        "base_cons_name", "base_cons_type", "base_cons_character")

//    结果
    toHBase2(resultInfo, "label_person", "base_info")

  }

  def updateTelInfo(hiveContext: HiveContext, policyId: DataFrame, hbaseInfo: DataFrame): Unit = {
    import hiveContext.implicits._
    hiveContext.udf.register("getEmptyString", () => "")
    hiveContext.udf.register("dropEmptys", (line: String) => dropEmpty(line))
    hiveContext.udf.register("dropSpecial", (line: String) => dropSpecial(line))
    hiveContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")//设置日期格式
      val data = df.format(new Date())// new Date()为获取当前系统时间
      (data + "")
    })
    hiveContext.udf.register("getNew", (line: String) => {
      if (line == null) true else false
    })

    /**
      * 从被保险人读取hive表
      */
    val insuredTel: DataFrame = hiveContext.sql("select insured_cert_no, insured_cert_type, insured_mobile, policy_id from odsdb.ods_policy_insured_detail")

    val newInsuredTel: DataFrame = insuredTel
      .join(policyId, insuredTel("policy_id") === policyId("policy_id_temp"))
      .where("insured_cert_type = '1' and length(insured_cert_no) = 18")
      .selectExpr("insured_cert_no as base_cert_no", "insured_mobile as base_mobile")

    /**
      * 从投保人读取hive表
      */
    val holderTel: DataFrame = hiveContext.sql("select holder_cert_no, holder_cert_type, mobile, policy_id from odsdb.ods_holder_detail")

    val newHolderTel: DataFrame = holderTel
      .join(policyId, holderTel("policy_id") === policyId("policy_id_temp"))
      .where("holder_cert_type = 1 and length(holder_cert_no) = 18")
      .selectExpr("holder_cert_no as base_cert_no", "mobile as base_mobile")

    //    获得全部手机号信息
    val TelInfoTemp: DataFrame = newInsuredTel
      .unionAll(newHolderTel)
      .selectExpr("base_cert_no", "dropEmptys(base_mobile) as base_mobile")
      .dropDuplicates(Array("base_cert_no", "base_mobile"))

    /**
      * 手机号码表
      */
    val mobileInfo: DataFrame = hiveContext.sql("select mobile, province, city, operator from odsdb.ods_mobile_dimension")

    //    获取新人员手机号信息
    val TelInfoAll: DataFrame = TelInfoTemp
      .join(mobileInfo, TelInfoTemp("base_mobile") === mobileInfo("mobile"), "leftouter")
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
    val oldTelInfo: DataFrame = hbaseInfo
      .selectExpr("cert_no as old_cert_no", "base_tel as old_base_tel")

    //    获取全部新保单手机号信息
    val result = TelInfoAll
      .join(oldTelInfo, TelInfoAll("base_cert_no") === oldTelInfo("old_cert_no"), "leftouter")
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
//        将旧手机号塞入List
        if (oldTelArr != null) {
          for (oldTel <- oldTelArr) {
            telInfo += ((oldTel.get("base_tel_name").toString, oldTel.get("base_tel_province").toString, oldTel.get("base_tel_city").toString, oldTel.get("base_tel_operator").toString))
            ifInfo.add(oldTel.get("base_tel_name").toString)
          }
        }
//        将新手机号判断并塞入List
        for (newTel <- newTelArr) {
          if (!ifInfo.contains(newTel.get("base_tel_name").toString)) telInfo += ((newTel.get("base_tel_name").toString, newTel.get("base_tel_province").toString, newTel.get("base_tel_city").toString, newTel.get("base_tel_operator").toString))
        }
//        创建最新的JSON
        val baseTel: String = udfJson(telInfo).toString
//        结果
        (newCertNo, baseTel)
      })
      .toDF("base_cert_no", "base_tel")

//    结果
    toHBase2(result, "label_person", "base_info")

  }


  /**
    * 读取hive中新增保单的policy_id
    * @param hiveContext
    * @return
    */
  def newPolicyId(hiveContext: HiveContext): DataFrame = {
    import hiveContext.implicits._
    hiveContext.udf.register("getEmptyString", () => "")
    hiveContext.udf.register("dropEmptys", (line: String) => dropEmpty(line))
    hiveContext.udf.register("dropSpecial", (line: String) => dropSpecial(line))
    hiveContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")//设置日期格式
      val data = df.format(new Date())// new Date()为获取当前系统时间
      (data + "")
    })

    /**
      * 读取hive中新增保单表
      */
    val PolicyId: DataFrame = hiveContext.sql("select policy_id, inc_type from dwdb.dw_policy_detail_inc")
      .where("inc_type = 0")
      .selectExpr("policy_id_temp")

//    新增保单号
    PolicyId

  }

  /**
    * 读取hbase中的标签
    * @param sc
    * @param hiveContext
    * @return
    */
  def hbaseInfo(sc: SparkContext, hiveContext: HiveContext): DataFrame = {
    import hiveContext.implicits._

    /**
      * 读取hbase中的部分标签
      */
    val hbaseData: DataFrame = getHbaseBussValue(sc, "label_person")
      .map(line => {
        val key: String = Bytes.toString(line._2.getRow)
        val baseTel: String = Bytes.toString(line._2.getValue("base_info".getBytes, "base_tel".getBytes))
        val baseHabit: String = Bytes.toString(line._2.getValue("base_info".getBytes, "base_habit".getBytes))
        val baseChildCun: String = Bytes.toString(line._2.getValue("base_info".getBytes, "base_child_cun".getBytes))
        val baseChildAge: String = Bytes.toString(line._2.getValue("base_info".getBytes, "base_child_age".getBytes))
        val baseChildAttendSch: String = Bytes.toString(line._2.getValue("base_info".getBytes, "base_child_attend_sch".getBytes))
//        结果
        (key, baseTel, baseHabit, baseChildCun, baseChildAge, baseChildAttendSch)
      })
      .toDF("cert_no", "base_tel", "base_habit", "base_child_cun", "base_child_age", "base_child_attend_sch")

//    结果
    hbaseData

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

      saveToHbase(hbaseRDD, columnFamily,x, conf_fs, tableName, conf)
    })
  }

}
