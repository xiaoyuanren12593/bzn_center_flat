package c_person.highinfo

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import bzn.job.common.{HbaseUtil, Until}
import c_person.util.SparkUtil
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.hive.HiveContext

/**
  * author:sangJiaQi
  * Date:2019/8/5
  * describe: 高级标签增量部分
  */
object CPersonHighInfoIncrement extends SparkUtil with Until with HbaseUtil {

  def main(args: Array[String]): Unit = {

//    初始化设置
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf = sparkConfInfo(appName, "")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4

    //    更新数据
    val peopleInfo: DataFrame = newPolicyId(hiveContext)

    updateSportRateInfo(hiveContext)
    updateTravelInfo(hiveContext)
    updatePerceiveInfo(hiveContext, peopleInfo)

    sc.stop()

  }

  /**
    * 清洗体育频率
    * @param hiveContext
    * @return 体育频率表
    */
  def updateSportRateInfo(hiveContext: HiveContext): Unit = {
    import hiveContext.implicits._

    /**
      * 从被保人主表获取身份证件号与保单号
      */
    val insuredInfo: DataFrame = hiveContext.sql("select insured_cert_no, insured_cert_type, policy_id, start_date, " +
      "update_time from odsdb.ods_policy_insured_detail")
      .where("insured_cert_type = '1' and length(insured_cert_no) = 18")
      .selectExpr("insured_cert_no as high_cert_no", "policy_id", "start_date", "update_time")

    /**
      * 从保单表获取保单号与产品信息
      */
    val policyInfo: DataFrame = hiveContext.sql("select policy_id, product_code from odsdb.ods_policy_detail")
      .where("length(policy_id) > 0")
      .selectExpr("policy_id as policy_id_temp", "product_code")

    /**
      * 从产品表获得产品信息
      */
    val productInfo: DataFrame = hiveContext.sql("select product_code, one_level_pdt_cate from odsdb.ods_product_detail")
      .selectExpr("product_code as product_code_temp", "one_level_pdt_cate as one_level")

    val sportInfoJoin: DataFrame = insuredInfo
      .join(policyInfo, insuredInfo("policy_id") === policyInfo("policy_id_temp"))
      .selectExpr("high_cert_no", "start_date", "update_time", "product_code")

    val sportInfo: DataFrame = sportInfoJoin
      .join(productInfo, sportInfoJoin("product_code") === productInfo("product_code_temp"), "leftouter")
      .selectExpr("high_cert_no", "start_date", "update_time", "one_level")
      .map(line => {
        val highCertNo: String = line.getAs[String]("high_cert_no")
        val startDate: String = line.getAs[String]("start_date")
        val update_time: String = line.getAs[String]("update_time")
        val oneLevel: String = line.getAs[String]("one_level")
        //        获得开始时间
        val start: Timestamp = if (startDate != null) Timestamp.valueOf(startDate) else Timestamp.valueOf(update_time)
        //        获得当前时间90天前的时间
        val sign: Timestamp = getNintyDaysAgo()
        //        结果
        (highCertNo, sign, start, oneLevel)
      })
      .filter(line => (line._2.compareTo(line._3) < 0) && (line._4 == "体育"))
      .map(line => (line._1, 1))
      .reduceByKey(_ + _)
      .map(line => (line._1, line._2.toString))
      .toDF("high_cert_no", "sports_rate")

    //    结果
    toHBase(sportInfo, "label_person", "high_info", "high_cert_no")

  }

  /**
    * 清洗旅游标签
    * @param hiveContext
    * @return 获得旅游表
    */
  def updateTravelInfo(hiveContext: HiveContext): Unit = {
    import hiveContext.implicits._

    /**
      * 从被保人主表获取身份证件号与保单号
      */
    val insuredInfo: DataFrame = hiveContext.sql("select insured_cert_no, insured_cert_type, policy_id, start_date, " +
      "update_time from odsdb.ods_policy_insured_detail")
      .where("insured_cert_type = '1' and length(insured_cert_no) = 18")
      .selectExpr("insured_cert_no as high_cert_no", "policy_id", "start_date", "update_time")

    /**
      * 从保单表获取保单号与产品信息
      */
    val policyInfo: DataFrame = hiveContext.sql("select policy_id, product_code from odsdb.ods_policy_detail")
      .where("length(policy_id) > 0")
      .selectExpr("policy_id as policy_id_temp", "product_code")

    //    将被保险人与保单关联
    val travelInfo: DataFrame = insuredInfo
      .join(policyInfo, insuredInfo("policy_id") === policyInfo("policy_id_temp"))
      .selectExpr("high_cert_no", "start_date", "update_time", "product_code")
      .map(line => {
        val highCertNo: String = line.getAs[String]("high_cert_no")
        val startDate: String = line.getAs[String]("start_date")
        val update_time: String = line.getAs[String]("update_time")
        val productCode: String = line.getAs[String]("product_code")
        //        获得开始时间
        val start: Timestamp = if (startDate != null) Timestamp.valueOf(startDate) else Timestamp.valueOf(update_time)
        //        获得当前时间180天前的时间
        val sign: Timestamp = getHalfYearDaysAgo()
        //        结果
        (highCertNo, sign, start, productCode)
      })
      .filter(line => {
        (line._2.compareTo(line._3) < 0) &&
          (line._4 == "12000001" || line._4 == "12000002" || line._4 == "12000003" || line._4 == "P00001647" || line._4 == "P00001678" || line._4 == "F201700001")
      })
      .map(line => (line._1, 1))
      .reduceByKey(_ + _)
      .map(line => {
        val highCertNo: String = line._1
        val num: Int = line._2
        val value: String = if (num >= 2) "是" else null
        //        返回结果
        (highCertNo, value)
      })
      .filter(line => line._2 != null)
      .toDF("high_cert_no", "love_travel")

    //    结果
    toHBase(travelInfo, "label_person", "high_info", "high_cert_no")

  }

  /**
    * 更新是否感知标签
    * @param hiveContext
    * @param peopleInfo
    */
  def updatePerceiveInfo(hiveContext: HiveContext, peopleInfo: DataFrame): Unit = {
    import hiveContext.implicits._

    /**
      * 读取投保人的hive表
      */
    val holderInfo: DataFrame = hiveContext.sql("select holder_cert_no, holder_cert_type, policy_id from odsdb.ods_holder_detail")
      .where("holder_cert_type = 1 and length(holder_cert_no) = 18")
      .selectExpr("holder_cert_no as high_cert_no", "policy_id")

    /**
      * 读取理赔的hive表
      */
    val claimInfo: DataFrame = hiveContext.sql("select risk_cert_no from odsdb.ods_claims_detail")
      .where("(risk_cert_no) = 18")
      .selectExpr("risk_cert_no as high_cert_no")

    //    合并新的投保人和理赔表
    val perceiveInfo: DataFrame = holderInfo
      .join(peopleInfo, holderInfo("policy_id") === peopleInfo("policy_id_temp"))
      .selectExpr("high_cert_no")
      .unionAll(claimInfo)
      .dropDuplicates(Array("high_cert_no"))
      .map(line => {
        val highCertNo: String = line.getAs[String]("high_cert_no")
        val perceive: String = "是"
        //        结果
        (highCertNo, perceive)
      })
      .toDF("high_cert_no", "is_perceive")

    //    结果
    toHBase(perceiveInfo, "label_person", "high_info", "high_cert_no")

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
      .selectExpr("policy_id as policy_id_temp")

    //    新增保单号
    PolicyId

  }

  /**
    * 获得当前时间九十天前的时间戳
    * @return
    */
  def getNintyDaysAgo(): Timestamp = {
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date: Date = new Date()
    val c: Calendar = Calendar.getInstance
    c.setTime(date)
    c.add(Calendar.DATE, -90)
    val newDate: Date = c.getTime
    Timestamp.valueOf(sdf.format(newDate))
  }

  /**
    * 获得当前时间一百八十天天前的时间戳
    * @return
    */
  def getHalfYearDaysAgo(): Timestamp = {
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date: Date = new Date()
    val c: Calendar = Calendar.getInstance
    c.setTime(date)
    c.add(Calendar.DATE, -180)
    val newDate: Date = c.getTime
    Timestamp.valueOf(sdf.format(newDate))
  }

}
