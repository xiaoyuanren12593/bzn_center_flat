package bzn.c_person.highinfo

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import c_person.util.SparkUtil
import bzn.job.common.Until
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

object CPersonHighInfoTest extends SparkUtil with Until{

  def main(args: Array[String]): Unit = {

    //    初始化设置
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf = sparkConfInfo(appName, "local[*]")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4

//    清洗标签
//    val certNoInfo: DataFrame = getCertNoInfo(hiveContext)
    val sportRateInfo: DataFrame = getSportRateInfo(hiveContext)
    val travelInfo: DataFrame = getTravelInfo(hiveContext)
//    val perceiveInfo: DataFrame = getPerceiveInfo(hiveContext)

    sportRateInfo.show()
    travelInfo.show()

//    合并表
//    val result: DataFrame = getUnion(hiveContext, certNoInfo, sportRateInfo, travelInfo, perceiveInfo)

//    result.show()

  }

  /**
    * 获得全部身份证号
    * @param hiveContext
    * @return 证件号DataFrame
    */
  def getCertNoInfo(hiveContext: HiveContext): DataFrame = {
    import hiveContext.implicits._

    /**
      * 读取被保险人Master的hive表
      */
    val insuredInfo: DataFrame = hiveContext.sql("select insured_cert_no, insured_cert_type from odsdb.ods_policy_insured_detail")
      .where("insured_cert_type = '1' and length(insured_cert_no) = 18")
      .selectExpr("insured_cert_no as high_cert_no")
      .limit(10)

    /**
      * 读取被保人Slave的hive表
      */
    val slaveInfo: DataFrame = hiveContext.sql("select slave_cert_no, slave_cert_type from odsdb.ods_policy_insured_slave_detail")
      .where("slave_cert_type = '1' and length(slave_cert_no) = 18")
      .selectExpr("slave_cert_no as high_cert_no")
      .limit(10)

    /**
      * 读取投保人的hive表
      */
    val holderInfo: DataFrame = hiveContext.sql("select holder_cert_no, holder_cert_type from odsdb.ods_holder_detail")
      .where("holder_cert_type = 1 and length(holder_cert_no) = 18")
      .selectExpr("holder_cert_no as high_cert_no")
      .limit(10)

    //    获得全部身份信息
    val peopleInfo: DataFrame = insuredInfo
      .unionAll(slaveInfo)
      .unionAll(holderInfo)
      .dropDuplicates(Array("high_cert_no"))
      .filter(!$"high_cert_no".contains("*"))

//    结果
    peopleInfo

  }

  /**
    * 清洗体育频率
    * @param hiveContext
    * @return 体育频率表
    */
  def getSportRateInfo(hiveContext: HiveContext): DataFrame = {
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
      .limit(10)

    /**
      * 从产品表获得产品信息
      */
    val productInfo: DataFrame = hiveContext.sql("select product_code, one_level_pdt_cate from odsdb.ods_product_detail")
      .selectExpr("product_code as product_code_temp", "one_level_pdt_cate as one_level")

    val sportInfoJoin: DataFrame = insuredInfo
      .join(policyInfo, insuredInfo("policy_id") === policyInfo("policy_id_temp"))
      .selectExpr("high_cert_no", "start_date", "update_time", "product_code")

    val sparkInfo: DataFrame = sportInfoJoin
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
    sparkInfo

  }

  /**
    * 清洗旅游标签
    * @param hiveContext
    * @return 获得旅游表
    */
  def getTravelInfo(hiveContext: HiveContext): DataFrame = {
    import hiveContext.implicits._

    /**
      * 从被保人主表获取身份证件号与保单号
      */
    val insuredInfo: DataFrame = hiveContext.sql("select insured_cert_no, insured_cert_type, policy_id, start_date, " +
      "update_time from odsdb.ods_policy_insured_detail")
      .where("insured_cert_type = '1' and length(insured_cert_no) = 18")
      .selectExpr("insured_cert_no as high_cert_no", "policy_id", "start_date", "update_time")
      .limit(10)

    /**
      * 从保单表获取保单号与产品信息
      */
    val policyInfo: DataFrame = hiveContext.sql("select policy_id, product_code from odsdb.ods_policy_detail")
      .where("length(policy_id) > 0")
      .selectExpr("policy_id as policy_id_temp", "product_code")
      .limit(10)

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
      .groupByKey()
      .map(line => {
        val highCertNo: String = line._1
        val value: String = if (line._2.toArray.length >= 6) "是" else "否"
//        返回结果
        (highCertNo, value)
      })
      .toDF("high_cert_no", "love_travel")

//    结果
    travelInfo

  }

  /**
    * 获得有感知的人
    * @param hiveContext
    * @return 有感知人的表
    */
  def getPerceiveInfo(hiveContext: HiveContext): DataFrame = {
    import hiveContext.implicits._

    /**
      * 读取投保人的hive表
      */
    val holderInfo: DataFrame = hiveContext.sql("select holder_cert_no, holder_cert_type from odsdb.ods_holder_detail")
      .where("holder_cert_type = 1 and length(holder_cert_no) = 18")
      .selectExpr("holder_cert_no as high_cert_no")
      .limit(10)

    /**
      * 读取理赔的hive表
      */
    val claimInfo: DataFrame = hiveContext.sql("select risk_cert_no from odsdb.ods_claims_detail")
      .where("(risk_cert_no) = 18")
      .selectExpr("risk_cert_no as high_cert_no")
      .limit(10)

//    获得感知标签
    val perceiveInfo: DataFrame = holderInfo
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
    perceiveInfo

  }

  /**
    * 合并表格
    * @param hiveContext
    * @param certNoInfo
    * @param sportRateInfo
    * @param travelInfo
    * @param perceiveInfo
    * @return
    */
  def getUnion(hiveContext: HiveContext, certNoInfo: DataFrame, sportRateInfo: DataFrame, travelInfo: DataFrame, perceiveInfo: DataFrame): DataFrame = {
    import hiveContext.implicits._
    hiveContext.udf.register("dropEmptys", (line: String) => dropEmpty(line))

    val sportRateInfos: DataFrame = sportRateInfo.withColumnRenamed("high_cert_no", "sport_cert_no")

    val travelInfos: DataFrame = travelInfo.withColumnRenamed("high_cert_no", "travel_cert_no")

    val perceiveInfos: DataFrame = perceiveInfo.withColumnRenamed("high_cert_no", "perceive_cert_no")

//    关联表
    val result: DataFrame = certNoInfo
      .join(sportRateInfos, certNoInfo("high_cert_no") === sportRateInfos("sport_cert_no"), "leftouter")
      .join(travelInfos, certNoInfo("high_cert_no") === travelInfos("travel_cert_no"), "leftouter")
      .join(perceiveInfos, certNoInfo("high_cert_no") === perceiveInfos("perceive_cert_no"), "leftouter")
      .selectExpr("high_cert_no", "dropEmptys(sports_rate) as sports_rate", "dropEmptys(love_travel) as love_travel",
        "dropEmptys(is_perceive) as is_perceive")

//    结果
    result

  }

  /**
    * 获得当前时间九十天前的时间戳
    * @return
    */
  def getNintyDaysAgo(): Timestamp = {
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dateStr: String = sdf.format(new Date())
    val date: Date = sdf.parse(dateStr)
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
    val dateStr: String = sdf.format(new Date())
    val date: Date = sdf.parse(dateStr)
    val c: Calendar = Calendar.getInstance
    c.setTime(date)
    c.add(Calendar.DATE, -180)
    val newDate: Date = c.getTime
    Timestamp.valueOf(sdf.format(newDate))
  }

}
