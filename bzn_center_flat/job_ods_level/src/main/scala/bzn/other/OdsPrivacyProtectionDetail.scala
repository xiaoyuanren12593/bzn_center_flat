package bzn.other

import java.text.SimpleDateFormat
import java.util.Date

import bzn.job.common.Until
import bzn.util.SparkUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext


object OdsPrivacyProtectionDetail extends SparkUtil with Until {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName: String = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "")

    val sc: SparkContext = sparkConf._2
    val hiveContext: HiveContext = sparkConf._4
    val res1 = IncrementOfficial(hiveContext)
    val res2 = IncrementInter(hiveContext)
    val res3 = IncrementWedding(hiveContext)
    val res = res1.unionAll(res2).unionAll(res3)

    hiveContext.setConf("hive.exec.dynamic.partition", "true")
    hiveContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

    res.write.mode(SaveMode.Append).format("PARQUET").partitionBy("business_line", "years")
      .saveAsTable("odsdb.ods_md5_message_detail")
    sc.stop()
  }


  /**
   * 官网
   *
   * @param hiveContext
   * @return
   */
  def IncrementOfficial(hiveContext: HiveContext): DataFrame = {
    import hiveContext.implicits._
    hiveContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    hiveContext.udf.register("MD5", (str: String) => MD5(str))
    hiveContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      //设置日期格式
      val date = df.format(new Date()) // new Date()为获取当前系统时间
      (date + "")
    })

    //官网数据
    val officialTemp =
      hiveContext.sql("select " +
        "MD5(concat(insured_name,insured_cert_no)) as id," +
        "insured_cert_no,MD5(insured_cert_no) as insured_cert_no_md5," +
        "insured_mobile, MD5(insured_mobile) as insured_mobile_md5," +
        "business_line,years from odsdb.ods_all_business_person_base_info_detail where business_line = 'official'")

    //读取信息表,判断官网增量数据
    val odsMd5MessageOfficial = hiveContext.sql("select insured_cert_no as insured_cert_no_salve," +
      "insured_cert_no_md5 as insured_cert_no_md5_salve,insured_mobile as insured_mobile_salve," +
      "insured_mobile_md5 as insured_mobile_md5_salve,business_line as business_line_salve," +
      "years as years_salve from odsdb.ods_md5_message_detail where business_line='official'")

    //过滤出官网无效信息
    val official = officialTemp.select("id", "insured_cert_no", "insured_cert_no_md5", "insured_mobile", "insured_mobile_md5", "business_line", "years")
      .where("insured_cert_no is not null or insured_mobile is not null")

    //拿出官网不同类型的数据进行关联 手机号和证件号都不为空的
    val official1 = official.select("id", "insured_cert_no", "insured_cert_no_md5", "insured_mobile", "insured_mobile_md5", "business_line", "years")
      .where("insured_cert_no is not null and insured_mobile is not null")

    //官网数据身份证号和手机号不为空的增量
    val officialres1 = odsMd5MessageOfficial.select("insured_cert_no_salve", "insured_cert_no_md5_salve",
      "insured_mobile_salve", "business_line_salve", "years_salve")
      .where("insured_cert_no_salve is not null and insured_mobile_salve is not null")

    //手机号和证件号不为空的增量
    val officialRes1 = official1.join(officialres1,
      'insured_cert_no === 'insured_cert_no_salve and 'insured_mobile === 'insured_mobile_salve, "leftouter")
      .select("id", "insured_cert_no", "insured_cert_no_salve", "insured_cert_no_md5",
        "insured_mobile", "insured_mobile_salve", "insured_mobile_md5", "business_line", "years")
      .where("insured_cert_no_salve is null and insured_mobile_salve is null")

    //证件号不为空,手机号为空
    val official2 = official.select("id", "insured_cert_no", "insured_cert_no_md5", "insured_mobile", "insured_mobile_md5", "business_line", "years")
      .where("insured_cert_no is not null and insured_mobile is null")

    //身份证号不为空,手机号为空的增量
    val officialres2 = odsMd5MessageOfficial.select("insured_cert_no_salve", "insured_cert_no_md5_salve",
      "insured_mobile_salve", "business_line_salve", "years_salve")
      .where("insured_cert_no_salve is not null and insured_mobile_salve is null")

    //增量数据
    val officialRes2 = official2.join(officialres2,
      'insured_cert_no === 'insured_cert_no_salve, "leftouter")
      .select("id", "insured_cert_no", "insured_cert_no_salve", "insured_cert_no_md5",
        "insured_mobile", "insured_mobile_salve", "insured_mobile_md5", "business_line", "years")
      .where("insured_cert_no_salve is null")

    //证件号为空,手机号为空
    val official3 = official.select("id", "insured_cert_no", "insured_cert_no_md5", "insured_mobile", "insured_mobile_md5", "business_line", "years")
      .where("insured_cert_no is  null and insured_mobile is not null")

    //身份证号为空,手机号不为空的增量
    val officialres3 = odsMd5MessageOfficial.select("insured_cert_no_salve", "insured_cert_no_md5_salve",
      "insured_mobile_salve", "business_line_salve", "years_salve")
      .where("insured_cert_no_salve is null and insured_mobile_salve is not null")

    //增量数据
    val officialRes3 = official3.join(officialres3,
      'insured_mobile === 'insured_mobile_salve, "leftouter")
      .select("id", "insured_cert_no", "insured_cert_no_salve", "insured_cert_no_md5",
        "insured_mobile", "insured_mobile_salve", "insured_mobile_md5", "business_line", "years")
      .where("insured_mobile_salve is null")

    //所有官网的数据
    val incrementOfficial = officialRes1.unionAll(officialRes2).unionAll(officialRes3)
      .select("insured_cert_no", "insured_cert_no_md5",
        "insured_mobile", "insured_mobile_md5", "business_line", "years")
    incrementOfficial.registerTempTable("OfficialTable")

    val res = hiveContext.sql("select insured_cert_no,insured_cert_no_md5,insured_mobile,insured_mobile_md5,business_line,max(years) as years from OfficialTable group by insured_cert_no,insured_cert_no_md5,insured_mobile,insured_mobile_md5,business_line")

    res

  }


  /**
   * 接口增量数据
   *
   * @param hiveContext
   * @return
   */
  def IncrementInter(hiveContext: HiveContext): DataFrame = {
    import hiveContext.implicits._
    hiveContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    hiveContext.udf.register("MD5", (str: String) => MD5(str))
    hiveContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      //设置日期格式
      val date = df.format(new Date()) // new Date()为获取当前系统时间
      (date + "")
    })
    //接口数据当月增量去匹配
    val interTemp =
      hiveContext.sql("select " +
        "MD5(concat(insured_name,insured_cert_no)) as id," +
        "insured_cert_no,MD5(insured_cert_no) as insured_cert_no_md5," +
        "insured_mobile, MD5(insured_mobile) as insured_mobile_md5," +
        "business_line,years from odsdb.ods_all_business_person_base_info_detail " +
        "where business_line = 'inter' and substring(cast(getNow() as string),1,7) = years")

    //读取信息表,判断接口增量数据
    val odsMd5MessageInter = hiveContext.sql("select insured_cert_no as insured_cert_no_salve," +
      "insured_cert_no_md5 as insured_cert_no_md5_salve,insured_mobile as insured_mobile_salve," +
      "insured_mobile_md5 as insured_mobile_md5_salve,business_line as business_line_salve," +
      "years as years_salve from odsdb.ods_md5_message_detail " +
      "where business_line='inter' and substring(cast(getNow() as string),1,7) = years")

    //过滤出接口无效信息
    val inter = interTemp.select("id", "insured_cert_no", "insured_cert_no_md5", "insured_mobile", "insured_mobile_md5", "business_line", "years")
      .where("insured_cert_no is not null or insured_mobile is not null")

    //拿出接口不同类型的数据进行关联 手机号和证件号都不为空的
    val inter1 = inter.select("id", "insured_cert_no", "insured_cert_no_md5", "insured_mobile", "insured_mobile_md5", "business_line", "years")
      .where("insured_cert_no is not null and insured_mobile is not null")

    //接口数据身份证号和手机号不为空的增量
    val officialres1 = odsMd5MessageInter.select("insured_cert_no_salve", "insured_cert_no_md5_salve",
      "insured_mobile_salve", "business_line_salve", "years_salve")
      .where("insured_cert_no_salve is not null and insured_mobile_salve is not null")

    //手机号和证件号不为空的增量
    val interRes1 = inter1.join(officialres1,
      'insured_cert_no === 'insured_cert_no_salve and 'insured_mobile === 'insured_mobile_salve, "leftouter")
      .select("id", "insured_cert_no", "insured_cert_no_salve", "insured_cert_no_md5",
        "insured_mobile", "insured_mobile_salve", "insured_mobile_md5", "business_line", "years")
      .where("insured_cert_no_salve is null and insured_mobile_salve is null")

    //证件号不为空,手机号为空
    val inter2 = inter.select("id", "insured_cert_no", "insured_cert_no_md5", "insured_mobile", "insured_mobile_md5", "business_line", "years")
      .where("insured_cert_no is not null and insured_mobile is null")

    //身份证号不为空,手机号为空的增量
    val interres2 = odsMd5MessageInter.select("insured_cert_no_salve", "insured_cert_no_md5_salve",
      "insured_mobile_salve", "business_line_salve", "years_salve")
      .where("insured_cert_no_salve is not null and insured_mobile_salve is null")

    //增量数据
    val interRes2 = inter2.join(interres2,
      'insured_cert_no === 'insured_cert_no_salve, "leftouter")
      .select("id", "insured_cert_no", "insured_cert_no_salve", "insured_cert_no_md5",
        "insured_mobile", "insured_mobile_salve", "insured_mobile_md5", "business_line", "years")
      .where("insured_cert_no_salve is null")

    //证件号为空,手机号为空
    val inter3 = inter.select("id", "insured_cert_no", "insured_cert_no_md5", "insured_mobile", "insured_mobile_md5", "business_line", "years")
      .where("insured_cert_no is  null and insured_mobile is not null")

    //身份证号为空,手机号不为空的增量
    val interres3 = odsMd5MessageInter.select("insured_cert_no_salve", "insured_cert_no_md5_salve",
      "insured_mobile_salve", "business_line_salve", "years_salve")
      .where("insured_cert_no_salve is null and insured_mobile_salve is not null")

    //增量数据
    val interRes3 = inter3.join(interres3,
      'insured_mobile === 'insured_mobile_salve, "leftouter")
      .select("id", "insured_cert_no", "insured_cert_no_salve", "insured_cert_no_md5",
        "insured_mobile", "insured_mobile_salve", "insured_mobile_md5", "business_line", "years")
      .where("insured_mobile_salve is null")

    //所有接口的数据
    val incrementInter = interRes1.unionAll(interRes2).unionAll(interRes3)
      .select("insured_cert_no", "insured_cert_no_md5",
        "insured_mobile", "insured_mobile_md5", "business_line", "years")
    incrementInter.registerTempTable("InterTable")

    val res = hiveContext.sql("select insured_cert_no,insured_cert_no_md5,insured_mobile,insured_mobile_md5,business_line,max(years) as years from InterTable group by insured_cert_no,insured_cert_no_md5,insured_mobile,insured_mobile_md5,business_line")

    res

  }

  /**
   * 婚礼纪增量数据
   *
   * @param hiveContext
   * @return
   */

  def IncrementWedding(hiveContext: HiveContext): DataFrame = {
    import hiveContext.implicits._
    hiveContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    hiveContext.udf.register("MD5", (str: String) => MD5(str))
    hiveContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      //设置日期格式
      val date = df.format(new Date()) // new Date()为获取当前系统时间
      (date + "")
    })

    //接口数据当月增量去匹配
    val weddingTemp =
      hiveContext.sql("select " +
        "MD5(concat(insured_name,insured_cert_no)) as id," +
        "insured_cert_no,MD5(insured_cert_no) as insured_cert_no_md5," +
        "insured_mobile, MD5(insured_mobile) as insured_mobile_md5," +
        "business_line,years from odsdb.ods_all_business_person_base_info_detail where business_line = 'wedding'")

    //读取信息表,判断官网增量数据
    val odsMd5MessageWedding = hiveContext.sql("select insured_cert_no as insured_cert_no_salve," +
      "insured_cert_no_md5 as insured_cert_no_md5_salve,insured_mobile as insured_mobile_salve," +
      "insured_mobile_md5 as insured_mobile_md5_salve,business_line as business_line_salve," +
      "years as years_salve from odsdb.ods_md5_message_detail where business_line='wedding'")

    //过滤出接口无效信息
    val wedding = weddingTemp.select("id", "insured_cert_no", "insured_cert_no_md5", "insured_mobile", "insured_mobile_md5", "business_line", "years")
      .where("insured_cert_no is not null or insured_mobile is not null")

    //拿出接口不同类型的数据进行关联 手机号和证件号都不为空的
    val wedding1 = wedding.select("id", "insured_cert_no", "insured_cert_no_md5", "insured_mobile", "insured_mobile_md5", "business_line", "years")
      .where("insured_cert_no is not null and insured_mobile is not null")

    //接口数据身份证号和手机号不为空的增量
    val weddingres1 = odsMd5MessageWedding.select("insured_cert_no_salve", "insured_cert_no_md5_salve",
      "insured_mobile_salve", "business_line_salve", "years_salve")
      .where("insured_cert_no_salve is not null and insured_mobile_salve is not null")

    //手机号和证件号不为空的增量
    val weddingRes1 = wedding1.join(weddingres1,
      'insured_cert_no === 'insured_cert_no_salve and 'insured_mobile === 'insured_mobile_salve, "leftouter")
      .select("id", "insured_cert_no", "insured_cert_no_salve", "insured_cert_no_md5",
        "insured_mobile", "insured_mobile_salve", "insured_mobile_md5", "business_line", "years")
      .where("insured_cert_no_salve is null and insured_mobile_salve is null")

    //证件号不为空,手机号为空
    val wedding2 = wedding.select("id", "insured_cert_no", "insured_cert_no_md5", "insured_mobile", "insured_mobile_md5", "business_line", "years")
      .where("insured_cert_no is not null and insured_mobile is null")

    //身份证号不为空,手机号为空的增量
    val weddingres2 = odsMd5MessageWedding.select("insured_cert_no_salve", "insured_cert_no_md5_salve",
      "insured_mobile_salve", "business_line_salve", "years_salve")
      .where("insured_cert_no_salve is not null and insured_mobile_salve is null")

    //增量数据
    val weddingRes2 = wedding2.join(weddingres2,
      'insured_cert_no === 'insured_cert_no_salve, "leftouter")
      .select("id", "insured_cert_no", "insured_cert_no_salve", "insured_cert_no_md5",
        "insured_mobile", "insured_mobile_salve", "insured_mobile_md5", "business_line", "years")
      .where("insured_cert_no_salve is null")

    //证件号为空,手机号为空
    val wedding3 = wedding.select("id", "insured_cert_no", "insured_cert_no_md5", "insured_mobile", "insured_mobile_md5", "business_line", "years")
      .where("insured_cert_no is  null and insured_mobile is not null")

    //身份证号为空,手机号不为空的增量
    val weddingres3 = odsMd5MessageWedding.select("insured_cert_no_salve", "insured_cert_no_md5_salve",
      "insured_mobile_salve", "business_line_salve", "years_salve")
      .where("insured_cert_no_salve is null and insured_mobile_salve is not null")

    //增量数据
    val weddingRes3 = wedding3.join(weddingres3,
      'insured_mobile === 'insured_mobile_salve, "leftouter")
      .select("id", "insured_cert_no", "insured_cert_no_salve", "insured_cert_no_md5",
        "insured_mobile", "insured_mobile_salve", "insured_mobile_md5", "business_line", "years")
      .where("insured_mobile_salve is null")

    //所有接口的数据
    val incrementWedding = weddingRes1.unionAll(weddingRes2).unionAll(weddingRes3)
      .select("insured_cert_no", "insured_cert_no_md5",
        "insured_mobile", "insured_mobile_md5", "business_line", "years")
    incrementWedding.registerTempTable("WeddingTable")
    val res = hiveContext.sql("select insured_cert_no,insured_cert_no_md5,insured_mobile,insured_mobile_md5,business_line,max(years) as years from WeddingTable group by insured_cert_no,insured_cert_no_md5,insured_mobile,insured_mobile_md5,business_line")

    res
  }

}

