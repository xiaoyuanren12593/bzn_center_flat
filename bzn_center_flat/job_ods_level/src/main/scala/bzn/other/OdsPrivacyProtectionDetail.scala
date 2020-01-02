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
    val res = PrivacyProtecion(hiveContext)

    hiveContext.setConf("hive.exec.dynamic.partition", "true")
    hiveContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

    res.write.mode(SaveMode.Append).format("PARQUET").partitionBy("business_line", "years")
      .saveAsTable("odsdb.ods_md5_message_detail")
    sc.stop()
  }

  /**
   * 数据加密 官网,接口,婚礼纪
   *
   * @param hiveContext
   * @return
   */
  def PrivacyProtecion(hiveContext: HiveContext): DataFrame = {
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

    //婚礼纪数据所有增量去匹配
    val weddingTemp =
      hiveContext.sql("select " +
        "MD5(concat(insured_name,insured_cert_no)) as id," +
        "insured_cert_no,MD5(insured_cert_no) as insured_cert_no_md5," +
        "insured_mobile, MD5(insured_mobile) as insured_mobile_md5,business_line,years from odsdb.ods_all_business_person_base_info_detail where business_line = 'wedding'")

    //接口数据当月增量去匹配
    val interTemp =
      hiveContext.sql("select " +
        "MD5(concat(insured_name,insured_cert_no)) as id," +
        "insured_cert_no,MD5(insured_cert_no) as insured_cert_no_md5," +
        "insured_mobile, MD5(insured_mobile) as insured_mobile_md5," +
        "business_line,years from odsdb.ods_all_business_person_base_info_detail where business_line = 'inter' and substring(cast(getNow() as string),1,7) = years")

    //过滤出官网无效信息
    val official = officialTemp.select("id", "insured_cert_no", "insured_cert_no_md5", "insured_mobile", "insured_mobile_md5", "business_line", "years")
      .where("insured_cert_no is not null or insured_mobile is not null")

    //过滤出婚礼纪无效信息
    val wedding = weddingTemp.select("id", "insured_cert_no", "insured_cert_no_md5", "insured_mobile", "insured_mobile_md5", "business_line", "years")
      .where("insured_cert_no is not null or insured_mobile is not null")

    //过滤出接口无效信息
    val inter = interTemp.select("id", "insured_cert_no", "insured_cert_no_md5", "insured_mobile", "insured_mobile_md5", "business_line", "years")
      .where("insured_cert_no is not null or insured_mobile is not null")

    //读取信息表,判断官网增量数据
    val odsMd5MessageOfficial = hiveContext.sql("select id as id_salve,insured_cert_no as insured_cert_no_salve," +
      "insured_cert_no_md5 as insured_cert_no_md5_salve,insured_mobile as insured_mobile_salve," +
      "insured_mobile_md5 as insured_mobile_md5_salve,business_line as business_line_salve," +
      "years as years_salve from odsdb.ods_md5_message_detail where business_line='official'")

    //增量数据
    val officialRes = official.join(odsMd5MessageOfficial, 'insured_cert_no === 'insured_cert_no_salve and 'insured_mobile === 'insured_mobile_salve, "leftouter")
      .select("id", "id_salve", "insured_cert_no", "insured_cert_no_salve", "insured_cert_no_md5", "insured_mobile", "insured_mobile_salve", "insured_mobile_md5", "business_line", "years")
      .where("insured_cert_no_salve is null and insured_mobile_salve is null")

    //读取信息表,判断接口增量数据
    val odsMd5MessageWedding = hiveContext.sql("select id as id_salve,insured_cert_no as insured_cert_no_salve," +
      "insured_cert_no_md5 as insured_cert_no_md5_salve,insured_mobile as insured_mobile_salve," +
      "insured_mobile_md5 as insured_mobile_md5_salve,business_line as business_line_salve," +
      "years as years_salve from odsdb.ods_md5_message_detail where business_line='wedding'")

    //增量数据
    val weddingRes = wedding.join(odsMd5MessageWedding, 'insured_cert_no === 'insured_cert_no_salve and 'insured_mobile === 'insured_mobile_salve, "leftouter")
      .select("id", "id_salve", "insured_cert_no", "insured_cert_no_salve", "insured_cert_no_md5", "insured_mobile", "insured_mobile_salve", "insured_mobile_md5", "business_line", "years")
      .where("insured_cert_no_salve is null and insured_mobile_salve is null")

    //读取信息表,判断当月接口增量数据
    val odsMd5MessageInter = hiveContext.sql("select id as id_salve,insured_cert_no as insured_cert_no_salve," +
      "insured_cert_no_md5 as insured_cert_no_md5_salve,insured_mobile as insured_mobile_salve," +
      "insured_mobile_md5 as insured_mobile_md5_salve,business_line as business_line_salve," +
      "years as years_salve from odsdb.ods_md5_message_detail where business_line='inter' and substring(cast(getNow() as string),1,7) = years")

    //增量数据
    val interRes = inter.join(odsMd5MessageInter, 'insured_cert_no === 'insured_cert_no_salve and 'insured_mobile === 'insured_mobile_salve, "leftouter")
      .select("id", "id_salve", "insured_cert_no", "insured_cert_no_salve", "insured_cert_no_md5", "insured_mobile", "insured_mobile_salve", "insured_mobile_md5", "business_line", "years")
      .where("insured_cert_no_salve is null and insured_mobile_salve is null")

    val resTable = officialRes.unionAll(weddingRes).unionAll(interRes)
      .select("id", "insured_cert_no", "insured_cert_no_md5", "insured_mobile", "insured_mobile_md5", "business_line", "years")

    //注册临时表,拿到同一个人最小的时间
    resTable.registerTempTable("resTableSalve")
    val res = hiveContext.sql("select insured_cert_no,insured_cert_no_md5,insured_mobile,insured_mobile_md5,business_line,max(years) as years from resTableSalve group by insured_cert_no,insured_cert_no_md5,insured_mobile,insured_mobile_md5,business_line")

    res.registerTempTable("resFinalTable")
    val resfinal = hiveContext.sql("select getUUID() as id,insured_cert_no,insured_cert_no_md5,insured_mobile,insured_mobile_md5,business_line,years from resFinalTable")
    resfinal

  }

}

