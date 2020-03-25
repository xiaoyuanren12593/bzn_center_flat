package bzn.safedata

import java.text.SimpleDateFormat
import java.util.Date

import bzn.job.common.{MysqlUntil, Until}
import bzn.safedata.OdsOfficialMd5.{MD5, clean}
import bzn.util.SparkUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext

object OdsOfficialMd5Test extends SparkUtil with Until with MysqlUntil {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName: String = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "local[*]")

    val sc: SparkContext = sparkConf._2
    val hiveContext = sparkConf._4

    hiveContext.setConf("hive.exec.dynamic.partition", "true")
    hiveContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

    val res = OfficialMd5FullDose(hiveContext)
    res.printSchema()
    /* res.write.mode(SaveMode.Append).format("PARQUET").partitionBy("business_line", "years")
       .saveAsTable("odsdb.ods_md5_message_detail")*/

    sc.stop()

  }


  /**
   * 官网全量写法
   *
   * @param hiveContext
   * @return
   */
  def OfficialMd5FullDose(hiveContext: HiveContext): DataFrame = {

    hiveContext.udf.register("clean", (str: String) => clean(str))
    hiveContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    hiveContext.udf.register("MD5", (str: String) => MD5(str))
    hiveContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      //设置日期格式
      val date = df.format(new Date()) // new Date()为获取当前系统时间
      (date + "")
    })

    //1.0被保人表
    val res1 = hiveContext.sql("select clean(regexp_replace(cert_no,'\\n','')) as cert_no,clean(mobile) as mobile," +
      "trim(substring(cast(if(start_date is null,if(end_date is null ,if(create_time is null,if(update_time is null,now(),update_time),create_time),end_date),start_date) as STRING),1,7)) as years " +
      "from sourcedb.odr_policy_insured_bznprd")

    //2.0被保人表
    val res2 = hiveContext.sql("select clean(regexp_replace(cert_no,'\\n','')) as cert_no,clean(tel) as mobile," +
      "trim(substring(cast(if(start_date is null,if(end_date is null ,if(create_time is null,if(update_time is null,now(),update_time),create_time),end_date),start_date) as STRING),1,7)) as years " +
      "from sourcedb.b_policy_subject_person_master_bzncen")

    //合并1.0和2.0的数据
    val res3 = res1.unionAll(res2)
      .where("cert_no is not null or mobile is not null")
      .selectExpr("cert_no",
        "MD5(cert_no) as cert_no_md5",
        "mobile",
        "MD5(mobile) as mobile_md5",
        "'official' as business_line",
        "years")
    res3.registerTempTable("OfficialTable")
    val res4 = hiveContext.sql("select getUUID() as id,cert_no,cert_no_md5,mobile,mobile_md5,getNow() as dw_create_time,business_line,max(years) as years " +
      "from OfficialTable group by cert_no,cert_no_md5,mobile,mobile_md5,business_line")

    res4


  }

  /**
   * 官网增量数据
   *
   * @param hiveContext
   */

  def OfficialMd5(hiveContext: HiveContext): DataFrame = {
    import hiveContext.implicits._
    hiveContext.udf.register("clean", (str: String) => clean(str))
    hiveContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    hiveContext.udf.register("MD5", (str: String) => MD5(str))
    hiveContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      //设置日期格式
      val date = df.format(new Date()) // new Date()为获取当前系统时间
      (date + "")
    })
    //读取加密库的数据
    val odsMd5MessageDetail = hiveContext.sql("select insured_cert_no as insured_cert_no_salve,insured_mobile as insured_mobile_salve,business_line  as business_line_salve from odsdb.ods_md5_message_detail where business_line ='official'")

    //1.0被保人表
    val odrPolicyInsuredBznprdSalve = hiveContext.sql("select clean(cert_no) as cert_no,regexp_replace(clean(mobile),' ','') as mobile," +
      "trim(substring(cast(if(start_date is null,if(end_date is null ,if(create_time is null,if(update_time is null,now(),update_time),create_time),end_date),start_date) as STRING),1,7)) as years " +
      "from sourcedb.odr_policy_insured_bznprd where clean(cert_no) is not null or regexp_replace(clean(mobile),' ','') is not null")

    //身份证号和手机号都不为空的数据
    val odrPolicyInsuredBznprdTemp1 = odrPolicyInsuredBznprdSalve
      .selectExpr("cert_no", "mobile", "years")
      .where("cert_no is not null and mobile is not null")

    //增量数据
    val odrPolicyInsuredBznprdRes1 = odrPolicyInsuredBznprdTemp1.join(odsMd5MessageDetail, 'cert_no === 'insured_cert_no_salve and 'mobile === 'insured_mobile_salve, "leftouter")
      .selectExpr("cert_no", "mobile", "years", "insured_cert_no_salve", "insured_mobile_salve")
      .where("insured_cert_no_salve is null and insured_mobile_salve is null")

    //身份证号不为空,手机号为空的数据
    val odrPolicyInsuredBznprdTemp2 = odrPolicyInsuredBznprdSalve
      .selectExpr("cert_no", "mobile", "years")
      .where("cert_no is not null and mobile is null")

    //增量数据
    val odrPolicyInsuredBznprdRes2 = odrPolicyInsuredBznprdTemp2.join(odsMd5MessageDetail, 'cert_no === 'insured_cert_no_salve, "leftouter")
      .selectExpr("cert_no", "mobile", "years", "insured_cert_no_salve", "insured_mobile_salve")
      .where("insured_cert_no_salve is null")

    //身份证号为空,手机号不为空的数据
    val odrPolicyInsuredBznprdTemp3 = odrPolicyInsuredBznprdSalve
      .selectExpr("cert_no", "mobile", "years")
      .where("cert_no is null and mobile is not null")

    //增量数据
    val odrPolicyInsuredBznprdRes3 = odrPolicyInsuredBznprdTemp3.join(odsMd5MessageDetail, 'mobile === 'insured_mobile_salve, "leftouter")
      .selectExpr("cert_no", "mobile", "years", "insured_cert_no_salve", "insured_mobile_salve")
      .where("insured_mobile_salve is null")
    //1.0所有增量数据
    val res1 = odrPolicyInsuredBznprdRes1
      .unionAll(odrPolicyInsuredBznprdRes2)
      .unionAll(odrPolicyInsuredBznprdRes3)

    //2.0被保人表
    val bPolicySubjectPersonMasterBzncenSalve = hiveContext.sql("select clean(cert_no) as cert_no,regexp_replace(clean(tel),' ','') as tel," +
      "trim(substring(cast(if(start_date is null,if(end_date is null ,if(create_time is null,if(update_time is null,now(),update_time),create_time),end_date),start_date) as STRING),1,7)) as years " +
      "from sourcedb.b_policy_subject_person_master_bzncen where clean(cert_no) is not null or regexp_replace(clean(tel),' ','') is not null")

    //身份证号和手机号都不为空的数据
    val bPolicySubjectPersonMasterBzncenTemp1 = bPolicySubjectPersonMasterBzncenSalve
      .selectExpr("cert_no", "tel", "years")
      .where("cert_no is not null and tel is not null")

    //增量数据
    val bPolicySubjectPersonMasterBzncenRes1 = bPolicySubjectPersonMasterBzncenTemp1.join(odsMd5MessageDetail, 'cert_no === 'insured_cert_no_salve and 'tel === 'insured_mobile_salve, "leftouter")
      .selectExpr("cert_no", "tel as mobile", "years", "insured_cert_no_salve", "insured_mobile_salve")
      .where("insured_cert_no_salve is null and insured_mobile_salve is null")

    //身份证号不为空,手机号为空的数据
    val bPolicySubjectPersonMasterBzncenTemp2 = bPolicySubjectPersonMasterBzncenSalve
      .selectExpr("cert_no", "tel", "years")
      .where("cert_no is not null and tel is null")

    //增量数据
    val bPolicySubjectPersonMasterBzncenRes2 = bPolicySubjectPersonMasterBzncenTemp2.join(odsMd5MessageDetail, 'cert_no === 'insured_cert_no_salve, "leftouter")
      .selectExpr("cert_no", "tel as mobile", "years", "insured_cert_no_salve", "insured_mobile_salve")
      .where("insured_cert_no_salve is null")

    //身份证号为空,手机号不为空的数据
    val bPolicySubjectPersonMasterBzncenTemp3 = bPolicySubjectPersonMasterBzncenSalve
      .selectExpr("cert_no", "tel", "years")
      .where("cert_no is null and tel is not null")

    //增量数据
    val bPolicySubjectPersonMasterBzncenRes3 = bPolicySubjectPersonMasterBzncenTemp3.join(odsMd5MessageDetail, 'tel === 'insured_mobile_salve, "leftouter")
      .selectExpr("cert_no", "tel as mobile", "years", "insured_cert_no_salve", "insured_mobile_salve")
      .where("insured_mobile_salve is null")

    val res2 =
      bPolicySubjectPersonMasterBzncenRes1
        .unionAll(bPolicySubjectPersonMasterBzncenRes2)
        .unionAll(bPolicySubjectPersonMasterBzncenRes3)

    //合并1.0和2.0的数据
    val res3 = res1.unionAll(res2)
      .selectExpr("cert_no",
        "MD5(cert_no) as cert_no_md5",
        "mobile",
        "MD5(mobile) as mobile_md5",
        "'official' as business_line",
        "years")
    res3.registerTempTable("OfficialTable")
    val res4 = hiveContext.sql("select getUUID() as id,cert_no,cert_no_md5,mobile,mobile_md5,getNow() as dw_create_time,business_line,max(years) as years from OfficialTable group by cert_no,cert_no_md5,mobile,mobile_md5,business_line")

    res4
  }


  /**
   * 官网增量写法
   * @param hiveContext
   * @return
   */

  def OfficialMd5Increment(hiveContext: HiveContext): DataFrame = {
    import hiveContext.implicits._
    hiveContext.udf.register("clean", (str: String) => clean(str))
    hiveContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    hiveContext.udf.register("MD5", (str: String) => MD5(str))
    hiveContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      //设置日期格式
      val date = df.format(new Date()) // new Date()为获取当前系统时间
      (date + "")
    })

    //1.0被保人表
    val res1 = hiveContext.sql("select clean(cert_no) as cert_no,clean(regexp_replace(mobile,' ','')) as mobile," +
      "trim(substring(cast(if(start_date is null,if(end_date is null ,if(create_time is null,if(update_time is null,now(),update_time),create_time),end_date),start_date) as STRING),1,7)) as years " +
      "from sourcedb.odr_policy_insured_bznprd where clean(cert_no) is not null or clean(regexp_replace(mobile,' ','')) is not null")

    //2.0被保人表
    val res2 = hiveContext.sql("select clean(cert_no) as cert_no,clean(regexp_replace(tel,' ','')) as mobile," +
      "trim(substring(cast(if(start_date is null,if(end_date is null ,if(create_time is null,if(update_time is null,now(),update_time),create_time),end_date),start_date) as STRING),1,7)) as years " +
      "from sourcedb.b_policy_subject_person_master_bzncen where clean(cert_no) is not null or clean(regexp_replace(tel,' ','')) is not null")
      .repartition(200)

    //合并1.0和2.0的数据
    val res3 = res1.unionAll(res2)
      .selectExpr("case when cert_no is null then '' else cert_no end as cert_no",
        "case when mobile is null then '' else mobile end as mobile",
        "'official' as business_line",
        "years")
      .repartition(200)

    //读取安全库的数据
    val odsMd5MessageDetail = hiveContext.sql(
      "select case when insured_cert_no is null then '' else insured_cert_no end as insured_cert_no_salve," +
        "case when insured_mobile is null then '' else insured_mobile end as insured_mobile_salve," +
        "business_line as business_line_salve from odsdb.ods_md5_message_detail where business_line ='official'")

    //判断增量数据
    val resTemp = res3.join(odsMd5MessageDetail, 'cert_no === 'insured_cert_no_salve and 'mobile === 'insured_mobile_salve, "leftouter")
      .where("insured_cert_no_salve is null and insured_mobile_salve is null")
      .selectExpr(
        "clean(cert_no) as cert_no",
        "MD5(clean(cert_no)) as cert_no_md5",
        "clean(mobile) as mobile",
        "MD5(clean(mobile)) as mobile_md5",
        "business_line",
        "years")

    resTemp.registerTempTable("resTempTable")

    val res = hiveContext.sql("select getUUID() as id,cert_no,cert_no_md5,mobile,mobile_md5,getNow() as dw_create_time,business_line,max(years) as years " +
      "from resTempTable group by cert_no,cert_no_md5,mobile,mobile_md5,business_line")

    res

  }


}