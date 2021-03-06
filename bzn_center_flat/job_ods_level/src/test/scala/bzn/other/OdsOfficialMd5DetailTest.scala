package bzn.other

import java.text.SimpleDateFormat
import java.util.Date

import bzn.job.common.{MysqlUntil, Until}

import bzn.util.SparkUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.hive.HiveContext


object OdsOfficialMd5DetailTest extends SparkUtil with Until with MysqlUntil {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName: String = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "local[*]")

    val sc: SparkContext = sparkConf._2
    val hiveContext = sparkConf._4
    val res1 = OfficialMd5(hiveContext)
    val res2 = WeddingMd5(hiveContext)
    res2.printSchema()
    sc.stop()


  }

  /**
   * 官网增量数据
   *
   * @param hiveContext
   */

  def OfficialMd5(hiveContext: HiveContext): DataFrame = {
    import hiveContext.implicits._

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
    val odrPolicyInsuredBznprd = hiveContext.sql("select cert_no,mobile," +
      "trim(substring(cast(if(start_date is null,if(end_date is null ,if(create_time is null,if(update_time is null,now(),update_time),create_time),end_date),start_date) as STRING),1,7)) as years from sourcedb_test.odr_policy_insured_bznprd")

    //过滤出无效数据
    val odrPolicyInsuredBznprdSalve = odrPolicyInsuredBznprd
      .select("cert_no", "mobile", "years")
      .where("cert_no is not null or mobile is not null")

    //身份证号和手机号都不为空的数据
    val odrPolicyInsuredBznprdTemp1 = odrPolicyInsuredBznprdSalve
      .select("cert_no", "mobile", "years")
      .where("cert_no is not null and mobile is not null")

    //增量数据
    val odrPolicyInsuredBznprdRes1 = odrPolicyInsuredBznprdTemp1.join(odsMd5MessageDetail, 'cert_no === 'insured_cert_no_salve and 'mobile === 'insured_mobile_salve, "leftouter")
      .select("cert_no", "mobile", "years", "insured_cert_no_salve", "insured_mobile_salve")
      .where("insured_cert_no_salve is null and insured_mobile_salve is null")

    //身份证号不为空,手机号为空的数据
    val odrPolicyInsuredBznprdTemp2 = odrPolicyInsuredBznprdSalve
      .select("cert_no", "mobile", "years")
      .where("cert_no is not null and mobile is null")

    //增量数据
    val odrPolicyInsuredBznprdRes2 = odrPolicyInsuredBznprdTemp2.join(odsMd5MessageDetail, 'cert_no === 'insured_cert_no_salve, "leftouter")
      .select("cert_no", "mobile", "years", "insured_cert_no_salve", "insured_mobile_salve")
      .where("insured_cert_no_salve is null")

    //身份证号为空,手机号不为空的数据
    val odrPolicyInsuredBznprdTemp3 = odrPolicyInsuredBznprdSalve
      .select("cert_no", "mobile", "years")
      .where("cert_no is null and mobile is not null")

    //增量数据
    val odrPolicyInsuredBznprdRes3 = odrPolicyInsuredBznprdTemp3.join(odsMd5MessageDetail, 'mobile === 'insured_mobile_salve, "leftouter")
      .select("cert_no", "mobile", "years", "insured_cert_no_salve", "insured_mobile_salve")
      .where("insured_mobile_salve is null")

    //1.0所有增量数据
    val res1 = odrPolicyInsuredBznprdRes1
      .unionAll(odrPolicyInsuredBznprdRes2)
      .unionAll(odrPolicyInsuredBznprdRes3)

    //2.0被保人表
    val bPolicySubjectPersonMasterBzncen = hiveContext.sql("select cert_no,tel," +
      "trim(substring(cast(if(start_date is null,if(end_date is null ,if(create_time is null,if(update_time is null,now(),update_time),create_time),end_date),start_date) as STRING),1,7)) as years from sourcedb_test.b_policy_subject_person_master_bzncen")

    //过滤出无效数据
    val bPolicySubjectPersonMasterBzncenSalve = bPolicySubjectPersonMasterBzncen
      .select("cert_no", "tel", "years")
      .where("cert_no is not null or tel is not null")

    //身份证号和手机号都不为空的数据
    val bPolicySubjectPersonMasterBzncenTemp1 = bPolicySubjectPersonMasterBzncenSalve
      .select("cert_no", "tel", "years")
      .where("cert_no is not null and tel is not null")

    //增量数据
    val bPolicySubjectPersonMasterBzncenRes1 = bPolicySubjectPersonMasterBzncenTemp1.join(odsMd5MessageDetail, 'cert_no === 'insured_cert_no_salve and 'tel === 'insured_mobile_salve, "leftouter")
      .selectExpr("cert_no", "tel as mobile", "years", "insured_cert_no_salve", "insured_mobile_salve")
      .where("insured_cert_no_salve is null and insured_mobile_salve is null")

    //身份证号不为空,手机号为空的数据
    val bPolicySubjectPersonMasterBzncenTemp2 = bPolicySubjectPersonMasterBzncenSalve
      .select("cert_no", "tel", "years")
      .where("cert_no is not null and tel is null")

    //增量数据
    val bPolicySubjectPersonMasterBzncenRes2 = bPolicySubjectPersonMasterBzncenTemp2.join(odsMd5MessageDetail, 'cert_no === 'insured_cert_no_salve, "leftouter")
      .selectExpr("cert_no", "tel as mobile", "years", "insured_cert_no_salve", "insured_mobile_salve")
      .where("insured_cert_no_salve is null")

    //身份证号为空,手机号不为空的数据
    val bPolicySubjectPersonMasterBzncenTemp3 = bPolicySubjectPersonMasterBzncenSalve
      .select("cert_no", "tel", "years")
      .where("cert_no is null and tel is not null")

    //增量数据
    val bPolicySubjectPersonMasterBzncenRes3 = bPolicySubjectPersonMasterBzncenTemp3.join(odsMd5MessageDetail, 'tel === 'insured_mobile_salve, "leftouter")
      .selectExpr("cert_no", "tel as mobile", "years", "insured_cert_no_salve", "insured_mobile_salve")
      .where("insured_mobile_salve is null")

    val res2 =
      bPolicySubjectPersonMasterBzncenRes1
        .unionAll(bPolicySubjectPersonMasterBzncenRes2)
        .unionAll(bPolicySubjectPersonMasterBzncenRes3)
    val res3 = res1.unionAll(res2)
      .where("length(mobile) =11")
      .selectExpr("cert_no",
        "MD5(cert_no) as cert_no_md5",
        "mobile",
        "MD5(mobile) as mobile_md5",
        "years",
        "'official' as business_line")
    res3.registerTempTable("OfficialTable")
    val res4 = hiveContext.sql("select cert_no,cert_no_md5,mobile,mobile_md5,getNow() as dw_create_time,business_line,max(years) as years from OfficialTable group by cert_no,cert_no_md5,mobile,mobile_md5,business_line")


    res4

  }

  /**
   * 婚礼纪加密增量数据
   *
   * @param hiveContext
   * @return
   */
  def WeddingMd5(hiveContext: HiveContext): DataFrame = {
    import hiveContext.implicits._

    hiveContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    hiveContext.udf.register("MD5", (str: String) => MD5(str))
    hiveContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      //设置日期格式
      val date = df.format(new Date()) // new Date()为获取当前系统时间
      (date + "")
    })

    //保单表
    val openPolicy = readMysqlTable(hiveContext, "open_policy_bznapi", "mysql.username.106",
      "mysql.password.106", "mysql.driver", "mysql.url.106")
      .selectExpr("proposal_no", "create_time", "update_time")

    //被保人表保人表
    val openInsuredMd5 = readMysqlTable(hiveContext, "open_insured_bznapi_md5", "mysql.username.106",
      "mysql.password.106", "mysql.driver", "mysql.url.106")
      .selectExpr("proposal_no as proposal_no_salve", "cert_no", "tel")

    //保单表关联被保人表
    val weddingInsuredPolicy = openPolicy.join(openInsuredMd5, 'proposal_no === 'proposal_no_salve, "leftouter")
      .selectExpr(
        "create_time",
        "update_time",
        "cert_no as insured_cert_no",
        "tel as insured_mobile",
        "'wedding' as business_line",
        "substring(cast(case when create_time is null then now() else create_time end as string),1,7) as years")

    // 读取hive的表中的数据
    val OdsMd5Wedding = hiveContext.sql("select insured_cert_no as insured_cert_no_salve,insured_mobile as insured_mobile_salve from odsdb.ods_md5_message_detail where business_line = 'wedding'")

    //过滤出无效数据
    val weddingInsuredPolicySalve = weddingInsuredPolicy.selectExpr("insured_cert_no", "insured_mobile", "business_line", "years")
      .where("insured_cert_no is not null or insured_mobile is not null")

    //身份证号不为空,手机号不为空的数据
    val weddingInsuredPolicyTemp1 = weddingInsuredPolicySalve
      .selectExpr("insured_cert_no", "insured_mobile", "business_line", "years")
      .where("insured_cert_no is not null and insured_mobile is not null")

    //增量数据
    val weddingIncrement1 = weddingInsuredPolicyTemp1.join(OdsMd5Wedding, 'insured_cert_no === 'insured_cert_no_salve and 'insured_mobile === 'insured_mobile_salve, "leftouter")
      .selectExpr("insured_cert_no","insured_cert_no_salve", "insured_mobile","insured_mobile_salve","business_line", "years")
      .where("insured_cert_no_salve is null and insured_mobile_salve is null")

    //身份证号不为空,手机号为空的数据
    val weddingInsuredPolicyTemp2 = weddingInsuredPolicySalve.selectExpr("insured_cert_no", "insured_mobile", "business_line", "years")
      .where("insured_cert_no is not null and insured_mobile is null")

    //增量数据
    val weddingIncrement2 = weddingInsuredPolicyTemp2.join(OdsMd5Wedding, 'insured_cert_no === 'insured_cert_no_salve, "leftouter")
      .selectExpr("insured_cert_no","insured_cert_no_salve", "insured_mobile","insured_mobile_salve","business_line", "years")
      .where("insured_cert_no_salve is null")

    //身份证号为空,手机号不为空的数据
    val weddingInsuredPolicyTemp3 = weddingInsuredPolicySalve.selectExpr("insured_cert_no", "insured_mobile", "business_line", "years")
      .where("insured_cert_no is  null and insured_mobile is not null")

    //增量数据
    val weddingIncrement3 = weddingInsuredPolicyTemp3.join(OdsMd5Wedding, 'insured_mobile === 'insured_mobile_salve, "leftouter")
      .selectExpr("insured_cert_no","insured_cert_no_salve", "insured_mobile","insured_mobile_salve","business_line", "years")
      .where("insured_mobile_salve is null")

    //婚礼纪所有的增量数据
    val resTemp = weddingIncrement1.unionAll(weddingIncrement2).unionAll(weddingIncrement3)
      .where("length(insured_mobile) =11")
      .selectExpr("insured_cert_no",
        "MD5(insured_cert_no) as insured_cert_no_md5",
        "insured_mobile",
        "MD5(insured_mobile) as insured_mobile_md5",
        "business_line",
        "years")

    resTemp.registerTempTable("WeddingTable")

    val resSalve = hiveContext.sql("select insured_cert_no,insured_cert_no_md5,insured_mobile,insured_mobile_md5,business_line,max(years) as years from WeddingTable group by insured_cert_no,insured_cert_no_md5,insured_mobile,insured_mobile_md5,business_line")
    val res = resSalve.selectExpr("insured_cert_no",
      "insured_cert_no_md5",
      "insured_mobile",
      "insured_mobile_md5",
      "getNow() as dw_create_time",
      "business_line",
      "years")

    res
  }

}
