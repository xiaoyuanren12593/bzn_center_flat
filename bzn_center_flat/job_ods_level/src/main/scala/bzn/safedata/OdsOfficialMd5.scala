package bzn.safedata

import java.text.SimpleDateFormat
import java.util.Date

import bzn.job.common.{MysqlUntil, Until}
import bzn.util.SparkUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext

object OdsOfficialMd5 extends SparkUtil with Until with MysqlUntil {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName: String = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "")

    val sc: SparkContext = sparkConf._2
    val hiveContext = sparkConf._4

    hiveContext.setConf("hive.exec.dynamic.partition", "true")
    hiveContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

    //全量写入
    val res = OfficialMd5FullDose(hiveContext)
    res.registerTempTable("PersonBaseInfoSafeData")
    hiveContext.sql("INSERT OVERWRITE table odsdb.ods_md5_message_detail PARTITION(business_line = 'official',years) select * from PersonBaseInfoSafeData")

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
}