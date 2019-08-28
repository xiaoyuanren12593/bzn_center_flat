package bzn.ods.policy

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import bzn.job.common.Until
import bzn.ods.util.SparkUtil
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * author:xiaoYuanRen
  * Date:2019/5/23
  * Time:15:08
  * describe: this is new class
  **/
object OdsPolicyInsuredSlaveDetail extends SparkUtil with Until{
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName,"")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    val oneDate = oneOdsPolicyInsuredSlaveDetail(hiveContext)
    val twoDate = twoOdsPolicyInsuredSlaveDetail(hiveContext)
    val res = oneDate.unionAll(twoDate)
    res.cache()

    hiveContext.sql("truncate table odsdb.ods_policy_insured_slave_detail")
    res.repartition(10).write.mode(SaveMode.Append).saveAsTable("odsdb.ods_policy_insured_slave_detail")
    res.repartition(1).write.mode(SaveMode.Overwrite).parquet("/dw_data/ods_data/OdsPolicyInsuredSlaveDetail")

    sc.stop()
  }

  /**
    * 读取1.0系统的附属被保人信息
    * @param sqlContext
    */
  def oneOdsPolicyInsuredSlaveDetail(sqlContext: HiveContext)={
    sqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    sqlContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")//设置日期格式
      val date = df.format(new Date())// new Date()为获取当前系统时间
      (date + "")
    })
    sqlContext.udf.register("getAgeFromBirthTime", (cert_no: String, end: String) => getAgeFromBirthTime(cert_no, end))
    sqlContext.udf.register("getEmptyString", () => "")
    sqlContext.udf.register("clean", (str: String) => clean(str))
    sqlContext.udf.register("timeToString", (time: java.sql.Timestamp) => {
      val str: String = if (time != null) time.toString.split("\\.")(0) else null
      str
    })

    val odrPolicyInsuredChildBznprd = readMysqlTable(sqlContext,"odr_policy_insured_child_bznprd")
      .selectExpr("getUUID() as id","clean(id) as insured_slave_id","clean(insured_id) as master_id","clean(child_name) as slave_name","" +
        "case when `child_gender` = 0 then 0 when  child_gender = 1 then 1 else null  end  as gender",
        "case when child_cert_type = '1' then 1 else -1 end as slave_cert_type ",
        "clean(child_cert_no) as slave_cert_no","clean(timeToString(child_birthday)) as birthday","cast(clean(getEmptyString()) as int) as is_married","clean(getEmptyString()) as email",
        "case when child_policy_status = 1 then 1 else 0 end as policy_status","start_date","end_date",
        "case when child_cert_type ='1' and start_date is not null then getAgeFromBirthTime(child_cert_no,start_date) else null end as age","create_time","update_time","getNow() as dw_create_time")

    odrPolicyInsuredChildBznprd
  }

  /**
    * 读取2.0系统的附属被保人信息
    * @param sqlContext
    */
  def twoOdsPolicyInsuredSlaveDetail(sqlContext: HiveContext)={
    sqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    sqlContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")//设置日期格式
      val date = df.format(new Date())// new Date()为获取当前系统时间
      (date + "")
    })
    sqlContext.udf.register("getAgeFromBirthTime", (cert_no: String, end: String) => getAgeFromBirthTime(cert_no, end))
    sqlContext.udf.register("clean", (str: String) => clean(str))
    sqlContext.udf.register("timeToString", (time: java.sql.Timestamp) => {
      val str: String = if (time != null) time.toString.split("\\.")(0) else null
      str
    })

    val bPolicySubjectPersonSlaveBzncen = readMysqlTable(sqlContext,"b_policy_subject_person_slave_bzncen")
      .selectExpr("getUUID() as id","id as insured_slave_id","master_id","name as slave_name","sex as gender","cert_type as slave_cert_type",
        "","birthday","is_married","email","status","start_date","end_date",
        "case when cert_type ='1' and start_date is not null then getAgeFromBirthTime(cert_no,start_date) else null end as age","create_time","update_time","getNow() as dw_create_time")
        .registerTempTable("bPolicySubjectPersonSlaveBzncenTemp")ve_id","clean(cast(master_id as String)) as master_id","clean(slave_name) as slave_name",
        "case when `gender` = 2 then 0 when  gender = 1 then 1 else null  end  as gender", "slave_cert_type","clean(slave_cert_no) as slave_cert_no",
        "clean(timeToString(birthday)) as birthday","is_married","clean(email) as emai\n\n    val res = sqlContext.sql(\"select *,case when a.`status`='1' then '0' else '1' end as policy_status from bPolicySubjectPersonSlaveBzncenTemp a\")\n      .selectExpr(\"id\",\"clean(cast(insured_slave_id as String)) as insured_slal","cast(clean(policy_status) as int) as policy_status",
        "start_date","end_date","age","create_time","update_time","getNow() as dw_create_time")

    res
  }
  /**
    * 获取 Mysql 表的数据
    * @param sqlContext
    * @param tableName 读取Mysql表的名字
    * @return 返回 Mysql 表的 DataFrame
    */
  def readMysqlTable(sqlContext: SQLContext, tableName: String): DataFrame = {
    val properties: Properties = getProPerties()
    sqlContext
      .read
      .format("jdbc")
      .option("url", properties.getProperty("mysql.url.106"))
      .option("driver", properties.getProperty("mysql.driver"))
      .option("user", properties.getProperty("mysql.username.106"))
      .option("password", properties.getProperty("mysql.password.106"))
      .option("numPartitions","10")
      .option("partitionColumn","id")
      .option("lowerBound", "0")
      .option("upperBound","200")
      .option("dbtable", tableName)
      .load()
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
}
