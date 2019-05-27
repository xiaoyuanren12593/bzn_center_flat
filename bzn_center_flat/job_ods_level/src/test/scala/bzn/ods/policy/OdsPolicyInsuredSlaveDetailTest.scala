package bzn.ods.policy

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import bzn.ods.util.{SparkUtil, Until}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.hive.HiveContext

import scala.io.Source

/**
  * author:xiaoYuanRen
  * Date:2019/5/23
  * Time:15:08
  * describe: this is new class
  **/
object OdsPolicyInsuredSlaveDetailTest extends SparkUtil with Until{
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName,"local[*]")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
//    oneOdsPolicyInsuredSlaveDetail(hiveContext)
    twoOdsPolicyInsuredSlaveDetail(hiveContext)
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

    val odrPolicyInsuredChildBznprd = readMysqlTable(sqlContext,"odr_policy_insured_child_bznprd")
      .selectExpr("getUUID() as id","id as insured_slave_id","insured_id as master_id","child_name as slave_name","child_gender as gender","child_cert_type as slave_cert_type","child_cert_no as slave_cert_no","child_birthday as birthday","child_policy_status as policy_status","start_date","end_date",
        "case when child_cert_type ='1' and start_date is not null then getAgeFromBirthTime(child_cert_no,start_date) else null end as age","create_time","update_time","getNow() as dw_create_time")

    odrPolicyInsuredChildBznprd.show()
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

    val bPolicySubjectPersonSlaveBzncen = readMysqlTable(sqlContext,"b_policy_subject_person_slave_bzncen")
      .selectExpr("getUUID() as id","id as insured_slave_id","master_id","name as slave_name","sex as gender","cert_type as slave_cert_type","cert_no as slave_cert_no","birthday","status","start_date","end_date",
        "case when cert_type ='1' and start_date is not null then getAgeFromBirthTime(cert_no,start_date) else null end as age","create_time","update_time","getNow() as dw_create_time")
        .registerTempTable("bPolicySubjectPersonSlaveBzncenTemp")

    val res = sqlContext.sql("select *,case when a.`status`='1' then '0' else '1' end as policy_status from bPolicySubjectPersonSlaveBzncenTemp a")
      .selectExpr("id","insured_slave_id","master_id","slave_name","gender","slave_cert_type","slave_cert_no","birthday","policy_status","start_date","end_date",
        "age","create_time","update_time","getNow() as dw_create_time")
    res.show()
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
