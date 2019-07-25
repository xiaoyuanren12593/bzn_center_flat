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
  * Date:2019/5/28
  * Time:15:16
  * describe: this is new class
  **/
object OdsPreservationSlaveDetail extends SparkUtil with Until{
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName,"")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    val oneRes = onePreservationSlaveDetail(hiveContext)
    val twoRes = twoPreservationSlaveDetail(hiveContext)
    val res = oneRes.unionAll(twoRes)
    res.write.mode(SaveMode.Overwrite).saveAsTable("odsdb.ods_preservation_slave_detail")
    sc.stop()
  }

  /**
    * 读取2.0附属背包人信息表
    * @param sqlContext
    */
  def twoPreservationSlaveDetail(sqlContext:HiveContext) ={
    udfUtil(sqlContext)
    /**
      * 读取从属被保人表
      */
    val bPolicyPreservationSubjectPersonSlaveBzncen = readMysqlTable(sqlContext,"b_policy_preservation_subject_person_slave_bzncen")
      .selectExpr("inc_dec_order_no","inc_dec_order_no as inc_dec_order_no_temp","policy_no as policy_no_temp","name","case when sex = 2 then 0 else 1 end as gender","cert_type as child_cert_type","regexp_replace(cert_no,'\\n','') as child_cert_no","create_time","update_time")

    /**
      * 读取被保人表
      */
    val bPolicyPreservationSubjectPersonMasterBzncen =
      sqlContext.sql("select * from sourcedb.b_policy_preservation_subject_person_master_bzncen")
      .selectExpr("id as master_id","inc_dec_order_no as inc_dec_order_no_temp","policy_no as policy_no_temp","perservation_type as preserve_type","start_date","end_date","case when status = 1 then 0 else 1 end as insured_status")

    val resTemp = bPolicyPreservationSubjectPersonSlaveBzncen.join(bPolicyPreservationSubjectPersonMasterBzncen,Seq("inc_dec_order_no_temp","policy_no_temp"),"leftouter")
      .selectExpr("master_id","name as child_name","gender as child_gender","child_cert_type","child_cert_no","preserve_type","start_date","end_date","insured_status","case when child_cert_type ='1' and start_date is not null then getAgeFromBirthTime(child_cert_no,start_date) else null end as age","create_time","update_time")
      .distinct()

    val res = resTemp
      .selectExpr("getUUID() as id","master_id","child_name","case when child_gender = 2 then 0 else 1 end as child_gender",
        "case when child_cert_type = 1 then 1 else -1 end as child_cert_type ","child_cert_no",
        "case when preserve_type = 1 then 1 when preserve_type = 2 then 2 when preserve_type = 5 then 3 else -1 end as preserve_type",
        "start_date","end_date","case when insured_status = 1 then 0 else 1 end as insured_status","age","create_time","update_time","getNow() as dw_create_time")
    res
  }

  /**
    * 读取1.0系统的从属人信息表
    * @param sqlContext
    */
  def onePreservationSlaveDetail(sqlContext:HiveContext) ={
    udfUtil(sqlContext)
    /**
      * 读取从属被保人表
      */
    val plcPolicyPreserveInsuredChildBznprd = readMysqlTable(sqlContext,"plc_policy_preserve_insured_child_bznprd")
      .selectExpr("preserve_id as master_preserve_id","insured_id","child_name","child_gender","regexp_replace(child_cert_type,'\\n','') as insured_cert_no","child_cert_type","child_cert_no","create_time","update_time")

    /**
      * 读取被保人信息表
      */
    val plcPolicyPreserveInsuredBznprd =
      sqlContext.sql("select * from sourcedb.plc_policy_preserve_insured_bznprd")
      .selectExpr("preserve_id","status as insured_status")

    /**
      * 读取保全表
      */
    val plcPolicyPreserveBznprd = readMysqlTable(sqlContext,"plc_policy_preserve_bznprd")
      .selectExpr("id","type as preserve_type","start_date","end_date")

    val resOne = plcPolicyPreserveInsuredChildBznprd.join(plcPolicyPreserveInsuredBznprd,plcPolicyPreserveInsuredChildBznprd("master_preserve_id") ===plcPolicyPreserveInsuredBznprd("preserve_id"),"leftouter")


    val res = resOne.join(plcPolicyPreserveBznprd,resOne("master_preserve_id") === plcPolicyPreserveBznprd("id"),"leftouter")
      .selectExpr("insured_id as master_id","master_preserve_id as preserve_id","child_name","child_gender","insured_cert_no as child_cert_type","child_cert_no","preserve_type","start_date","end_date","insured_status",
        "case when child_cert_type ='1' and start_date is not null then getAgeFromBirthTime(insured_cert_no,start_date) else null end as age","create_time","update_time")
      .distinct()
      .selectExpr("getUUID() as id","master_id","child_name","case when child_gender = 0 then 0 when child_gender = 1 then 1 else null end as child_gender",
        "case when child_cert_type = 1 then 1 else -1 end as child_cert_type","child_cert_no","case when preserve_type = 1 then 1 when preserve_type = 2 then 2 else -1 end as preserve_type",
        "start_date","end_date","case when insured_status = 0 then 0 when insured_status = 1 then 1 else null end as insured_status",
        "age","create_time","update_time","getNow() as dw_create_time")
    res
  }

  /**
    * hive 自定义 udf函数
    * @param sqlContext
    * @return
    */
  def udfUtil(sqlContext:HiveContext) ={
    sqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    sqlContext.udf.register("getDefault", () => {
      val str = ""
      str
    })
    sqlContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")//设置日期格式
      val date = df.format(new Date())// new Date()为获取当前系统时间
      (date + "")
    })

    sqlContext.udf.register("getAgeFromBirthTime", (cert_no: String, end: String) => getAgeFromBirthTime(cert_no, end))
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
