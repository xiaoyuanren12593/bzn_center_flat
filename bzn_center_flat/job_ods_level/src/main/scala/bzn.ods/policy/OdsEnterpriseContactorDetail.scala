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
  * Date:2019/5/24
  * Time:17:33
  * describe: this is new class
  **/
object OdsEnterpriseContactorDetail extends SparkUtil with Until {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName,"")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    val twoRes = twoEnterpriseContactorDetail(hiveContext)
    val oneRes = oneEnterpriseContactorDetail(hiveContext)
    val res = oneRes.unionAll(twoRes)
    res.write.mode(SaveMode.Overwrite).saveAsTable("odsdb.ods_enterprise_contactor_detail")
    sc.stop()
  }

  /**
    * 读取2.0系统联系人信息
    * @param sqlContext
    */
  def twoEnterpriseContactorDetail(sqlContext:HiveContext) ={
    sqlContext.udf.register("clean", (str: String) => clean(str))
    sqlContext.udf.register("getMD5", (ent_name: String) => MD5(ent_name))
    sqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    sqlContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")//设置日期格式
      val date = df.format(new Date())// new Date()为获取当前系统时间
      (date + "")
    })
    val bPolicyBzncen = readMysqlTable(sqlContext,"b_policy_bzncen")
      .select("holder_type","policy_no","insurance_policy_no","holder_name","id")
      .where("holder_type='2' and length(insurance_policy_no)>0 and insurance_policy_no is not null")
      .selectExpr("getMD5(holder_name) as ent_id","id as policy_id","policy_no as master_policy_no")
      .distinct()

    /**
      * 读取投保人企业信息表
      */
    val bPolicyHolderCompanyBzncen = readMysqlTable(sqlContext,"b_policy_holder_company_bzncen")
      .selectExpr("policy_no","contact_name","contact_tel","contact_telephone","contact_email","contact_address")

    val res = bPolicyBzncen.join(bPolicyHolderCompanyBzncen,bPolicyBzncen("master_policy_no")===bPolicyHolderCompanyBzncen("policy_no"),"leftouter")
      .selectExpr("getUUID() as id","clean(ent_id) as ent_id","clean(cast(policy_id as String)) as policy_id",
        "clean(contact_name) as contact_name", "clean(contact_tel) as contact_mobile","clean(contact_telephone) as contact_tel",
        "clean(contact_email) as contact_email", "clean(contact_address) as contact_address","getNow() as dw_create_time")
    res
  }

  /**
    * 1.0企业联系人信息
    * @param sqlContext
    */
  def oneEnterpriseContactorDetail(sqlContext:HiveContext) ={
    import sqlContext.implicits._
    sqlContext.udf.register("clean", (str: String) => clean(str))
    sqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    sqlContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")//设置日期格式
      val date = df.format(new Date())// new Date()为获取当前系统时间
      (date + "")
    })
    /**
      * 读取保单信息表
      */
    val odrPolicyBznprd = readMysqlTable(sqlContext,"odr_policy_bznprd")
      .selectExpr("id","policy_code","user_id")

    /**
      * 读取投保人表
      */
    val odrPolicyHolderBznprd = readMysqlTable(sqlContext,"odr_policy_holder_bznprd")
      .selectExpr("policy_id","company_name","contact_name","contact_mobile","ent_telephone","contact_email","company_address")

    val odrPolicyHolderInfoRes = odrPolicyBznprd.join(odrPolicyHolderBznprd,odrPolicyBznprd("id")===odrPolicyHolderBznprd("policy_id"),"leftouter")

    /**
      * 天保新量创客空间管理（上海）有限公司的联系人信息为空，但是保单有效，单独拿出来得到保单企业信息,不然会影响保费
      */
    val resOne = odrPolicyHolderInfoRes.where("company_name = '天保新量创客空间管理（上海）有限公司'")
    val resTwo = odrPolicyHolderInfoRes.where("length(policy_code) > 0 and length(company_name) >0 and policy_code is not null and " +
      "company_name is not null and (user_id not in ('10100080492') or user_id is null) and contact_name is not null")
    val resTemp = resOne.unionAll(resTwo)
      .map(x=> {
        val holderName = x.getAs[String]("company_name").trim
        val id = x.getAs[String]("id")
        val contactName = x.getAs[String]("contact_name")
        val contactMobile = x.getAs[String]("contact_mobile")
        val entTelephone = x.getAs[String]("ent_telephone")
        val contactEmail = x.getAs[String]("contact_email")
        val companyAddress = x.getAs[String]("company_address")
        val entId = MD5(holderName)
        (entId,id,contactName,contactMobile,entTelephone,contactEmail,companyAddress)
      }).toDF("ent_id","policy_id","contact_name","contact_mobile","contact_tel","contact_email","contact_address")
      .distinct()
    val res = resTemp
      .selectExpr("getUUID() as id","clean(ent_id) as ent_id","clean(policy_id) as policy_id","clean(contact_name) as contact_name",
        "clean(contact_mobile) as contact_mobile","clean(contact_tel) as contact_tel","clean(contact_email) as contact_email",
        "clean(contact_address) as contact_address","getNow() as dw_create_time")

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
