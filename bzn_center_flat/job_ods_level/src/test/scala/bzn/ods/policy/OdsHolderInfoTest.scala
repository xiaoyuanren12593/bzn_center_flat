package bzn.ods.policy

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import bzn.job.common.Until
import bzn.ods.util.SparkUtil
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * author:xiaoYuanRen
  * Date:2019/5/27
  * Time:9:49
  * describe: 投保人信息表
  **/
object OdsHolderInfoTest extends SparkUtil with Until{
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName,"local[*]")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    val oneRes: DataFrame = oneHolderInfoDetail(hiveContext)
    val twoRes: DataFrame = twoHolderInfoDetail(hiveContext)
    val res = unionOneAndTwo(hiveContext,oneRes,twoRes)
    res.printSchema()
//    res.write.mode(SaveMode.Overwrite).saveAsTable("odsdb.ods_holder_detail")
    sc.stop()
  }

  /**
    * 将1.0和2.0数据进行合并
    * @param one 1.0结果
    * @param two 2.0结果
    */
  def unionOneAndTwo(sqlContext:HiveContext,one:DataFrame,two:DataFrame) ={
    sqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    sqlContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")//设置日期格式
      val date = df.format(new Date())// new Date()为获取当前系统时间
      (date + "")
    })
    val res = one.unionAll(two)
      .distinct()
      .selectExpr("getUUID() as id","policy_id","holder_name","holder_cert_type","holder_cert_no","birthday","gender","mobile","email",
        "bank_card_no","bank_name","getNow() as dw_create_time")
    res
  }

  /**
    * 读取2.0保单信息表
    * @param sqlContext
    */
  def twoHolderInfoDetail(sqlContext:HiveContext) ={
    import sqlContext.implicits._
    sqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    sqlContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")//设置日期格式
      val date = df.format(new Date())// new Date()为获取当前系统时间
      (date + "")
    })

    /**
      * 读取保单表
      */
    val bPolicyBznprd = readMysqlTable(sqlContext,"b_policy_bzncen")
      .selectExpr("id as policy_id","policy_no as master_policy_no")

    /**
      * 读取投保人
      */

    val bpolicyHolderPersonBzncen = readMysqlTable(sqlContext,"b_policy_holder_person_bzncen")
      .where("length(name) > 0")
      .selectExpr("policy_no","name as holder_name","cert_type as holder_cert_type","cert_no as holder_cert_no","birthday",
        "sex as gender","tel as mobile","email","bank_account as bank_card_no","bank_name")
      .map(x => {
        val policyNo = x.getAs[String]("policy_no")
        val holderName = x.getAs[String]("holder_name")
        val holderCertType = x.getAs[Int]("holder_cert_type")
        val holderCertNo = x.getAs[String]("holder_cert_no")
        var birthday = x.getAs[Timestamp]("birthday").toString()
        if(birthday != null && birthday.length > 18){
          birthday = birthday.substring(0,10)
        }
        val gender = x.getAs[Int]("gender")
        val mobile = x.getAs[String]("mobile")
        val email = x.getAs[String]("email")
        val bankAccount = x.getAs[String]("bank_card_no")
        val bankName = x.getAs[String]("bank_name")
        (policyNo,holderName,holderCertType,holderCertNo,birthday,gender,mobile,email,bankAccount,bankName)
      })
      .toDF("policy_no","holder_name","holder_cert_type","holder_cert_no","birthday","gender","mobile","email","bank_card_no","bank_name")

    val odsHolderRes = bPolicyBznprd.join(bpolicyHolderPersonBzncen,bPolicyBznprd("master_policy_no")===bpolicyHolderPersonBzncen("policy_no"))
      .selectExpr("policy_id","holder_name","case when holder_cert_type = 1 then 1 else -1 end as holder_cert_type","holder_cert_no","birthday",
        "case when gender = 2 then 0 else 1 end as gender","mobile","email","bank_card_no","bank_name")
//      .selectExpr("getUUID() as id","holder_name","holder_cert_type","holder_cert_no","birthday","gender","mobile","email","getNow() as dw_create_time")
    odsHolderRes.printSchema()
    odsHolderRes
  }

  /**
    * 1.0获取投保人信息
    * @param sqlContext
    */
  def oneHolderInfoDetail(sqlContext:HiveContext) ={
    sqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    sqlContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")//设置日期格式
      val date = df.format(new Date())// new Date()为获取当前系统时间
      (date + "")
    })

    /**
      * 读取投保人信息表
      */
    val odrPolicyBznprd = readMysqlTable(sqlContext,"odr_policy_bznprd")
      .selectExpr("id")

    /**
      * 读取投保人信息表
      */
    val odrPolicyHolderBznprd = readMysqlTable(sqlContext,"odr_policy_holder_bznprd")
      .where("holder_type = 2 and length(name) > 0")
      .selectExpr("policy_id","name as holder_name","cert_type as holder_cert_type","cert_no as holder_cert_no","birthday",
        "gender","mobile","email","ent_bank_account as bank_card_no","ent_bank_name as bank_name")

    val odsHolderRes = odrPolicyBznprd.join(odrPolicyHolderBznprd,odrPolicyBznprd("id")===odrPolicyHolderBznprd("policy_id"))
      .selectExpr("id as policy_id","holder_name","case when holder_cert_type = 1 then 1 else -1 end as holder_cert_type","holder_cert_no","birthday",
        "case when gender = 0 then 0 when gender = 1 then 1 else null end as gender","mobile","email","bank_card_no","bank_name")
//      .selectExpr("getUUID() as id","holder_name","holder_cert_type","holder_cert_no","birthday","gender","mobile","email","getNow() as dw_create_time")
    odsHolderRes.printSchema()
    odsHolderRes
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
