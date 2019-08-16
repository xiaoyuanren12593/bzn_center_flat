package bzn.ods.policy

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import bzn.job.common.Until
import bzn.ods.util.SparkUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.hive.HiveContext

import scala.io.Source

/**
  * author:xiaoYuanRen
  * Date:2019/5/24
  * Time:10:29
  * describe: 企业信息表
  **/
object OdsEnterpriseDetailTest extends SparkUtil with Until{
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName,"local[*]")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    val res = updataEnterprise(hiveContext)

    sc.stop()
  }

  /**
    * 合并1.0和2.0企业并进行更新
    * @param sqlContext
    */
  def updataEnterprise(sqlContext:HiveContext) ={
    import sqlContext.implicits._
    sqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    sqlContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")//设置日期格式
      val date = df.format(new Date())// new Date()为获取当前系统时间
      (date + "")
    })
    sqlContext.udf.register("getDefault", () => {
      val str = ""
      str
    })
    //1.0和2.0系统合并
    val oneAndTwoEmpData =oneEnterprise(sqlContext).unionAll(twoEnterprise(sqlContext)).distinct()

    /**
      * 读取1.0企业信息表
      */
    val entEnterpriseInfoBznprd = readMysqlTable(sqlContext,"ent_enterprise_info_bznprd")
      .where("ent_name is not null")
      .selectExpr("ent_name","license_code","org_code","tax_code","office_address","office_province","office_city","office_district","office_street","create_time","update_time")
      .map(x => {
        val entName = x.getAs[String]("ent_name").trim
        val licenseCode = x.getAs[String]("license_code")
        val orgCode = x.getAs[String]("org_code")
        val taxCode = x.getAs[String]("tax_code")
        val officeAddress = x.getAs[String]("office_address")
        val officeProvince = x.getAs[Int]("office_province")
        val officeCity = x.getAs[Int]("office_city")
        val officeDistrict = x.getAs[Int]("office_district")
        val officeStreet = x.getAs[String]("office_street")
        val createTime = x.getAs[Timestamp]("create_time").toString
        val updateTime = x.getAs[Timestamp]("update_time").toString
        ((entName),(updateTime,licenseCode,orgCode,taxCode,officeAddress,officeProvince,officeCity,officeDistrict,officeStreet,createTime))
      })
      .reduceByKey((x1,x2)=>{
         val res = if(x1._1>x2._1) x1 else x2
        res
      })
      .map(x => {
        (x._1,x._2._2,x._2._3,x._2._4,x._2._5,x._2._6,x._2._7,x._2._8,x._2._9,x._2._10,x._2._1)
      })
      .toDF("ent_name_temp","license_code","org_code","tax_code","office_address","office_province","office_city","office_district","office_street","create_time","update_time")
    println("1.0")
    entEnterpriseInfoBznprd.printSchema()


     /**
      * 读取2.0投保企业信息
      */
    val bPolicyHolderCompanyBzncen = readMysqlTable(sqlContext,"b_policy_holder_company_bzncen")
      .where("name is not null")
      .selectExpr("name as ent_name","license_code","unite_credit_code as org_code","tax_code","license_address as office_address","province_code as office_province","city_code as office_city","county_code as office_district","address as office_street","create_time","update_time")
      .map(x => {
        val entName = x.getAs[String]("ent_name").trim
        val licenseCode = x.getAs[String]("license_code")
        val orgCode = x.getAs[String]("org_code")
        val taxCode = x.getAs[String]("tax_code")
        val officeAddress = x.getAs[String]("office_address")
        val officeProvince = x.getAs[String]("office_province")
        val officeCity = x.getAs[String]("office_city")
        val officeDistrict = x.getAs[String]("office_district")
        val officeStreet = x.getAs[String]("office_street")
        val createTime = x.getAs[Timestamp]("create_time").toString
        val updateTime = x.getAs[Timestamp]("update_time").toString
        ((entName),(updateTime,licenseCode,orgCode,taxCode,officeAddress,officeProvince,officeCity,officeDistrict,officeStreet,createTime))
      })
      .reduceByKey((x1,x2)=>{
        val res = if(x1._1>x2._1) x1 else x2
        res
      })
      .map(x => {
        (x._1,x._2._2,x._2._3,x._2._4,x._2._5,x._2._6,x._2._7,x._2._8,x._2._9,x._2._10,x._2._1)
      })
      .toDF("ent_name_temp","license_code","org_code","tax_code","office_address","office_province","office_city","office_district","office_street","create_time","update_time")
    println("2.0")
    bPolicyHolderCompanyBzncen.printSchema()


    /**
      * 1.0和2.0的企业信息进行合并如果有相同企业的话就用2.0的信息
      */
    val res = entEnterpriseInfoBznprd.unionAll(bPolicyHolderCompanyBzncen)
      .map(x => {
        val entNameTemp = x.getAs[String]("ent_name_temp")
        val licenseCode = x.getAs[String]("license_code")
        val orgCode = x.getAs[String]("org_code")
        val taxCode = x.getAs[String]("tax_code")
        val officeAddress = x.getAs[String]("office_address")
        val officeProvince = x.getAs[String]("office_province")
        val officeCity = x.getAs[String]("office_city")
        val officeDistrict = x.getAs[String]("office_district")
        val officeStreet = x.getAs[String]("office_street")
        val createTime = x.getAs[String]("create_time")
        val updateTime = x.getAs[String]("update_time")
        (entNameTemp,(updateTime,licenseCode,orgCode,taxCode,officeAddress,officeProvince,officeCity,officeDistrict,officeStreet,createTime))
      })
      .reduceByKey((x1,x2)=>{
        val res = x2
        res
      })
      .map(x => {
        (x._1,x._2._2,x._2._3,x._2._4,x._2._5,x._2._6,x._2._7,x._2._8,x._2._9,x._2._10,x._2._1)
      })
      .toDF("ent_name_temp","license_code","org_code","tax_code","office_address","office_province","office_city","office_district","office_street","create_time","update_time")
    println(res.count())
    val twoRes = oneAndTwoEmpData.join(res,oneAndTwoEmpData("ent_name") === res("ent_name_temp"),"leftouter")
      .selectExpr("getUUID() as id","ent_id","ent_name","license_code","org_code","tax_code","office_address","office_province","office_city","office_district","office_street","getDefault() as curr_count","getDefault() as first_policy_time","create_time","update_time","getNow() as dw_create_time")
    twoRes.show(3000)

    twoRes
  }

  /**
    * 读取2.0企业信息表
    * @param sqlContext
    * @return
    */
  def twoEnterprise(sqlContext:HiveContext) ={
    sqlContext.udf.register("getMD5", (ent_name: String) => MD5(ent_name))
    val bPolicyBzncen = readMysqlTable(sqlContext,"b_policy_bzncen")
      .select("holder_type","insurance_policy_no","holder_name")
      .where("holder_type='2' and length(insurance_policy_no)>0 and insurance_policy_no is not null")
      .selectExpr("trim(holder_name) as ent_name","getMD5(holder_name) as ent_id")
      .distinct()
    bPolicyBzncen
  }

  /**
    * 1.0读取企业信息表
    * 将投保公司使用mk5加密，变成企业的id
    * @param sqlContext
    */
  def oneEnterprise(sqlContext:HiveContext) ={
    import sqlContext.implicits._

    /**
      * 读取保单信息表
      */
    val odrPolicyBznprd = readMysqlTable(sqlContext,"odr_policy_bznprd")
      .selectExpr("id","policy_code","user_id")

    /**
      * 读取投保人表
      */
    val odrPolicyHolderBznprd = readMysqlTable(sqlContext,"odr_policy_holder_bznprd")
      .selectExpr("policy_id","company_name")

    val odrPolicyRes = odrPolicyBznprd.join(odrPolicyHolderBznprd,odrPolicyBznprd("id")===odrPolicyHolderBznprd("policy_id"),"leftouter")
      .where("length(policy_code) > 0 and length(company_name) >0 and policy_code is not null and company_name is not null and (user_id not in ('10100080492') or user_id is null)")
      .map(x=> {
        val holderName = x.getAs[String]("company_name").trim
        val entId = MD5(holderName)
        (holderName,entId)
      }).toDF("ent_name","ent_id")
      .distinct()
    odrPolicyRes
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
