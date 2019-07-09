package bzn.ods.policy

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import bzn.job.common.Until
import bzn.ods.util.SparkUtil
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * author:xiaoYuanRen
  * Date:2019/5/23
  * Time:9:14
  * describe: 人员清单明细表
  **/
object OdsPolicyInsuredDetail extends SparkUtil with Until{
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName,"")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    val oneDate = oneOdsPolicyInsuredDetail(hiveContext)
    val twoData = twoOdsPolicyInsuredDetail(hiveContext)
    val res = oneDate.unionAll(twoData)
    res.write.mode(SaveMode.Overwrite).saveAsTable("odsdb.ods_policy_insured_detail")
    sc.stop()
  }

  /**
    * 读取2.0保单明细表
    * @param sqlContext
    */
  def twoOdsPolicyInsuredDetail(sqlContext:HiveContext) ={
    sqlContext.udf.register("getDate", (time:String) => timeSubstring(time))
    sqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    sqlContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")//设置日期格式
      val date = df.format(new Date())// new Date()为获取当前系统时间
      (date + "")
    })
    sqlContext.udf.register("getAgeFromBirthTime", (cert_no: String, end: String) => getAgeFromBirthTime(cert_no, end))

    /**
      * 读取被保人表
      */
    val bPolicySubjectPersonMasterBzncen = sqlContext.sql("select * from sourcedb.b_policy_subject_person_master_bzncen")
      .selectExpr("id as master_id","policy_no as master_policy_no","name as insured_name","cert_type as insured_cert_type",
        "cert_no","birthday","sex as gender","tel as insured_mobile","industry_name as industry","work_type","company_name",
        "company_phone","status","start_date as insured_start_date","end_date as insured_end_date","create_time","update_time")
      .registerTempTable("bPolicySubjectPersonMasterBzncenTable")

    sqlContext.sql("select regexp_replace(insured_name,'\\n','') as insured_name_new ,regexp_replace(work_type,'\\n','') as insured_work_type ,regexp_replace(cert_no,'\\n','') as insured_cert_no,* from bPolicySubjectPersonMasterBzncenTable")
      .drop("work_type").drop("cert_no").drop("insured_name")
      .registerTempTable("bPolicySubjectPersonMasterBzncenTemp")

    /**
      * 2.0状态  1在职 2不在职   改成 0在职 1不在职
      */
    val bPolicySubjectPersonMasterBzncenTemp = sqlContext.sql("select " +
      "*,CASE WHEN a.`status` = '1'" +
      "    THEN '0'" +
      "    ELSE '1'" +
      "  END as insured_status, " +
      "case when (a.`status`='1' and insured_end_date > now() and insured_start_date< now() ) then '1' else '0' end as insure_policy_status " +
      "from bPolicySubjectPersonMasterBzncenTemp as a")

    val bPolicySubjectPersonMasterBzncenTempSchema = bPolicySubjectPersonMasterBzncenTemp.schema.map(x=> x.name):+"work_type_new" :+ "name_new"
    val bPolicySubjectPersonMasterBzncenValue = bPolicySubjectPersonMasterBzncenTemp.map(x => {
      val insuredWorkType = to_null(x.getAs[String]("insured_work_type"))
      val insuredNameNew = to_null(x.getAs[String]("insured_name_new"))
      val temp = x.toSeq:+insuredWorkType :+insuredNameNew
      temp.map(x => if(x == null) null else x.toString)
    })
    val value = bPolicySubjectPersonMasterBzncenValue.map(r=> Row(r:_*))
    val schema = StructType(bPolicySubjectPersonMasterBzncenTempSchema.map(fieldName => StructField(fieldName, StringType, nullable = true)))
    //创建dataframe
    val InsuredData = sqlContext.createDataFrame(value, schema)
      .drop("insured_work_type").drop("insured_name_new")

    /**
      * 读取保单表
      */
    var bPolicyBzncen = readMysqlTable(sqlContext,"b_policy_bzncen")
      .selectExpr("id as policy_id","policy_no","insurance_policy_no")

    val res = InsuredData.join(bPolicyBzncen,InsuredData("master_policy_no") ===bPolicyBzncen("policy_no"),"leftouter")
      .selectExpr("getUUID() as id","master_id as insured_id","policy_id","insurance_policy_no as policy_code","name_new as insured_name",
        "case when insured_cert_type = '1' then '1' else '-1' end as insured_cert_type ","insured_cert_no","birthday",
        "case when `gender` = 2 then 0 when  gender = 1 then 1 else null  end  as gender","insured_mobile","industry","work_type_new as work_type",
        "company_name","company_phone","insured_status","insure_policy_status as policy_status","getDate(insured_start_date) as start_date",
        "getDate(insured_end_date) as end_date", "case when insured_cert_type ='1' and insured_start_date is not null then getAgeFromBirthTime(insured_cert_no,insured_start_date) else null end as age",
        "getDate(create_time) as create_time","getDate(update_time) as update_time","getNow() as dw_create_time")
    res
  }

  /**
    * 1.0人员清单明细表
    * @param sqlContext
    */
  def oneOdsPolicyInsuredDetail(sqlContext:HiveContext) ={
    sqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    sqlContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")//设置日期格式
      val date = df.format(new Date())// new Date()为获取当前系统时间
      (date + "")
    })
    sqlContext.udf.register("getDate", (time:String) => timeSubstring(time))
    sqlContext.udf.register("getAgeFromBirthTime", (cert_no: String, end: String) => getAgeFromBirthTime(cert_no, end))

    /**
      * 读取被保人表
      */
    val odrPolicyInsuredBznprd = sqlContext.sql("select * from sourcedb.odr_policy_insured_bznprd")
      .selectExpr("id as master_id","policy_id","policy_code","name as insured_name","cert_type as insured_cert_type","cert_no","birthday","gender","mobile as insured_mobile",
        "industry","work_type","company_name","company_phone","status","insure_policy_status","start_date","end_date","create_time","update_time","remark")
      .registerTempTable("odrPolicyInsuredBznprdTemp")
    //odrPolicyInsuredBznprd.show()

    //替换掉姓名、工种、证件号中的换行符,然后注册成临时表
    sqlContext.sql("select regexp_replace(insured_name,'\\n','') as insured_name_new, regexp_replace(work_type,'\\n','') as insured_work_type ,regexp_replace(cert_no,'\\n','') as insured_cert_no,* from odrPolicyInsuredBznprdTemp a")
      .drop("work_type").drop("cert_no").drop("insured_name")
      .registerTempTable("a_new")

    //读取临时表数据
    val notNullWorkAndName: DataFrame = sqlContext.sql("select * from a_new")

    //获取元数据
    val fields_schema = notNullWorkAndName.schema.map(x => x.name) :+ "work_type_new" :+ "name_new"

    //将工种和姓名中的非中文替换掉使用notNull（）
    val notNullWorkAndNameTemp = notNullWorkAndName.map(x => {
      val insuredWorkType = to_null(x.getAs[String]("insured_work_type"))
      val insuredNameNew = to_null(x.getAs[String]("insured_name_new"))
      val temp = x.toSeq:+insuredWorkType :+insuredNameNew
      temp.map(x => if(x == null) null else x.toString)
    })
    val value = notNullWorkAndNameTemp.map(r => Row(r: _*))
    val schema = StructType(fields_schema.map(fieldName => StructField(fieldName, StringType, nullable = true)))
    //创建dataframe
    val InsuredData = sqlContext.createDataFrame(value, schema)
      .drop("insured_work_type").drop("insured_name_new")

    /**
      * 读取保单表
      */
    val odrPolicyBznprd = readMysqlTable(sqlContext,"odr_policy_bznprd")
      .selectExpr("id","user_id","insure_code","policy_code as policy_code_temp")

    val insuredDataPolicytemp = InsuredData.join(odrPolicyBznprd,InsuredData("policy_id") ===odrPolicyBznprd("id"),"leftouter")


    val insuredDataPolicyOne = insuredDataPolicytemp
      .where("user_id not in ('10100080492') or user_id is null")
      .where("remark not in ('obsolete') or remark is null")

    val insuredDataPolicyTwo = insuredDataPolicytemp
      .where("insure_code in ('15000001') and policy_code_temp like 'BZN%'")

    /**
      * one 和 Two 数据合并
      */
    val resTemp = insuredDataPolicyOne.unionAll(insuredDataPolicyTwo)
      .selectExpr("getUUID() as id","master_id as insured_id","policy_id","policy_code","name_new as insured_name ","case when insured_cert_type = '1' then '1' else '-1' end as insured_cert_type ",
        "insured_cert_no","birthday","case when `gender` = 0 then 0 when  gender = 1 then 1 else null  end  as gender","insured_mobile","industry",
        "work_type_new as work_type","company_name","company_phone","case when status = '0' then '0' else '1' end as insured_status",
        "case when insure_policy_status = '1' then '1' else '0' end  as policy_status",
        "getDate(start_date) as start_date", "getDate(end_date) as end_date","case when insured_cert_type ='1' and start_date is not null then getAgeFromBirthTime(insured_cert_no,start_date) else null end as age",
        "getDate(create_time) as create_time","getDate(update_time) as update_time","getNow() as dw_create_time")

    resTemp
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
  def getProPerties(): Properties = {
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
