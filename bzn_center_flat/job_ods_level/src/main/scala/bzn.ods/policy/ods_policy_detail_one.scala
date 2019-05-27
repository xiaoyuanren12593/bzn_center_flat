package bzn.ods.policy

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import bzn.job.common.Until
import bzn.ods.util.SparkUtil
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * author:xiaoYuanRen
  * Date:2019/5/21
  * Time:9:47
  * describe: 1.0 系统保全表
  **/
object ods_policy_detail_one extends SparkUtil with Until{

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName,"")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4

    val odsPolicyDetail = oneOdsPolicyDetail(hiveContext).unionAll( twoOdsPolicyDetail(hiveContext))

//    val outputTmpDir = "/share/odsPolicyDetail"
//    val output = "odsdb.ods_policy_detail"
//    val test = twoOdsPolicyDetail(hiveContext)
//    twoOdsPolicyDetail(hiveContext).map(x => x.mkString("\001")).repartition(1).saveAsTextFile(outputTmpDir)
//    hiveContext.sql(s"""load data  inpath '$outputTmpDir' overwrite into table $output""")
//    odsPolicyDetail.write.format("parquet").mode("overwrite").save()
    odsPolicyDetail.write.mode(SaveMode.Overwrite).saveAsTable("odsdb.ods_policy_detail")
    sc.stop()
  }

  /**
    * 2.0系统保单明细表
    * @param sqlContext
    */
  def twoOdsPolicyDetail(sqlContext:HiveContext) ={
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
    val bPolicyBzncen: DataFrame = readMysqlTable(sqlContext,"b_policy_bzncen")
      .selectExpr("holder_name","policy_no as master_policy_no","insurance_policy_no as policy_code","proposal_no as order_id","product_code","proposal_no as order_code","user_code as user_id","first_insure_premium as first_premium","sum_premium as premium","status","sell_channel_code as channel_id","sell_channel_name as channel_name","start_date","end_date","continued_policy_no","insurance_name","create_time","update_time")

    /**
      * 读取投保人企业
      */
    val bPolicyHolderCompanyBzncen: DataFrame = readMysqlTable(sqlContext,"b_policy_holder_company_bzncen")
      .selectExpr("policy_no","name","industry_code","province_code","city_code","county_code")
      .map(x => {
        val policyNo = x.getAs[String]("policy_no")
        val name = x.getAs[String]("name")
        val industryCode = x.getAs[String]("industry_code")
        val provinceCode = x.getAs[String]("province_code")
        val cityCode = x.getAs[String]("city_code")
        val countyCode = x.getAs[String]("county_code")
        var belongArea = ""
        if((provinceCode!=null && cityCode != null && countyCode != null)){
          if(provinceCode.length == 6 && cityCode.length == 6 && countyCode.length == 6)
            belongArea = provinceCode.substring(0,2)+cityCode.substring(2,4)+countyCode.substring(4,6)
        }
        if(belongArea == ""){
          belongArea = null
        }
        (policyNo,name,industryCode,belongArea)
      }).toDF("policy_no","name","industry_code","belongArea")


    /**
      * 读取投保人个人在
      */
    val bPolicyHolderPersonBzncen: DataFrame = readMysqlTable(sqlContext,"b_policy_holder_person_bzncen")
      .selectExpr("policy_no","name","industry_code","province_code","city_code","county_code")
      .map(x => {
        val policyNo = x.getAs[String]("policy_no")
        val name = x.getAs[String]("name")
        val industryCode = x.getAs[String]("industry_code")
        val provinceCode = x.getAs[String]("province_code")
        val cityCode = x.getAs[String]("city_code")
        val countyCode = x.getAs[String]("county_code")
        var belongArea = ""
        if((provinceCode!=null && cityCode != null && countyCode != null)){
          if(provinceCode.length == 6 && cityCode.length == 6 && countyCode.length == 6)
            belongArea = provinceCode.substring(0,2)+cityCode.substring(2,4)+countyCode.substring(4,6)
        }
        if(belongArea == ""){
          belongArea = null
        }
        (policyNo,name,industryCode,belongArea)
      }).toDF("policy_no","name","industry_code","belongArea")


    val bPolicyHolderCompanyUnion = bPolicyHolderCompanyBzncen.unionAll(bPolicyHolderPersonBzncen)

    /**
      * 读取产品表
      */
    val bsProductBzncen: DataFrame = readMysqlTable(sqlContext,"bs_product_bzncen")
      .selectExpr("product_code as product_code_2","product_name")


    /**
      * 读取被保人企业
      */
    val bPolicySubjectCompanyBzncen: DataFrame = readMysqlTable(sqlContext,"b_policy_subject_company_bzncen")
      .selectExpr("policy_no","name as insured_name")


    /**
      * 读取被保人信息
      */
    val bPolicySubjectPersonMasterBzncen: DataFrame = readMysqlTable(sqlContext,"b_policy_subject_person_master_bzncen")
      .selectExpr("policy_no","name as insured_name")

    val bPolicySubject = bPolicySubjectCompanyBzncen.unionAll(bPolicySubjectPersonMasterBzncen)


    /**
      * 保单表和投保人表进行关联
      */
    val bPolicyHolderCompany = bPolicyBzncen.join(bPolicyHolderCompanyUnion,bPolicyBzncen("master_policy_no") ===bPolicyHolderCompanyUnion("policy_no"),"leftouter")
      .selectExpr("holder_name","name as holder_company_person_name","master_policy_no","policy_code","order_id","product_code","order_code","user_id","first_premium","premium","status","channel_id","channel_name","start_date","end_date","continued_policy_no","insurance_name","industry_code","belongArea","create_time","update_time")

    /**
      * 上结果与产品表进行关联
      */
    val bPolicyHolderCompanyProduct = bPolicyHolderCompany.join(bsProductBzncen,bPolicyHolderCompany("product_code")===bsProductBzncen("product_code_2"),"leftouter")
      .selectExpr("holder_name","holder_company_person_name","master_policy_no","policy_code","order_id","product_code","product_name","order_code","user_id","first_premium","premium","status","channel_id","channel_name","start_date","end_date","continued_policy_no","insurance_name","industry_code","belongArea","create_time","update_time")

    /**
      * 与被保人信息表关联
      */
    val bPolicyHolderCompanyProductInsured = bPolicyHolderCompanyProduct.join(bPolicySubject,bPolicyHolderCompanyProduct("master_policy_no") ===bPolicySubject("policy_no"),"leftouter")
      .selectExpr("holder_name","holder_company_person_name","master_policy_no","policy_code","order_id","product_code","product_name","order_code","user_id","first_premium","premium","insured_name","status","channel_id","channel_name","start_date","end_date","continued_policy_no","insurance_name","industry_code","belongArea","create_time","update_time")

    bPolicyHolderCompanyProductInsured.registerTempTable("bPolicyHolderCompanyProductTemp")

    /**
      * 创建一个临时表
      */
    val bPolicyHolderCompanyProductNew = sqlContext.sql("select *,case when a.status='1' and a.end_date>=now() then '1' else '0' end as policy_status,case when a.holder_company_person_name = null then a.holder_name ELSE a.holder_company_person_name end as holder_name_new from bPolicyHolderCompanyProductTemp a")
      .selectExpr("getUUID() as id","order_id","order_code","user_id","product_code","product_name","master_policy_no as policy_id","policy_code","first_premium","premium as sum_premium","holder_name_new as holder_name","insured_name as insured_subject","start_date as policy_start_date","end_date as policy_end_date","policy_status","continued_policy_no as preserve_policy_no","insurance_name as insure_company_name","belongArea as belongs_regional","industry_code as belongs_industry","channel_id","channel_name","create_time as policy_create_time","update_time as policy_update_time","getNow() as dw_create_time")

    bPolicyHolderCompanyProductNew
  }

  /**
    * 1.0系统保单明细表
    * @param sqlContext
    */
  def oneOdsPolicyDetail(sqlContext:HiveContext)={
    sqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    sqlContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")//设置日期格式
      val date = df.format(new Date())// new Date()为获取当前系统时间
      (date + "")
    })
    /**
      * 读取订单信息表
      */
      val odrOrderInfoBznprd: DataFrame = readMysqlTable(sqlContext,"odr_order_info_bznprd")
        .selectExpr("id as master_order_id","order_code","user_id","pay_amount")


    /**
      * 读取1.0保单信息
      */
    val odrPolicyBznprd: DataFrame = readMysqlTable(sqlContext,"odr_policy_bznprd")
      .selectExpr("id as master_policy_id","policy_code","order_id","insure_code","premium","status","channelId","channel_name","start_date","end_date","renewal_policy_code","insure_company_name","create_time","update_time")


    /**
      * 读取投保人信息表
      */
    val odrPolicyHolderBznprdTemp = readMysqlTable(sqlContext,"odr_policy_holder_bznprd")
      .selectExpr("policy_id","name","province","city","district")
      .map(x => {
        val policyId = x.getAs[String]("policy_id")
        val name = x.getAs[String]("name")
        val province = x.getAs[String]("province")
        val city = x.getAs[String]("city")
        val district = x.getAs[String]("district")
        var belongArea = ""
        if((province!=null && city != null && district != null)){
          if(province.length == 6 && city.length == 6 && district.length == 6)
          belongArea = province.substring(0,2)+city.substring(2,4)+district.substring(4,6)
        }
        (policyId,name,belongArea)
      })

    /**
      * 读取被保企业信息表
      */
    val odrPolicyInsurantBznprd: DataFrame = readMysqlTable(sqlContext,"odr_policy_insurant_bznprd")
      .selectExpr("policy_id","name as insured_subject")


    /**
      * 读取产品表
      */
    val pdtProductBznprd: DataFrame = readMysqlTable(sqlContext,"pdt_product_bznprd")
      .selectExpr("code as product_code","name as product_name")


    /**
      * 读取子保单表
      */
    val odrOrderItemInfoBznprd: DataFrame = readMysqlTable(sqlContext,"odr_order_item_info_bznprd")
      .selectExpr("order_id","industry_code")

    import sqlContext.implicits._
    val odrPolicyHolderBznprd = odrPolicyHolderBznprdTemp.toDF("policy_id","holder_subject","belongArea")

    val orderPolicy = odrOrderInfoBznprd.join(odrPolicyBznprd,odrOrderInfoBznprd("master_order_id") === odrPolicyBznprd("order_id"),"leftouter")
      .selectExpr("master_order_id","order_code","user_id","pay_amount","master_policy_id","policy_code","insure_code","premium","status","channelId","channel_name","start_date","end_date","renewal_policy_code","insure_company_name","create_time","update_time")

    val orderPolicyProduct = orderPolicy.join(pdtProductBznprd,orderPolicy("insure_code") === pdtProductBznprd("product_code"),"leftouter")
      .selectExpr("master_order_id","order_code","user_id","pay_amount","master_policy_id","policy_code","insure_code","product_name","premium","status","channelId","channel_name","start_date","end_date","renewal_policy_code","insure_company_name","create_time","update_time")

    val orderPolicyProductHolder = orderPolicyProduct.join(odrPolicyHolderBznprd,orderPolicyProduct("master_policy_id") === odrPolicyHolderBznprd("policy_id"),"leftouter")
      .selectExpr("master_order_id","order_code","user_id","pay_amount","master_policy_id","policy_code","insure_code","product_name","premium","holder_subject","status","channelId","channel_name","start_date","end_date","renewal_policy_code","insure_company_name","belongArea","create_time","update_time")

    val orderPolicyProductHolderInsurant = orderPolicyProductHolder.join(odrPolicyInsurantBznprd,orderPolicyProductHolder("master_policy_id") === odrPolicyInsurantBznprd("policy_id"),"leftouter")
      .selectExpr("master_order_id","order_code","user_id","pay_amount","master_policy_id","policy_code","insure_code","product_name","premium","holder_subject","insured_subject","status","channelId","channel_name","start_date","end_date","renewal_policy_code","insure_company_name","belongArea","create_time","update_time")

    val orderPolicyProductHolderInsurantItemOrder = orderPolicyProductHolderInsurant.join(odrOrderItemInfoBznprd,orderPolicyProductHolderInsurant("master_order_id") === odrOrderItemInfoBznprd("order_id"),"leftouter")
      .selectExpr("getUUID() as id","master_order_id as order_id","order_code","user_id","insure_code as product_code","product_name","master_policy_id as policy_id ","policy_code","pay_amount as first_premium","premium as sum_premium","holder_subject as holder_name","insured_subject","start_date as policy_start_date","end_date as policy_end_date","status as policy_status","renewal_policy_code as preserve_policy_no" ,"insure_company_name","belongArea as belongs_regional","industry_code as belongs_industry","channelId","channel_name","create_time as policy_create_time","update_time as policy_update_time","getNow() as dw_create_time")
      .cache()
    val orderPolicyProductHolderInsurantItemOrderone = orderPolicyProductHolderInsurantItemOrder
      .where("order_id not in ('934cec7f92f54be7812cfcfa23a093cb') ")
      .where("(user_id not in ('10100080492') or user_id is null) and product_code not in ('15000001')")
    val orderPolicyProductHolderInsurantItemOrderTwo = orderPolicyProductHolderInsurantItemOrder
      .where("" +
        " in ('15000001') and (user_id not in ('10100080492') or user_id is null)")
    val res = orderPolicyProductHolderInsurantItemOrderone.unionAll(orderPolicyProductHolderInsurantItemOrderTwo)

    res
//    res.write.mode(SaveMode.Overwrite).saveAsTable("odsdb.ods_policy_detail")

  }

  /**
    * 获取 Mysql 表的数据
    *
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

