package bzn.ods.policy

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
  * Date:2019/6/5
  * Time:17:13
  * describe: this is new class
  **/
object OdsPolicyProductPlanTest extends SparkUtil with Until{
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName,"local[*]")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    val res = oneProductPlan(hiveContext)
    sc.stop()
  }

  /**
    * 产品方案表
    * @param sqlContext
    */
  def oneProductPlan(sqlContext:HiveContext) ={
    sqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    sqlContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")//设置日期格式
      val date = df.format(new Date())// new Date()为获取当前系统时间
      (date + "")
    })

    import sqlContext.implicits._
    /**
      * 读取保单明细表
      */
    val odsPolicyDetailTemp1 =
      sqlContext.sql("select policy_id as policy_id_master, policy_code,product_code as insure_code,policy_status,sku_coverage,sku_append,sku_ratio," +
        "sku_price,sku_charge_type,tech_service_rate,economic_rate from odsdb.ods_policy_detail")
      .distinct()

    /**
      * 读取保单表，通过技术服务费率和经济费率算出手续费率
      */
    val odsPolicyDetailTemp2 =
      sqlContext.sql("select policy_id,tech_service_rate,economic_rate from odsdb.ods_policy_detail")
      .map(x => {
        val policyId = x.getAs[String]("policy_id")
        var techServiceRate = x.getAs[String]("tech_service_rate")
        var economicRate = x.getAs[String]("economic_rate")
        var commessionRate = ""

        if(techServiceRate == null){
          if(economicRate == null){
            commessionRate = null
          }else{
            commessionRate = economicRate
          }
        }else{
          if(economicRate == null){
            commessionRate = techServiceRate
          }else{
            commessionRate = (techServiceRate.toDouble+economicRate.toDouble).toString
          }
        }
        (policyId,commessionRate)
      }).toDF("policyId","commession_rate")

    val odsPolicyDetail = odsPolicyDetailTemp1.join(odsPolicyDetailTemp2,odsPolicyDetailTemp1("policy_id_master") === odsPolicyDetailTemp2("policyId"))
      .selectExpr("policy_code","insure_code","policy_status","sku_coverage","sku_append","sku_ratio","sku_price","sku_charge_type"
        ,"tech_service_rate","economic_rate","commession_rate")
      .distinct()

      /**
      * 读取产品表
      */
    val odsProducteDetail = sqlContext.sql("select * from odsdb.ods_product_detail")
      .selectExpr("product_code","product_name","one_level_pdt_cate","two_level_pdt_cate","business_line")

    /**
      * 读取历史方案配置表
      */
    val policy_product_plan_his = readMysqlTable(sqlContext,"policy_product_plan_his")
      .selectExpr("policy_code as policy_code_master","sku_coverage as sku_coverage_master","sku_append as sku_append_master",
        "sku_ratio as sku_ratio_master","sku_price as sku_price_master","sku_charge_type as sku_charge_type_master",
        "tech_service_rate","economic_rate")

    /**
      * 读取历史方案配置表 并计算出手续费率
      */
    val policy_product_plan_his_rate = readMysqlTable(sqlContext,"policy_product_plan_his")
      .selectExpr("policy_code","tech_service_rate","economic_rate")
      .map(x => {
        val policyCode = x.getAs[String]("policy_code")
        var techServiceRate = x.getAs[java.math.BigDecimal]("tech_service_rate")
        var economicRate = x.getAs[java.math.BigDecimal]("economic_rate")
        var commessionRate = ""

        if(techServiceRate == null){
          if(economicRate == null){
            commessionRate = null
          }else{
            commessionRate = economicRate.toString
          }
        }else{
          if(economicRate == null){
            commessionRate = techServiceRate.toString
          }else{
            commessionRate = (techServiceRate.add(economicRate)).toString
          }
        }
        (policyCode,commessionRate)
      })
      .toDF("policy_code","commession_rate")

    val policy_product_plan_his_105 = policy_product_plan_his.join(policy_product_plan_his_rate,policy_product_plan_his("policy_code_master") === policy_product_plan_his_rate("policy_code"))
      .selectExpr("policy_code_master","sku_coverage_master","sku_append_master","sku_ratio_master","sku_price_master","sku_charge_type_master",
        "tech_service_rate as tech_service_rate_master","economic_rate as economic_rate_master","commession_rate as commession_rate_master")
    /**
      * 保单表和产品表关联
      * LGB000001  17000001  众安和国寿财零工保
      */
    val policyProductRes = odsPolicyDetail.join(odsProducteDetail,odsPolicyDetail("insure_code") === odsProducteDetail("product_code"),"leftouter")
      .selectExpr("policy_code","insure_code","policy_status","product_name","one_level_pdt_cate","two_level_pdt_cate","business_line","sku_coverage",
        "sku_append","sku_ratio","sku_price","sku_charge_type","tech_service_rate","economic_rate","commession_rate")
      .where("policy_status in (0,1,-1) and policy_code is not null and length(policy_code) >0")

    /**
      * 如果policy_code_master不是null：以policy_product_plan_his_105他的值为准
      */
    val resTemp = policyProductRes.join(policy_product_plan_his_105,policyProductRes("policy_code") === policy_product_plan_his_105("policy_code_master"),"leftouter")
        .selectExpr("getUUID() as id","policy_code","insure_code as product_code","product_name","one_level_pdt_cate","two_level_pdt_cate","business_line",
        "case when policy_code_master is not null then sku_coverage_master else sku_coverage end as sku_coverage",
        "case when policy_code_master is not null then sku_append_master else sku_append end as sku_append",
        "case when policy_code_master is not null then sku_ratio_master else sku_ratio end as sku_ratio",
        "case when policy_code_master is not null then sku_price_master else sku_price end as sku_price",
        "case when policy_code_master is not null then sku_charge_type_master else sku_charge_type end as sku_charge_type",
        "case when policy_code_master is not null then tech_service_rate_master else tech_service_rate end as tech_service_rate",
        "case when policy_code_master is not null then economic_rate_master else economic_rate end as economic_rate",
        "case when policy_code_master is not null then commession_rate_master else commession_rate end as commession_rate",
        "getNow() as dw_create_time")
      .cache()

    /**
      * 众安和国寿财零工保
      */
    val firstRes = resTemp.where("product_code = 'LGB000001'")
      .selectExpr("id","policy_code","product_code","product_name","one_level_pdt_cate","two_level_pdt_cate","business_line",
        "sku_coverage","sku_append", "sku_ratio", "sku_price", "sku_charge_type", "tech_service_rate", "economic_rate", "'0.1' as commession_rate", "dw_create_time")

    val threeRes = resTemp.where("product_code = '17000001'")
      .selectExpr("id","policy_code","product_code","product_name","one_level_pdt_cate","two_level_pdt_cate","business_line",
        "sku_coverage","sku_append", "sku_ratio", "sku_price", "sku_charge_type", "tech_service_rate", "economic_rate", "'0.2' as commession_rate", "dw_create_time")

    /**
      * 其他产品的数据
      */
    val secondRes = resTemp.where("product_code <> 'LGB000001' and product_code <> '17000001'")


    val res = firstRes.unionAll(secondRes).unionAll(threeRes)

    res.printSchema()
    res
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
