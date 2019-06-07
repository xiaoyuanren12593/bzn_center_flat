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
  * Date:2019/6/5
  * Time:17:13
  * describe: this is new class
  **/
object OdsPolicyProductPlan extends SparkUtil with Until{
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName,"")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    val res = oneProductPlan(hiveContext)
    res.write.mode(SaveMode.Overwrite).saveAsTable("odsdb.ods_policy_product_plan")
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
    /**
      * 读取保单明细表
      */
    val odsPolicyDetail =
      sqlContext.sql("select policy_code,product_code as insure_code,policy_status,sku_coverage,sku_append,sku_ratio,sku_price,sku_charge_type from odsdb.ods_policy_detail")
      .distinct()

      /**
      * 读取产品表
      */
    val odsProducteDetail = sqlContext.sql("select * from odsdb.ods_product_detail")
      .selectExpr("product_code","product_name","one_level_pdt_cate")

    /**
      * 读取历史方案配置表
      */
    val policy_product_plan_his_105 = readMysqlTable(sqlContext,"policy_product_plan_his_105")
      .selectExpr("policy_code as policy_code_master","sku_coverage as sku_coverage_master","sku_append as sku_append_master",
        "sku_ratio as sku_ratio_master","sku_price as sku_price_master","sku_charge_type as sku_charge_type_master")

    /**
      * 保单表和产品表关联
      */
    val policyProductRes = odsPolicyDetail.join(odsProducteDetail,odsPolicyDetail("insure_code") === odsProducteDetail("product_code"),"leftouter")
      .selectExpr("policy_code","insure_code","policy_status","product_name","one_level_pdt_cate","sku_coverage","sku_append","sku_ratio","sku_price","sku_charge_type")
      .where("insure_code <> 'LGB000001' and insure_code <> '17000001' " +
        "and policy_status in (0,1) and policy_code is not null and one_level_pdt_cate = '蓝领外包'")

    val res = policyProductRes.join(policy_product_plan_his_105,policyProductRes("policy_code") === policy_product_plan_his_105("policy_code_master"),"leftouter")
      .selectExpr("getUUID() as id","policy_code","insure_code","product_name",
        "case when sku_coverage_master is not null then sku_coverage_master else sku_coverage end as sku_coverage",
        "case when sku_append_master is not null then sku_coverage_master else sku_append end as sku_append",
        "case when sku_ratio_master is not null then sku_ratio_master else sku_ratio end as sku_ratio",
        "case when sku_price_master is not null then sku_price_master else sku_price end as sku_price",
        "case when sku_charge_type_master is not null then sku_charge_type_master else sku_charge_type end as sku_charge_type",
        "getNow() as dw_create_time")

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
