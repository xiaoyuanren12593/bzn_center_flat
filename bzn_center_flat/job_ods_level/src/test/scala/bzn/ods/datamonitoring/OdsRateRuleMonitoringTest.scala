package bzn.ods.datamonitoring

import java.text.SimpleDateFormat
import java.util.Date

import bzn.job.common.{MysqlUntil, Until}
import bzn.other.OdsEarlyWarningMonitoring.{readMysqlTable, sparkConfInfo}
import bzn.util.SparkUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext

object OdsRateRuleMonitoringTest extends SparkUtil with Until with MysqlUntil {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "local[*]")

    val sc = sparkConf._2
    val sqlContext = sparkConf._3
    val hiveContext = sparkConf._4


    //经纪费
    val bPolicyProductPlanBzncen1 = MysqlRateRules(sqlContext, "sourced", "b_policy_product_plan_bzncen",
      "brokerage_fee", "mysql.username.106",
      "mysql.password.106", "mysql.driver",
      "mysql.url.106")

    //技术服务费
    val bPolicyProductPlanBzncen2 = MysqlRateRules(sqlContext, "sourced", "b_policy_product_plan_bzncen",
      "technology_fee", "mysql.username.106",
      "mysql.password.106", "mysql.driver",
      "mysql.url.106")

    //返佣费
    val bPolicyProductPlanBzncen3 = MysqlRateRules(sqlContext, "sourced", "b_policy_product_plan_bzncen",
      "brokerage_percent", "mysql.username.106",
      "mysql.password.106", "mysql.driver",
      "mysql.url.106")
    val res = bPolicyProductPlanBzncen1.unionAll(bPolicyProductPlanBzncen2).unionAll(bPolicyProductPlanBzncen3)



    //写入规则监控级别
    saveASMysqlTable(res, "dm_rate_rule_monitoring_detail", SaveMode.Overwrite,
      "mysql.username.103",
      "mysql.password.103",
      "mysql.driver",
      "mysql.url.103.dmdb")

  }

  //Mysql表费率超过正常范围值
  def MysqlRateRules(SQLContext: SQLContext, houseName: String, tableName: String, fieldType: String, user: String, pass: String, driver: String, url: String): DataFrame = {
    import SQLContext.implicits._
    SQLContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") //设置日期格式
      val date = df.format(new Date()) // new Date()为获取当前系统时间
      date + ""
    })
    var table = tableName
    var field = fieldType
    var username = user
    var password = pass
    val house = houseName
    var drivers = driver
    var urls = url
    //规则一 小数类的值超过1 预警
    val resTemp: DataFrame = readMysqlTable(SQLContext, table, username, password, drivers, urls)
      .selectExpr(s"cast($field as double) as field")
      .map(x => {
        val rate = x.getAs[Double]("field")
        val rateStr = if (rate/100 >= 0 && rate/100 < 1) {
          house + "\u0001" + s"$table" + "\u0001" + s"$field"+ "\u0001" + 0
        } else if (rate/100 >= 1) {
          house + "\u0001" + s"$table" + "\u0001" + s"$field" + "\u0001" + 2
        } else if (rate/100 == null) {
          house + "\u0001" + s"$table" + "\u0001" + s"$field" + "\u0001" + 0
        } else {
          house + "\u0001" + s"$table" + "\u0001" + s"$field" + "\u0001" + 0
        }
        val reString: Array[String] = rateStr.split("\u0001")
        (reString(0), reString(1), reString(2), reString(3))
      }).toDF("monitoring_house", "monitoring_table", "monitoring_field","rate_range_monitoring")
    val resTable = resTemp.
      selectExpr(
        "monitoring_house",
        "monitoring_table",
        "monitoring_field",
        "rate_range_monitoring",
        "getNow() as create_time",
        "getNow() as update_time")
    resTable.registerTempTable("rateRangeMonitoring")

    val res = SQLContext.sql("select monitoring_house,monitoring_table,monitoring_field,rate_range_monitoring,count(1) as level_counts from rateRangeMonitoring group by monitoring_house,monitoring_table,monitoring_field,rate_range_monitoring")

    res
  }



}