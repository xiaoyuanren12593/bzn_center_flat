package bzn.datamonitoring

import java.text.SimpleDateFormat
import java.util.Date

import bzn.datamonitoring.OdsRateRuleMonitoringTest.{readMysqlTable, saveASMysqlTable, sparkConfInfo}
import bzn.job.common.{MysqlUntil, Until}
import bzn.util.SparkUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext

object OdsRateRuleMonitoringWriteDetailTest extends SparkUtil with Until with MysqlUntil {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "local[*]")

    val sc = sparkConf._2
    val sqlContext = sparkConf._3
    val hiveContext = sparkConf._4


    //错误明细监控
    val odsProductRateDetail = MysqlRateRulesDetail(sqlContext, "sourced", "ods_product_rate",
      "economic_rate", "mysql.username", "mysql.password",
      "mysql.driver", "mysql.url")


    //写入错误明细数据
    saveASMysqlTable(odsProductRateDetail, "dm_warning_interdict_monitoring_detail", SaveMode.Overwrite,
      "mysql.username.103",
      "mysql.password.103",
      "mysql.driver",
      "mysql.url.103.dmdb")
  }



  //Mysql表费率超过正常范围值
  def MysqlRateRulesDetail(SQLContext: SQLContext, houseName: String, tableName: String, fieldType: String, user: String, pass: String, driver: String, url: String): DataFrame = {
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
        val rateStr = if (rate >= 0 && rate < 1) {
          house + "\u0001"  + s"$table" +"\u0001"+ s"$field" +"\u0001"+rate+"\u0001" +  "费率正常" + "\u0001" + 0
        } else if (rate >= 1) {
          house + "\u0001"  + s"$table" +"\u0001"+ s"$field" +"\u0001"+rate+"\u0001"  + "费率超过1" + "\u0001" + 2
        } else if (rate == null) {
          house + "\u0001"  + s"$table" +"\u0001"+ s"$field" +"\u0001"+rate+"\u0001" +  "费率为空" + "\u0001" + 0
        } else {
          house + "\u0001"  + s"$table" +"\u0001"+ s"$field" +"\u0001"+rate+"\u0001"  + "费率小于1" + "\u0001" +1
        }
        val reString: Array[String] = rateStr.split("\u0001")
        (reString(0), reString(1), reString(2), reString(3), reString(4), reString(5))
      }).toDF("monitoring_house", "monitoring_table", "monitoring_field", "monitoring_text","rule_desc", "special_character_monitoring")
    val resTable = resTemp.
      selectExpr(
        "monitoring_house",
        "monitoring_table",
        "monitoring_field",
        "monitoring_text",
        "rule_desc",
        "special_character_monitoring",
        "getNow() as create_time",
        "getNow() as update_time")
    resTable.registerTempTable("specialCharacterMonitoring")
    val res = SQLContext.sql("select * from specialCharacterMonitoring where special_character_monitoring=2 or special_character_monitoring=1")
    res
  }

}