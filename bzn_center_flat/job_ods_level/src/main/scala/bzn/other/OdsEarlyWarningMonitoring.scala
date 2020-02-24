package bzn.other

import java.text.SimpleDateFormat
import java.util.Date

import bzn.job.common.{MysqlUntil, Until}
import bzn.ods.monitoring.OdsLevelMonitoring.sparkConfInfo
import bzn.util.SparkUtil
import org.apache.derby.impl.services.locks.TableNameInfo
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object OdsEarlyWarningMonitoring extends SparkUtil with Until with MysqlUntil {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "")

    val sc = sparkConf._2
    val sqlContext = sparkConf._3
    val hiveContext = sparkConf._4

    // MysqlPecialCharacter(sqlContext, "b_policy", "policy_no", "mysql.username.103", "mysql.password.103", "mysql.driver", "mysql_url.103.odsdb")
    //HivePecialCharacter(hiveContext,"odsdb.ods_policy_detail","policy_code")
    //MysqlRateRules(sqlContext, "b_policy_product_plan", "brokerage_percent", "mysql.username.103", "mysql.password.103", "mysql.driver", "mysql_url.103.odsdb")
    //HiveRateRules(hiveContext, "odsdb.ods_policy_product_plan_detail", "commission_rate")
    //source保单
    //监控2.0保单特殊字符
    val policyTwo = MysqlPecialCharacter(sqlContext, "b_policy_bzncen", "insurance_policy_no", "mysql.username.106", "mysql.password.106", "mysql.driver", "mysql.url.106")
    //监控1.0保单特殊字符
    val policyOne = MysqlPecialCharacter(sqlContext, "odr_policy_bznprd", "policy_code", "mysql.username.106", "mysql.password.106", "mysql.driver", "mysql.url.106")
    //监控2.0保全信息
    val preserveTwo = MysqlPecialCharacter(sqlContext, "b_policy_preservation_bzncen", "id", "mysql.username.106", "mysql.password.106", "mysql.driver", "mysql.url.106")
    //监控1.0保全信息
    val preserveNoe = MysqlPecialCharacter(sqlContext, "plc_policy_preserve_bznprd", "id", "mysql.username.106", "mysql.password.106", "mysql.driver", "mysql.url.106")
    //监控2.0人员信息
    val insuredTwo = HivePecialCharacter(hiveContext, "sourcedb.b_policy_subject_person_master_bzncen", "name")
    //监控1.0人员信息
    val insuredOne = HivePecialCharacter(hiveContext, "sourcedb.odr_policy_insured_bznprd", "name")
    //监控费率字段是否在范围内
    val rateOds = MysqlRateRules(sqlContext, "ods_product_rate", "economic_rate", "mysql.username", "mysql.password", "mysql.driver", "mysql.url")
    val res = policyOne.unionAll(policyTwo).unionAll(preserveNoe).unionAll(preserveTwo).unionAll(insuredOne).unionAll(insuredTwo).unionAll(rateOds)
    saveASMysqlTable(res,"dm_monitorin_detail_test",
    SaveMode.Overwrite,"mysql.username.103",
      "mysql.password.103",
      "mysql.driver","mysql.url.103.dmdb")
  }


  //mysql表字符串匹配特殊字符
  def MysqlPecialCharacter(SQLContext: SQLContext, tableName: String, fieldType: String, user: String, pass: String, driver: String, url: String): DataFrame = {
    SQLContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") //设置日期格式
      val date = df.format(new Date()) // new Date()为获取当前系统时间
      date + ""
    })
    import SQLContext.implicits._
    var table = tableName
    var field = fieldType
    var username = user
    var password = pass
    var drivers = driver
    var urls = url

    val resTemp: DataFrame = readMysqlTable(SQLContext, table, username, password, drivers, urls)
      .selectExpr(s"cast($field as string) as field")
      .map(x => {
        val fieldInfo = x.getAs[String]("field")
        //匹配空格
        val Space: Boolean = SpaceMatching(fieldInfo)
        //匹配换行
        val Linefeed = LinefeedMatching(fieldInfo)
        val str = if (Space == true) {
          fieldInfo + "\u0001" + table + "\u0001" + "有空格" + "\u0001" + "字段信息有空格" + "\u0001" + 1 + "\u0001" + 2
        } else if (Linefeed == true) {
          fieldInfo + "\u0001" + table + "\u0001" + "有换行" + "\u0001" + "字段信息有换行" + "\u0001" + 1 + "\u0001" + 1
        } else {
          fieldInfo + "\u0001" + table + "\u0001" + "无空格,无换行" + "\u0001" + "字段信息正确" + "\u0001" + 1 + "\u0001" + 0
        }
        val strings = str.split("\u0001")
        (strings(0), strings(1), strings(2), strings(3), strings(4), strings(5))
      }).toDF("rule_type", "rule_table", "rule_name", "rule_desc", "rule_status", "rule_level")
    val res = resTemp.
      selectExpr(
        "rule_type",
        "rule_table",
        "rule_name",
        "rule_desc",
        "rule_status",
        "rule_level",
        "getNow() as create_time",
        "getNow() as update_time")
    res
  }

  //hive匹配特殊字符串
  def HivePecialCharacter(hiveContext: HiveContext, tableName: String, fieldType: String): DataFrame = {
    import hiveContext.implicits._
    hiveContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") //设置日期格式
      val date = df.format(new Date()) // new Date()为获取当前系统时间
      date + ""
    })
    var table = tableName
    var field = fieldType
    val resTemp: DataFrame = hiveContext.sql(s"select cast($field as string) as field from $table group by $field")
      .map(x => {
        val fieldInfo = x.getAs[String]("field")
        //匹配空格
        val Space: Boolean = SpaceMatching(fieldInfo)
        //匹配换行
        val Linefeed = LinefeedMatching(fieldInfo)
        val str = if (Space == true) {
          fieldInfo + "\u0001" + table + "\u0001" + "有空格" + "\u0001" + "字段信息有空格" + "\u0001" + 1 + "\u0001" + 2
        } else if (Linefeed == true) {
          fieldInfo + "\u0001" + table + "\u0001" + "有换行" + "\u0001" + "字段信息有换行" + "\u0001" + 1 + "\u0001" + 1
        } else {
          fieldInfo + "\u0001" + table + "\u0001" + "无空格,无换行" + "\u0001" + "字段信息正确" + "\u0001" + 1 + "\u0001" + 0
        }
        val strings = str.split("\u0001")
        (strings(0), strings(1), strings(2), strings(3), strings(4), strings(5))
      }).toDF("rule_type", "rule_table", "rule_name", "rule_desc", "rule_status", "rule_level")
    val res = resTemp.
      selectExpr(
        "rule_type",
        "rule_table",
        "rule_name",
        "rule_desc",
        "rule_status",
        "rule_level",
        "getNow() as create_time",
        "getNow() as update_time")
    res
  }


  //Mysql表费率超过正常范围值
  def MysqlRateRules(SQLContext: SQLContext, tableName: String, fieldType: String, user: String, pass: String, driver: String, url: String): DataFrame = {
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
    var drivers = driver
    var urls = url
    //规则一 小数类的值超过1 预警
    val resTemp: DataFrame = readMysqlTable(SQLContext, table, username, password, drivers, urls)
      .selectExpr(s"cast($field as double) as field")
      .map(x => {
        val rate = x.getAs[Double]("field")
        val rateStr = if (rate >= 0 && rate < 1) {
          rate + "\u0001" + s"$table" + "\u0001" + "无错误" + "\u0001" + "费率正常" + "\u0001" + "1" + "\u0001" + "0"
        } else if (rate >= 1) {
          rate + "\u0001" + s"$table" + "\u0001" + "有错误" + "\u0001" + "费率超过1" + "\u0001" + "1" + "\u0001" + "2"
        } else if (rate == null) {
          rate + "\u0001" + s"$table" + "\u0001" + "无错误" + "\u0001" + "费率为空" + "\u0001" + "1" + "\u0001" + "0"
        } else {
          rate + "\u0001" + s"$table" + "\u0001" + "有错误" + "\u0001" + "费率小于1" + "\u0001" + "1" + "\u0001" + "0"
        }
        val reString: Array[String] = rateStr.split("\u0001")
        (reString(0), reString(1), reString(2), reString(3), reString(4), reString(5))
      }) toDF("rule_type", "rule_table", "rule_name", "rule_desc", "rule_status", "rule_level")
    val res = resTemp.
      selectExpr(
        "rule_type",
        "rule_table",
        "rule_name",
        "rule_desc",
        "rule_status",
        "rule_level",
        "getNow() as create_time",
        "getNow() as update_time")
    res
  }


  //hive表费率判断
  def HiveRateRules(hiveContext: HiveContext, tableName: String, fieldType: String): DataFrame = {
    import hiveContext.implicits._
    hiveContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") //设置日期格式
      val date = df.format(new Date()) // new Date()为获取当前系统时间
      date + ""
    })
    var table = tableName
    var field = fieldType
    //规则一 小数类的值超过1 预警
    val resTemp = hiveContext.sql(s"select  DISTINCT cast($field as double) as field from $table")
      .map(x => {
        val rate = x.getAs[Double]("field")
        val rateStr = if (rate >= 0 && rate < 1) {
          rate + "\u0001" + s"$table" + "\u0001" + "无错误" + "\u0001" + "费率正常" + "\u0001" + "1" + "\u0001" + "0"
        } else if (rate >= 1) {
          rate + "\u0001" + s"$table" + "\u0001" + "有错误" + "\u0001" + "费率超过1" + "\u0001" + "1" + "\u0001" + "2"
        } else if (rate == null) {
          rate + "\u0001" + s"$table" + "\u0001" + "无错误" + "\u0001" + "费率为空" + "\u0001" + "1" + "\u0001" + "0"
        } else {
          rate + "\u0001" + s"$table" + "\u0001" + "有错误" + "\u0001" + "费率小于1" + "\u0001" + "1" + "\u0001" + "0"
        }
        val reString: Array[String] = rateStr.split("\u0001")
        (reString(0), reString(1), reString(2), reString(3), reString(4), reString(5))
      }) toDF("rule_type", "rule_table", "rule_name", "rule_desc", "rule_status", "rule_level")
    val res = resTemp.
      selectExpr(
        "rule_type",
        "rule_table",
        "rule_name",
        "rule_desc",
        "rule_status",
        "rule_level",
        "getNow() as create_time",
        "getNow() as update_time")
    res
  }


  //匹配空格
  def SpaceMatching(Temp: String): Boolean = {
    if (Temp != null) {
      "\\s".r.findFirstIn(Temp).isDefined
    } else false
  }

  //匹配换行符
  def LinefeedMatching(Temp: String): Boolean = {
    if (Temp != null) {
      "\\n".r.findFirstIn(Temp).isDefined
    } else false
  }


}