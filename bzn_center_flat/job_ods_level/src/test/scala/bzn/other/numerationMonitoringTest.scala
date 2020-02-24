package bzn.other

import java.text.SimpleDateFormat
import java.util.Date
import java.util.regex.Pattern

import bzn.job.common.{MysqlUntil, Until}
import bzn.other.OdsEarlyWarningMonitoring.MysqlPecialCharacter
import bzn.util.SparkUtil
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.matching.Regex
import scala.collection.immutable

object numerationMonitoringTest extends SparkUtil with Until with MysqlUntil {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "local[*]")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    val sqlContext = sparkConf._3
    /*PolicyStatusMonitoring(hiveContext, sc, "odsdb.ods_policy_detail", "policy_status")
    PayStatusMonitoring(hiveContext, sc, "odsdb.ods_preservation_detail", "pay_status")*/
    //ProfessionsMonitoring(hiveContext, sc, "odsdb.ods_work_grade_dimension", "profession_type")
    //TestMonitoring(hiveContext, sc, "odsdb.ods_policy_detail", "policy_status")
    //BussinessLineMonitorings(hiveContext,  "odsdb.ods_product_detail", "business_line")
    //业务条线监控
    //val mysqlBusunessLine = MysqlBussinessLineMonitorings(sqlContext, "ods_product_detail", "business_line", "mysql.username.106", "mysql.password.106", "mysql.driver", "mysql.url.106.odsdb")
    //监控2.0保全信息2.0保全信息
   // val preserveTwo = MysqlPayStatusMonitoring(sqlContext, "b_policy_preservation_bzncen", "pay_status", "mysql.username.106", "mysql.password.106", "mysql.driver", "mysql.url.106")
    //监控1.0保全信息
    //val preserveNoe = MysqlPayStatusMonitoring(sqlContext, "plc_policy_preserve_bznprd", "status", "mysql.username.106", "mysql.password.106", "mysql.driver", "mysql.url.106")
    //MysqlPolicyStatusMonitoring(sqlContext, "b_policy_bzncen", "status", "mysql.username.106", "mysql.password.106", "mysql.driver", "mysql.url.106")
    //MysqlPolicyStatusMonitoring(sqlContext, "odr_policy_bznprd", "status", "mysql.username.106", "mysql.password.106", "mysql.driver", "mysql.url.106")

  }

  //Mysql业务条线监控
  def MysqlBussinessLineMonitorings(SQLContext: SQLContext, tableName: String, fieldType: String, user: String, pass: String, driver: String, url: String): DataFrame = {
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
    val bussinessLineList = List("接口", "健康", "员福", "雇主", "场景", "体育")
    val resTemp: DataFrame = readMysqlTable(SQLContext, table, username, password, drivers, urls)
      .selectExpr(s"cast($field as string) as field").distinct
      .map(x => {
        val value = x.getAs[String]("field")
        val payStatusRes = if (bussinessLineList.contains(value)) {
          //监控字段+当前值+正确值+预警+预警描述+监控来源
          "正常值:" + value + "\u0001" + table + "\u0001" + "无新增" + "\u0001" + "与源端一致" + "\u0001" + 1 + "\u0001" + 0

        } else {
          "新增值:" + value + "\u0001" + table + "\u0001" + "新增" + "\u0001" + "本地有而源端没有" + "\u0001" + 1 + "\u0001" + 2
        }

        val split = payStatusRes.split("\u0001")
        //监控字段+当前值+正确值+预警+预警描述+监控来源
        (split(0), split(1), split(2), split(3), split(4), split(5))
      }).toDF("rule_type", "rule_table", "rule_name", "rule_desc", "rule_status", "rule_level")
    val res = resTemp
      .selectExpr("rule_type",
        "rule_table",
        "rule_name",
        "rule_desc",
        "rule_status",
        "rule_level",
        "getNow() as create_time",
        "getNow() as update_time")
    res.show(100)
    res

  }

  //Hive业务条线监控
  def HiveBussinessLineMonitorings(hiveContext: HiveContext, tableName: String, fieldType: String) = {
    import hiveContext.implicits._
    var table = tableName
    var field = fieldType
    val bussinessLineList = List("接口", "健康", "员福", "雇主", "场景", "体育")
    val res: DataFrame = hiveContext.sql(s"select DISTINCT cast($field as String) as field from $table")
      .map(x => {
        val value = x.getAs[String]("field")

        val payStatusRes = if (bussinessLineList.contains(value)) {
          //监控字段+当前值+正确值+预警+预警描述+监控来源
          value + "\u0001" + table + "\u0001" + "无新增" + "\u0001" + "与源端一致" + "\u0001" + 1 + "\u0001" + 0

        } else {
          value + "\u0001" + table + "\u0001" + "新增" + "\u0001" + "本地有而源端没有" + "\u0001" + 1 + "\u0001" + 2
        }

        val split = payStatusRes.split("\u0001")
        //监控字段+当前值+正确值+预警+预警描述+监控来源
        (split(0), split(1), split(2), split(3), split(4), split(5))
      }).toDF("rule_type", "rule_table", "rule_name", "rule_desc", "rule_status", "rule_lev*/ el")
    res.show(100)
    res

  }

  //mysql保单状态监控
  def MysqlPolicyStatusMonitoring(SQLContext: SQLContext, tableName: String, fieldType: String, user: String, pass: String, driver: String, url: String): DataFrame = {
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
    val PolicyStatusList = List(1, 0, -1, 99)
    val resTemp: DataFrame = readMysqlTable(SQLContext, table, username, password, drivers, urls)
      .selectExpr(s"cast($field as Int) as field").distinct
      .map(x => {
        val value = x.getAs[Int]("field")
        val payStatusRes = if (PolicyStatusList.contains(value)) {

          "正常值:" + value + "\u0001" + table + "\u0001" + "无新增" + "\u0001" + "与源端一致" + "\u0001" + 1 + "\u0001" + 0

        } else {
          "新增值:" + value + "\u0001" + table + "\u0001" + "新增" + "\u0001" + "本地有而源端没有" + "\u0001" + 1 + "\u0001" + 2
        }

        val split = payStatusRes.split("\u0001")
        //监控字段+当前值+正确值+预警+预警描述+监控来源
        (split(0), split(1), split(2), split(3), split(4), split(5))
      }).toDF("rule_type", "rule_table", "rule_name", "rule_desc", "rule_status", "rule_level")
    val res = resTemp
      .selectExpr("rule_type",
        "rule_table",
        "rule_name",
        "rule_desc",
        "rule_status",
        "rule_level",
        "getNow() as create_time",
        "getNow() as update_time")
    res.show(100)
    res

  }


  //Hive保单状态字段添加新值
  def PolicyStatusMonitoring(hiveContext: HiveContext, sc: SparkContext, tableName: String, fieldType: String) = {
    import hiveContext.implicits._
    var table = tableName
    var field = fieldType
    val PolicyStatusList = List(1, 0, -1, 99)
    val res: DataFrame = hiveContext.sql(s"select DISTINCT cast($field as Int) as field from $table")
      .map(x => {
        val value = x.getAs[Int]("field")

        val payStatusRes = if (PolicyStatusList.contains(value)) {
          "正常值:" + value + "\u0001" + table + "\u0001" + "无新增" + "\u0001" + "与源端一致" + "\u0001" + 1 + "\u0001" + 0

        } else {
          "新增值:" + value + "\u0001" + table + "\u0001" + "新增" + "\u0001" + "本地有而源端没有" + "\u0001" + 1 + "\u0001" + 2
        }

        val split = payStatusRes.split("\u0001")
        //监控字段+当前值+正确值+预警+预警描述+监控来源
        (split(0), split(1), split(2), split(3), split(4), split(5))
      }).toDF("rule_type", "rule_table", "rule_name", "rule_desc", "rule_status", "rule_lev*/ el")
    res.show(100)
    res

  }

  //Mysql支付状态监控
  def MysqlPayStatusMonitoring(SQLContext: SQLContext, tableName: String, fieldType: String, user: String, pass: String, driver: String, url: String): DataFrame = {
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
    val payStatusList = List(1, 0, -1)
    val resTemp: DataFrame = readMysqlTable(SQLContext, table, username, password, drivers, urls)
      .selectExpr(s"cast($field as Int) as field").distinct
      .map(x => {
        val value = x.getAs[Int]("field")
        val payStatusRes = if (payStatusList.contains(value)) {

          "正常值:" + value + "\u0001" + table + "\u0001" + "无新增" + "\u0001" + "与源端一致" + "\u0001" + 1 + "\u0001" + 0

        } else {
          "新增值:" + value + "\u0001" + table + "\u0001" + "新增" + "\u0001" + "本地有而源端没有" + "\u0001" + 1 + "\u0001" + 2
        }

        val split = payStatusRes.split("\u0001")
        //监控字段+当前值+正确值+预警+预警描述+监控来源
        (split(0), split(1), split(2), split(3), split(4), split(5))
      }).toDF("rule_type", "rule_table", "rule_name", "rule_desc", "rule_status", "rule_level")
    val res = resTemp
      .selectExpr("rule_type",
        "rule_table",
        "rule_name",
        "rule_desc",
        "rule_status",
        "rule_level",
        "getNow() as create_time",
        "getNow() as update_time")
    res.show(100)
    res

  }


  //Hive支付状态监控
  def HivePayStatusMonitoring(hiveContext: HiveContext, sc: SparkContext, tableName: String, fieldType: String) = {
    import hiveContext.implicits._
    var table = tableName
    var field = fieldType
    val payStatusList = List(1, 0, -1)
    val res: DataFrame = hiveContext.sql(s"select DISTINCT cast($field as Int) as field from $table")
      .map(x => {
        val value = x.getAs[Int]("field")
        println(payStatusList)
        val payStatusRes = if (payStatusList.contains(value)) {
          "正常值:" + value + "\u0001" + table + "\u0001" + "无新增" + "\u0001" + "与源端一致" + "\u0001" + 1 + "\u0001" + 0

        } else {
          "新增值:" + value + "\u0001" + table + "\u0001" + "新增" + "\u0001" + "本地有而源端没有" + "\u0001" + 1 + "\u0001" + 2
        }

        val split = payStatusRes.split("\u0001")
        //监控字段+当前值+正确值+预警+预警描述+监控来源
        (split(0), split(1), split(2), split(3), split(4), split(5))
      }).toDF("rule_type", "rule_table", "rule_name", "rule_desc", "rule_status", "rule_lev*/ el")
    res.show(100)
    res

  }

  //Mysql方案类别监控
  def MysqlProfessionsMonitoring(SQLContext: SQLContext, tableName: String, fieldType: String, user: String, pass: String, driver: String, url: String): DataFrame = {
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
    val ProfessionsTypeList = List("1-2类", "1-3类", "1-4类", "5类")
    val resTemp: DataFrame = readMysqlTable(SQLContext, table, username, password, drivers, urls)
      .selectExpr(s"cast($field as string) as field").distinct
      .map(x => {
        val value = x.getAs[String]("field")
        val payStatusRes = if (ProfessionsTypeList.contains(value)) {
          //监控字段+当前值+正确值+预警+预警描述+监控来源
          "正常值:" + value + "\u0001" + table + "\u0001" + "无新增" + "\u0001" + "与源端一致" + "\u0001" + 1 + "\u0001" + 0

        } else {
          "新增值:" + value + "\u0001" + table + "\u0001" + "新增" + "\u0001" + "本地有而源端没有" + "\u0001" + 1 + "\u0001" + 2
        }

        val split = payStatusRes.split("\u0001")
        //监控字段+当前值+正确值+预警+预警描述+监控来源
        (split(0), split(1), split(2), split(3), split(4), split(5))
      }).toDF("rule_type", "rule_table", "rule_name", "rule_desc", "rule_status", "rule_level")
    val res = resTemp
      .selectExpr("rule_type",
        "rule_table",
        "rule_name",
        "rule_desc",
        "rule_status",
        "rule_level",
        "getNow() as create_time",
        "getNow() as update_time")
    res.show(100)
    res

  }

  //Hive方案类别监控
  def HiveProfessionsMonitoring(hiveContext: HiveContext, tableName: String, fieldType: String) = {
    import hiveContext.implicits._
    var table = tableName
    var field = fieldType
    val ProfessionsTypeList = List("1-2类", "1-3类", "1-4类", "5类")
    val res: DataFrame = hiveContext.sql(s"select DISTINCT cast($field) as field from $table")
      .map(x => {
        val value = x.getAs[String]("field")

        val payStatusRes = if (ProfessionsTypeList.contains(value)) {
          //监控字段+当前值+正确值+预警+预警描述+监控来源
          value + "\u0001" + table + "\u0001" + "无新增" + "\u0001" + "与源端一致" + "\u0001" + 1 + "\u0001" + 0

        } else {
          value + "\u0001" + table + "\u0001" + "新增" + "\u0001" + "本地有而源端没有" + "\u0001" + 1 + "\u0001" + 2
        }

        val split = payStatusRes.split("\u0001")
        //监控字段+当前值+正确值+预警+预警描述+监控来源
        (split(0), split(1), split(2), split(3), split(4), split(5))
      }).toDF("rule_type", "rule_table", "rule_name", "rule_desc", "rule_status", "rule_lev*/ el")
    res.show(100)
    res

  }








  //测试方法
  def TestMonitoring(hiveContext: HiveContext, sc: SparkContext, tableName: String, fieldType: String) = {
    import hiveContext.implicits._
    var table = tableName
    var field = fieldType
    var x = s"$fieldType".getClass
    println(x.getClass)
    //读取原先表的字段
    val odsMonotoringDetail: immutable.Seq[String] = hiveContext.sql(s"select true_value from odsdb.ods_monotoring_detail where monotoring_source ='$table'")
      .map(x => {
        val rst = x.getAs[String]("true_value")
        rst
      }).collect().toList.sorted
    //读取现在要判断的表
    val res: Seq[Int] = hiveContext.sql(s"select DISTINCT $field from $table")
      .map(x => {
        val value = x.getAs[Int](s"$field")
        value
      }).collect().toList.sorted

    val payStatusRes = if (res.sameElements(odsMonotoringDetail) && odsMonotoringDetail != null) {
      s"$field" + "\u0001" + s"$table" + "\u0001" + res.mkString(" ") + "\u0001" + odsMonotoringDetail.mkString(" ") + "\u0001" + "true"
    } else if (odsMonotoringDetail == null) {
      s"$field" + "\u0001" + s"$table" + "\u0001" + res.mkString(" ") + "\u0001" + odsMonotoringDetail.mkString(" ") + "\u0001" + "true"

    } else {
      s"$field" + "\u0001" + s"$table" + "\u0001" + res.mkString(" ") + "\u0001" + odsMonotoringDetail.mkString(" ") + "\u0001" + "false"
    }

    val res1 = sc.parallelize(List(payStatusRes))
      .map(x => {
        val split = x.split("\u0001")
        //监控字段+当前值+正确值+预警+预警描述+监控来源
        (split(0), split(1), split(2), split(3), split(1114))
      })
    val resframe = res1.toDF("monotoring_filed", "monotoring_table", "curr_value", "true_value", "result")
    resframe.show(100)
  }

}
