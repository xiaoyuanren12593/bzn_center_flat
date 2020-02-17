package bzn.datamonitoring

import java.text.SimpleDateFormat
import java.util.Date

import bzn.datamonitoring.OdsSpecialCharacterMonitoringTest.{MysqlPecialCharacter, MysqlPecialCharacterDetail, saveASMysqlTable}
import bzn.job.common.{MysqlUntil, Until}
import bzn.other.numerationMonitoringTest.{readMysqlTable, sparkConfInfo}
import bzn.util.SparkUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext

object OdsEnumerationTypeMonitoringTest extends SparkUtil with Until with MysqlUntil {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "local[*]")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    val sqlContext = sparkConf._3

    //业务条线监控
    val businessLine =
      MysqlBussinessLineMonitorings(sqlContext, "odsdb", "ods_product_detail",
        "business_line", "mysql.username.106",
        "mysql.password.106", "mysql.driver",
        "mysql.url.106.odsdb")

    //2.0保单状态监控
    val twoPolicyStatus =
      MysqlTwoPolicyStatusMonitoring(sqlContext, "sourcedb", "b_policy_bzncen",
        "status", "mysql.username.106",
        "mysql.password.106", "mysql.driver",
        "mysql.url.106")

    //1.0保单状态监控
    val OnePolicyStatus =
      MysqlOnePolicyStatusMonitoring(sqlContext, "sourcedb", "odr_policy_bznprd",
        "status", "mysql.username.106",
        "mysql.password.106", "mysql.driver",
        "mysql.url.106")
    //2.0批单状态监控
    val twoPreserveStatus =
      MysqlTwoPreserveStatusMonitoring(sqlContext, "sourcedb", "b_policy_preservation_bzncen",
        "status", "mysql.username.106",
        "mysql.password.106", "mysql.driver",
        "mysql.url.106")

    //1.0批单状态监控
    val onePreserveStatus =
      MysqlOnePreserveStatusMonitoring(sqlContext, "sourcedb", "plc_policy_preserve_bznprd",
        "status", "mysql.username.106",
        "mysql.password.106", "mysql.driver",
        "mysql.url.106")


    val res = businessLine.unionAll(OnePolicyStatus).unionAll(twoPolicyStatus).unionAll(twoPreserveStatus).unionAll(onePreserveStatus)
    //写入
    saveASMysqlTable(res, "dm_enumeration_type_monitoring_detail", SaveMode.Overwrite,
      "mysql.username.103",
      "mysql.password.103",
      "mysql.driver",
      "mysql.url.103.dmdb")


    /*val policyTwo =
      MysqlPecialCharacterDetail(sqlContext, "sourced",
        "b_policy_bzncen", "insurance_policy_no",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")

    //写入错误明细数据
    saveASMysqlTable(policyTwo, "dm_warning_interdict_monitoring_detail", SaveMode.Overwrite,
      "mysql.username.103",
      "mysql.password.103",
      "mysql.driver",
      "mysql.url.103.dmdb")*/


  }

  //Mysql业务条线监控
  def MysqlBussinessLineMonitorings(SQLContext: SQLContext, houseName: String, tableName: String, fieldType: String, user: String, pass: String, driver: String, url: String): DataFrame = {
    SQLContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") //设置日期格式
      val date = df.format(new Date()) // new Date()为获取当前系统时间
      date + ""
    })
    import SQLContext.implicits._
    var table = tableName
    var field = fieldType
    val name = houseName
    var username = user
    var password = pass
    var drivers = driver
    var urls = url
    val bussinessLineList = List("接口", "健康", "员福", "雇主", "场景", "体育")
    val resTemp: DataFrame = readMysqlTable(SQLContext, table, username, password, drivers, urls)
      .selectExpr(s"cast($field as string) as field")
      .map(x => {
        val value = x.getAs[String]("field")
        val payStatusRes = if (bussinessLineList.contains(value)) {

          name + "\u0001" + table + "\u0001" + s"$field " + "\u0001" + 0

        } else {
          name + "\u0001" + table + "\u0001" + s"$field " + "\u0001" + 2
        }

        val split = payStatusRes.split("\u0001")
        //监控字段+当前值+正确值+预警+预警描述+监控来源
        (split(0), split(1), split(2), split(3))
      }).toDF("monitoring_house", "monitoring_table", "monitoring_field", "enumeration_type_monitoring")
    val resTable = resTemp
      .selectExpr("monitoring_house",
        "monitoring_table",
        "monitoring_field",
        "enumeration_type_monitoring")
    resTable.registerTempTable("EnumerationTypeMonitoring")
    val res = SQLContext.sql("select monitoring_house,monitoring_table,monitoring_field,enumeration_type_monitoring,count(1) as level_counts from EnumerationTypeMonitoring group by monitoring_house,monitoring_table,monitoring_field,enumeration_type_monitoring")
    res.show(100)
    res

  }

  //mysql2.0保单状态监控
  def MysqlTwoPolicyStatusMonitoring(SQLContext: SQLContext, houseName: String, tableName: String, fieldType: String, user: String, pass: String, driver: String, url: String): DataFrame = {
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
    val house = houseName
    var drivers = driver
    var urls = url
    val PolicyStatusList = List(-1, 0, 1, 4)
    val resTemp: DataFrame = readMysqlTable(SQLContext, table, username, password, drivers, urls)
      .selectExpr(s"cast($field as Int) as field")
      .map(x => {
        val value = x.getAs[Int]("field")
        val payStatusRes = if (PolicyStatusList.contains(value)) {

          house + "\u0001" + table + "\u0001" + s"$field" + "\u0001" + 0

        } else {
          house + "\u0001" + table + "\u0001" + s"$field" + "\u0001" + 2
        }

        val split = payStatusRes.split("\u0001")
        //监控字段+当前值+正确值+预警+预警描述+监控来源
        (split(0), split(1), split(2), split(3))
      }).toDF("monitoring_house", "monitoring_table", "monitoring_field", "enumeration_type_monitoring")

    val resTable = resTemp
      .selectExpr("monitoring_house",
        "monitoring_table",
        "monitoring_field",
        "enumeration_type_monitoring")
    resTable.registerTempTable("EnumerationTypeMonitoring")
    val res = SQLContext.sql("select monitoring_house,monitoring_table,monitoring_field,enumeration_type_monitoring,count(1) as level_counts from EnumerationTypeMonitoring group by monitoring_house,monitoring_table,monitoring_field,enumeration_type_monitoring")
    res.show(100)
    res

  }


  //mysql1.0保单状态监控
  def MysqlOnePolicyStatusMonitoring(SQLContext: SQLContext, houseName: String, tableName: String, fieldType: String, user: String, pass: String, driver: String, url: String): DataFrame = {
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
    val house = houseName
    var drivers = driver
    var urls = url
    val PolicyStatusList = List(null,0,1,3,5,6,7,8,9,10)
    val resTemp: DataFrame = readMysqlTable(SQLContext, table, username, password, drivers, urls)
      .selectExpr(s"cast($field as Int) as field")
      .map(x => {
        val value = x.getAs[Int]("field")
        val payStatusRes = if (PolicyStatusList.contains(value)) {

          house + "\u0001" + table + "\u0001" + s"$field" + "\u0001" + 0

        } else {
          house + "\u0001" + table + "\u0001" + s"$field" + "\u0001" + 2
        }

        val split = payStatusRes.split("\u0001")
        //监控字段+当前值+正确值+预警+预警描述+监控来源
        (split(0), split(1), split(2), split(3))
      }).toDF("monitoring_house", "monitoring_table", "monitoring_field", "enumeration_type_monitoring")

    val resTable = resTemp
      .selectExpr("monitoring_house",
        "monitoring_table",
        "monitoring_field",
        "enumeration_type_monitoring")
    resTable.registerTempTable("EnumerationTypeMonitoring")
    val res = SQLContext.sql("select monitoring_house,monitoring_table,monitoring_field,enumeration_type_monitoring,count(1) as level_counts from EnumerationTypeMonitoring group by monitoring_house,monitoring_table,monitoring_field,enumeration_type_monitoring")
    res

  }


  //Mysql支付状态监控
  def MysqlPayStatusMonitoring(SQLContext: SQLContext, houseName: String, tableName: String, fieldType: String, user: String, pass: String, driver: String, url: String): DataFrame = {
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
    val house = houseName
    var drivers = driver
    var urls = url
    val payStatusList = List(1, 0, -1)
    val resTemp: DataFrame = readMysqlTable(SQLContext, table, username, password, drivers, urls)
      .selectExpr(s"cast($field as Int) as field")
      .map(x => {
        val value = x.getAs[Int]("field")
        val payStatusRes = if (payStatusList.contains(value)) {

          house + "\u0001" + table + "\u0001" + s"$field" + "\u0001" + 0

        } else {
          house + "\u0001" + table + "\u0001" + s"$field" + "\u0001" + 2
        }

        val split = payStatusRes.split("\u0001")
        //监控字段+当前值+正确值+预警+预警描述+监控来源
        (split(0), split(1), split(2), split(3))
      }).toDF("monitoring_house", "monitoring_table", "monitoring_field", "enumeration_type_monitoring")
    val resTable = resTemp
      .selectExpr("monitoring_house",
        "monitoring_table",
        "monitoring_field",
        "enumeration_type_monitoring")
    resTable.registerTempTable("EnumerationTypeMonitoring")
    val res = SQLContext.sql("select monitoring_house,monitoring_table,monitoring_field,enumeration_type_monitoring,count(1) as level_counts from EnumerationTypeMonitoring group by monitoring_house,monitoring_table,monitoring_field,enumeration_type_monitoring")
    res

  }


  //Mysql方案类别监控
  def MysqlProfessionsMonitoring(SQLContext: SQLContext, houseName: String, tableName: String, fieldType: String, user: String, pass: String, driver: String, url: String): DataFrame = {
    SQLContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") //设置日期格式
      val date = df.format(new Date()) // new Date()为获取当前系统时间
      date + ""
    })
    import SQLContext.implicits._
    var table = tableName
    var field = fieldType
    val house = houseName
    var username = user
    var password = pass
    var drivers = driver
    var urls = url
    val ProfessionsTypeList = List("1-2类", "1-3类", "1-4类", "5类")
    val resTemp: DataFrame = readMysqlTable(SQLContext, table, username, password, drivers, urls)
      .selectExpr(s"cast($field as string) as field")
      .map(x => {
        val value = x.getAs[String]("field")
        val payStatusRes = if (ProfessionsTypeList.contains(value)) {

          house + "\u0001" + table + "\u0001" + s"$field" + "\u0001" + 0

        } else {
          house + "\u0001" + table + "\u0001" + s"$field" + "\u0001" + 2
        }

        val split = payStatusRes.split("\u0001")
        //监控字段+当前值+正确值+预警+预警描述+监控来源
        (split(0), split(1), split(2), split(3))
      }).toDF("monitoring_house", "monitoring_table", "monitoring_field", "enumeration_type_monitoring")
    val resTable = resTemp
      .selectExpr("monitoring_house",
        "monitoring_table",
        "monitoring_field",
        "enumeration_type_monitoring")
    resTable.registerTempTable("EnumerationTypeMonitoring")
    val res = SQLContext.sql("select monitoring_house,monitoring_table,monitoring_field,enumeration_type_monitoring,count(1) as level_counts from EnumerationTypeMonitoring group by monitoring_house,monitoring_table,monitoring_field,enumeration_type_monitoring")
    res

  }


  //mysql1.0保单状态监控
  def MysqlOnePreserveStatusMonitoring(SQLContext: SQLContext, houseName: String, tableName: String, fieldType: String, user: String, pass: String, driver: String, url: String): DataFrame = {
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
    val house = houseName
    var drivers = driver
    var urls = url
    val PolicyStatusList = List(2,3,4,5,6)
    val resTemp: DataFrame = readMysqlTable(SQLContext, table, username, password, drivers, urls)
      .selectExpr(s"cast($field as Int) as field")
      .map(x => {
        val value = x.getAs[Int]("field")
        val payStatusRes = if (PolicyStatusList.contains(value)) {

          house + "\u0001" + table + "\u0001" + s"$field" + "\u0001" + 0

        } else {
          house + "\u0001" + table + "\u0001" + s"$field" + "\u0001" + 2
        }

        val split = payStatusRes.split("\u0001")
        //监控字段+当前值+正确值+预警+预警描述+监控来源
        (split(0), split(1), split(2), split(3))
      }).toDF("monitoring_house", "monitoring_table", "monitoring_field", "enumeration_type_monitoring")

    val resTable = resTemp
      .selectExpr("monitoring_house",
        "monitoring_table",
        "monitoring_field",
        "enumeration_type_monitoring")
    resTable.registerTempTable("EnumerationTypeMonitoring")
    val res = SQLContext.sql("select monitoring_house,monitoring_table,monitoring_field,enumeration_type_monitoring,count(1) as level_counts from EnumerationTypeMonitoring group by monitoring_house,monitoring_table,monitoring_field,enumeration_type_monitoring")
    res

  }


  //mysql1.0保单状态监控
  def MysqlTwoPreserveStatusMonitoring(SQLContext: SQLContext, houseName: String, tableName: String, fieldType: String, user: String, pass: String, driver: String, url: String): DataFrame = {
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
    val house = houseName
    var drivers = driver
    var urls = url
    val PolicyStatusList = List(0, 1, 4)
    val resTemp: DataFrame = readMysqlTable(SQLContext, table, username, password, drivers, urls)
      .selectExpr(s"cast($field as Int) as field")
      .map(x => {
        val value = x.getAs[Int]("field")
        val payStatusRes = if (PolicyStatusList.contains(value)) {

          house + "\u0001" + table + "\u0001" + s"$field" + "\u0001" + 0

        } else {
          house + "\u0001" + table + "\u0001" + s"$field" + "\u0001" + 2
        }

        val split = payStatusRes.split("\u0001")
        //监控字段+当前值+正确值+预警+预警描述+监控来源
        (split(0), split(1), split(2), split(3))
      }).toDF("monitoring_house", "monitoring_table", "monitoring_field", "enumeration_type_monitoring")

    val resTable = resTemp
      .selectExpr("monitoring_house",
        "monitoring_table",
        "monitoring_field",
        "enumeration_type_monitoring")
    resTable.registerTempTable("EnumerationTypeMonitoring")
    val res = SQLContext.sql("select monitoring_house,monitoring_table,monitoring_field,enumeration_type_monitoring,count(1) as level_counts from EnumerationTypeMonitoring group by monitoring_house,monitoring_table,monitoring_field,enumeration_type_monitoring")
    res

  }
}
