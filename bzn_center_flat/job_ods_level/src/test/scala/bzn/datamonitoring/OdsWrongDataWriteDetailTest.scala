package bzn.datamonitoring

import java.text.SimpleDateFormat
import java.util.Date

import bzn.datamonitoring.OdsEnumerationTypeMonitoringWriteDetail.{MysqlBussinessLineMonitorings, MysqlOnePolicyStatusMonitoring, MysqlOnePreserveStatusMonitoring, MysqlTwoPolicyStatusMonitoring, MysqlTwoPreserveStatusMonitoring, readMysqlTable}
import bzn.datamonitoring.OdsSpecialCharacterMonitoringTest.{LinefeedMatching, SpaceMatching, readMysqlTable, sparkConfInfo}
import bzn.job.common.{MysqlUntil, Until}
import bzn.util.SparkUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext

object OdsWrongDataWriteDetailTest extends SparkUtil with Until with MysqlUntil {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "local[*]")

    val sc = sparkConf._2
    val sqlContext = sparkConf._3
    val hiveContext = sparkConf._4


    //监控1.0保单特殊字符
    val bPolicyOne =
      MysqlPecialCharacterDetail(sqlContext, "sourced",
        "b_policy_bzncen", "insurance_policy_no",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")


    val openEmployerPolicyBznopen1 =
      MysqlPecialCharacterDetail(sqlContext, "sourced",
        "open_employer_policy_bznopen", "policy_no",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")

    val openEmployerPolicyBznopen2 =
      MysqlPecialCharacterDetail(sqlContext, "sourced",
        "open_employer_policy_bznopen", "holder_name",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")

    val openEmployerPolicyBznopen3 =
      MysqlPecialCharacterDetail(sqlContext, "sourced",
        "open_employer_policy_bznopen", "holder_cert_no",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")

    val openEmployerPolicyBznope4 =
      MysqlPecialCharacterDetail(sqlContext, "sourced",
        "open_employer_policy_bznopen", "insured_name",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")

    val openEmployerPolicyBznope5 =
      MysqlPecialCharacterDetail(sqlContext, "sourced",
        "open_employer_policy_bznopen", "insured_contact_phone",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")

    val odrPolicyBznprd1 =
      MysqlPecialCharacterDetail(sqlContext, "sourced",
        "odr_policy_bznprd", "policy_code",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")

    val odrPolicyHolderBznprd1 =
      MysqlPecialCharacterDetail(sqlContext, "sourced",
        "odr_policy_holder_bznprd", "policy_code",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")

    val odrPolicyHolderBznprd2 =
      MysqlPecialCharacterDetail(sqlContext, "sourced",
        "odr_policy_holder_bznprd", "name",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")

    val odrPolicyHolderBznprd3 =
      MysqlPecialCharacterDetail(sqlContext, "sourced",
        "odr_policy_holder_bznprd", "cert_no",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")

    val odrPolicyHolderBznprd4 =
      MysqlPecialCharacterDetail(sqlContext, "sourced",
        "odr_policy_holder_bznprd", "mobile",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")


    val odrPolicyHolderBznprd5 =
      MysqlPecialCharacterDetail(sqlContext, "sourced",
        "odr_policy_holder_bznprd", "policy_id",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")


    val odrPolicyInsurantBznprd1 =
      MysqlPecialCharacterDetail(sqlContext, "sourced",
        "odr_policy_insurant_bznprd", "policy_code",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")


    val odrPolicyInsurantBznprd2 =
      MysqlPecialCharacterDetail(sqlContext, "sourced",
        "odr_policy_insurant_bznprd", "policy_id",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")

    val odrPolicyInsurantBznprd3 =
      MysqlPecialCharacterDetail(sqlContext, "sourced",
        "odr_policy_insurant_bznprd", "name",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")


    val odrPolicyInsurantBznprd4 =
      MysqlPecialCharacterDetail(sqlContext, "sourced",
        "odr_policy_insurant_bznprd", "cert_no",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")


    val odrPolicyInsurantBznprd5 =
      MysqlPecialCharacterDetail(sqlContext, "sourced",
        "odr_policy_insurant_bznprd", "work_type",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")


    val odrPolicyInsurantBznprd6 =
      MysqlPecialCharacterDetail(sqlContext, "sourced",
        "odr_policy_insurant_bznprd", "mobile",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")


    val pdtProductBznprd1 =
      MysqlPecialCharacterDetail(sqlContext, "sourced",
        "pdt_product_bznprd", "code",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")


    val pdtProductBznprd2 =
      MysqlPecialCharacterDetail(sqlContext, "sourced",
        "pdt_product_bznprd", "name",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")


    val plcPolicyPreserveBznprd1 =
      MysqlPecialCharacterDetail(sqlContext, "sourced",
        "plc_policy_preserve_bznprd", "policy_id",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")


    val plcPolicyPreserveBznprd2 =
      MysqlPecialCharacterDetail(sqlContext, "sourced",
        "plc_policy_preserve_bznprd", "policy_code",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")


    val plcPolicyPreserveBznprd3 =
      MysqlPecialCharacterDetail(sqlContext, "sourced",
        "plc_policy_preserve_bznprd", "add_batch_code",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")


    val plcPolicyPreserveBznprd4 =
      MysqlPecialCharacterDetail(sqlContext, "sourced",
        "plc_policy_preserve_bznprd", "del_batch_code",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")


    val bPolicyBzncen1 =
      MysqlPecialCharacterDetail(sqlContext, "sourced",
        "b_policy_bzncen", "policy_no",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")

    val bPolicyBzncen5 =
      MysqlPecialCharacterDetail(sqlContext, "sourced",
        "b_policy_bzncen", "insurance_policy_no",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")

    val bPolicyBzncen2 =
      MysqlPecialCharacterDetail(sqlContext, "sourced",
        "b_policy_bzncen", "sum_premium",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")

    val bPolicyBzncen3 =
      MysqlPecialCharacterDetail(sqlContext, "sourced",
        "b_policy_bzncen", "operation_user_name",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")

    val bPolicyBzncen4 =
      MysqlPecialCharacterDetail(sqlContext, "sourced",
        "b_policy_bzncen", "sell_channel_name",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")


    val bPolicyHolderPersonBzncen1 =
      MysqlPecialCharacterDetail(sqlContext, "sourced",
        "b_policy_holder_person_bzncen", "policy_no",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")

    val bPolicyHolderPersonBzncen2 =
      MysqlPecialCharacterDetail(sqlContext, "sourced",
        "b_policy_holder_person_bzncen", "name",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")

    val bPolicyHolderPersonBzncen3 =
      MysqlPecialCharacterDetail(sqlContext, "sourced",
        "b_policy_holder_person_bzncen", "cert_no",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")


    val bPolicyProductPlanBzncen1 =
      MysqlPecialCharacterDetail(sqlContext, "sourced",
        "b_policy_product_plan_bzncen", "policy_no",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")


    val bPolicyProductPlanBzncen2 =
      MysqlPecialCharacterDetail(sqlContext, "sourced",
        "b_policy_product_plan_bzncen", "plan_name",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")


    val bPolicyProductPlanBzncen3 =
      MysqlPecialCharacterDetail(sqlContext, "sourced",
        "b_policy_product_plan_bzncen", "plan_code",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")


    val bPolicySubjectPersonSlaveBzncen1 =
      MysqlPecialCharacterDetail(sqlContext, "sourced",
        "b_policy_subject_person_slave_bzncen", "policy_no",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")

    val bPolicySubjectPersonSlaveBzncen2 =
      MysqlPecialCharacterDetail(sqlContext, "sourced",
        "b_policy_subject_person_slave_bzncen", "cert_no",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")

    val bPolicySubjectPersonSlaveBzncen3 =
      MysqlPecialCharacterDetail(sqlContext, "sourced",
        "b_policy_subject_person_slave_bzncen", "name",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")

    val bPolicySubjectPersonSlaveBzncen4 =
      MysqlPecialCharacterDetail(sqlContext, "sourced",
        "b_policy_subject_person_slave_bzncen", "tel",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")


    val bsChannelBznmana1 =
      MysqlPecialCharacterDetail(sqlContext, "sourced",
        "bs_channel_bznmana", "name",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")


    val bsChannelBznmana2 =
      MysqlPecialCharacterDetail(sqlContext, "sourced",
        "bs_channel_bznmana", "contact_name",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")


    val bsChannelBznmana3 =
      MysqlPecialCharacterDetail(sqlContext, "sourced",
        "bs_channel_bznmana", "contact_tel",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")


    val bsChannelBznmana4 =
      MysqlPecialCharacterDetail(sqlContext, "sourced",
        "bs_channel_bznmana", "channel_id",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")


    val tProposalBznbusi1 =
      MysqlPecialCharacterDetail(sqlContext, "sourced",
        "t_proposal_bznbusi", "insurance_proposal_no",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")


    val tProposalBznbusi2 =
      MysqlPecialCharacterDetail(sqlContext, "sourced",
        "t_proposal_bznbusi", "policy_no",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")


    val tProposalBznbusi3 =
      MysqlPecialCharacterDetail(sqlContext, "sourced",
        "t_proposal_bznbusi", "sum_premium",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")


    val tProposalBznbusi4 =
      MysqlPecialCharacterDetail(sqlContext, "sourced",
        "t_proposal_bznbusi", "sell_channel_name",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")


    val tProposalHolderCompanyBznbusi1 =
      MysqlPecialCharacterDetail(sqlContext, "sourced",
        "t_proposal_holder_company_bznbusi", "contact_name",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")


    val tProposalHolderCompanyBznbusi2 =
      MysqlPecialCharacterDetail(sqlContext, "sourced",
        "t_proposal_holder_company_bznbusi", "contact_tel",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")


    val tProposalHolderCompanyBznbusi3 =
      MysqlPecialCharacterDetail(sqlContext, "sourced",
        "t_proposal_holder_company_bznbusi", "proposal_no",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")


    val res1 = bPolicyOne.unionAll(openEmployerPolicyBznopen1).unionAll(openEmployerPolicyBznopen2)
      .unionAll(openEmployerPolicyBznopen3)
      .unionAll(openEmployerPolicyBznope4).unionAll(openEmployerPolicyBznope5)
      .unionAll(odrPolicyBznprd1).unionAll(odrPolicyHolderBznprd1).unionAll(odrPolicyHolderBznprd3)
      .unionAll(odrPolicyHolderBznprd4).unionAll(odrPolicyHolderBznprd5).unionAll(odrPolicyInsurantBznprd1)
      .unionAll(odrPolicyInsurantBznprd2).unionAll(odrPolicyInsurantBznprd3).unionAll(odrPolicyInsurantBznprd4)
      .unionAll(odrPolicyInsurantBznprd5).unionAll(odrPolicyInsurantBznprd6)
      .unionAll(pdtProductBznprd1).unionAll(pdtProductBznprd2)
      .unionAll(plcPolicyPreserveBznprd1).unionAll(plcPolicyPreserveBznprd2)
      .unionAll(plcPolicyPreserveBznprd3).unionAll(plcPolicyPreserveBznprd4)
      .unionAll(bPolicyBzncen1).unionAll(bPolicyBzncen2).unionAll(bPolicyBzncen3)
      .unionAll(bPolicyBzncen4).unionAll(bPolicyBzncen5)
      .unionAll(bPolicyHolderPersonBzncen1).unionAll(bPolicyHolderPersonBzncen2)
      .unionAll(bPolicyHolderPersonBzncen3)
      .unionAll(bPolicyProductPlanBzncen1).unionAll(bPolicyProductPlanBzncen2)
      .unionAll(bPolicyProductPlanBzncen3).unionAll(bPolicySubjectPersonSlaveBzncen1)
      .unionAll(bPolicySubjectPersonSlaveBzncen2).unionAll(bPolicySubjectPersonSlaveBzncen3)
      .unionAll(bPolicySubjectPersonSlaveBzncen4).unionAll(bsChannelBznmana1)
      .unionAll(bsChannelBznmana2).unionAll(bsChannelBznmana3).unionAll(bsChannelBznmana4)
      .unionAll(tProposalBznbusi1).unionAll(tProposalBznbusi2).unionAll(tProposalBznbusi3)
      .unionAll(tProposalBznbusi4).unionAll(tProposalHolderCompanyBznbusi1)
      .unionAll(tProposalHolderCompanyBznbusi2).unionAll(tProposalHolderCompanyBznbusi3)

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


    val res2 = businessLine.unionAll(OnePolicyStatus).unionAll(twoPolicyStatus).unionAll(twoPreserveStatus).unionAll(onePreserveStatus)

    val res = res1.unionAll(res2)
    saveASMysqlTable(res, "dm_warning_interdict_monitoring_detail", SaveMode.Overwrite,
      "mysql.username.103",
      "mysql.password.103",
      "mysql.driver",
      "mysql.url.103.dmdb")

  }


  //错误字段明细
  def MysqlPecialCharacterDetail(SQLContext: SQLContext, houseName: String, tableName: String, fieldType: String, user: String, pass: String, driver: String, url: String): DataFrame = {
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

    val resTemp: DataFrame = readMysqlTable(SQLContext, table, username, password, drivers, urls)
      .selectExpr(s"cast($field as string) as field").distinct
      .map(x => {
        val fieldInfo = x.getAs[String]("field")
        //匹配空格
        val Space: Boolean = SpaceMatching(fieldInfo)
        //匹配换行
        val Linefeed = LinefeedMatching(fieldInfo)
        val str = if (Space == true) {
          house + "\u0001" + table + "\u0001" + s"$field" + "\u0001" + fieldInfo + "\u0001" + "有空格" + "\u0001" + 2
        } else if (Linefeed == true) {
          house + "\u0001" + table + "\u0001" + s"$field" + "\u0001" + fieldInfo + "\u0001" + "有换行" + "\u0001" + 1
        } else {
          house + "\u0001" + table + "\u0001" + s"$field" + "\u0001" + fieldInfo + "\u0001" + "正确" + "\u0001" + 0
        }
        val strings = str.split("\u0001")
        (strings(0), strings(1), strings(2), strings(3), strings(4), strings(5))
      }).toDF("monitoring_house", "monitoring_table", "monitoring_field", "monitoring_text", "rule_desc", "monitoring_level")
    val resTable = resTemp.
      selectExpr(
        "monitoring_house",
        "monitoring_table",
        "monitoring_field",
        "monitoring_text",
        "monitoring_level",
        "rule_desc",
        "getNow() as create_time",
        "getNow() as update_time")
    resTable.registerTempTable("SpecialCharacterMonitoring")
    val res = SQLContext.sql("select * from SpecialCharacterMonitoring where monitoring_level=1 or monitoring_level=2")
    res
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
    val house = houseName
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

          house + "\u0001" + table + "\u0001" + s"$field" + "\u0001" + value + "\u0001" + "正确" + "\u0001" + 0

        } else {

          house + "\u0001" + table + "\u0001" + s"$field" + "\u0001" + value + "\u0001" + "新增类型" + "\u0001" + 2

        }

        val split = payStatusRes.split("\u0001")
        //监控字段+当前值+正确值+预警+预警描述+监控来源
        (split(0), split(1), split(2), split(3), split(4), split(5))
      }).toDF("monitoring_house", "monitoring_table", "monitoring_field", "monitoring_text", "rule_desc", "monitoring_level")
    val resTable = resTemp.
      selectExpr(
        "monitoring_house",
        "monitoring_table",
        "monitoring_field",
        "monitoring_text",
        "monitoring_level",
        "rule_desc",
        "getNow() as create_time",
        "getNow() as update_time")
    resTable.registerTempTable("EnumerationTypeMonitoring")
    val res = SQLContext.sql("select * from EnumerationTypeMonitoring where monitoring_level=1 or monitoring_level=2")
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

          house + "\u0001" + table + "\u0001" + s"$field" + "\u0001" + value + "\u0001" + "正确" + "\u0001" + 0

        } else {

          house + "\u0001" + table + "\u0001" + s"$field" + "\u0001" + value + "\u0001" + "新增类型" + "\u0001" + 2

        }

        val split = payStatusRes.split("\u0001")
        //监控字段+当前值+正确值+预警+预警描述+监控来源
        (split(0), split(1), split(2), split(3), split(4), split(5))
      }).toDF("monitoring_house", "monitoring_table", "monitoring_field", "monitoring_text", "rule_desc", "monitoring_level")
    val resTable = resTemp.
      selectExpr(
        "monitoring_house",
        "monitoring_table",
        "monitoring_field",
        "monitoring_text",
        "monitoring_level",
        "rule_desc",
        "getNow() as create_time",
        "getNow() as update_time")
    resTable.registerTempTable("EnumerationTypeMonitoring")
    val res = SQLContext.sql("select * from EnumerationTypeMonitoring where monitoring_level=1 or monitoring_level=2")
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

          house + "\u0001" + table + "\u0001" + s"$field" + "\u0001" + value + "\u0001" + "正确" + "\u0001" + 0

        } else {

          house + "\u0001" + table + "\u0001" + s"$field" + "\u0001" + value + "\u0001" + "新增类型" + "\u0001" + 2

        }

        val split = payStatusRes.split("\u0001")
        //监控字段+当前值+正确值+预警+预警描述+监控来源
        (split(0), split(1), split(2), split(3), split(4), split(5))
      }).toDF("monitoring_house", "monitoring_table", "monitoring_field", "monitoring_text", "rule_desc", "monitoring_level")
    val resTable = resTemp.
      selectExpr(
        "monitoring_house",
        "monitoring_table",
        "monitoring_field",
        "monitoring_text",
        "monitoring_level",
        "rule_desc",
        "getNow() as create_time",
        "getNow() as update_time")
    resTable.registerTempTable("EnumerationTypeMonitoring")
    val res = SQLContext.sql("select * from EnumerationTypeMonitoring where monitoring_level=1 or monitoring_level=2")
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

          house + "\u0001" + table + "\u0001" + s"$field" + "\u0001" + value + "\u0001" + "正确" + "\u0001" + 0

        } else {

          house + "\u0001" + table + "\u0001" + s"$field" + "\u0001" + value + "\u0001" + "新增类型" + "\u0001" + 2

        }

        val split = payStatusRes.split("\u0001")
        //监控字段+当前值+正确值+预警+预警描述+监控来源
        (split(0), split(1), split(2), split(3), split(4), split(5))
      }).toDF("monitoring_house", "monitoring_table", "monitoring_field", "monitoring_text", "rule_desc", "monitoring_level")
    val resTable = resTemp.
      selectExpr(
        "monitoring_house",
        "monitoring_table",
        "monitoring_field",
        "monitoring_text",
        "monitoring_level",
        "rule_desc",
        "getNow() as create_time",
        "getNow() as update_time")
    resTable.registerTempTable("EnumerationTypeMonitoring")
    val res = SQLContext.sql("select * from EnumerationTypeMonitoring where monitoring_level=1 or monitoring_level=2")
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

          house + "\u0001" + table + "\u0001" + s"$field" + "\u0001" + value + "\u0001" + "正确" + "\u0001" + 0

        } else {

          house + "\u0001" + table + "\u0001" + s"$field" + "\u0001" + value + "\u0001" + "新增类型" + "\u0001" + 2

        }

        val split = payStatusRes.split("\u0001")
        //监控字段+当前值+正确值+预警+预警描述+监控来源
        (split(0), split(1), split(2), split(3), split(4), split(5))
      }).toDF("monitoring_house", "monitoring_table", "monitoring_field", "monitoring_text", "rule_desc", "monitoring_level")
    val resTable = resTemp.
      selectExpr(
        "monitoring_house",
        "monitoring_table",
        "monitoring_field",
        "monitoring_text",
        "monitoring_level",
        "rule_desc",
        "getNow() as create_time",
        "getNow() as update_time")
    resTable.registerTempTable("EnumerationTypeMonitoring")
    val res = SQLContext.sql("select * from EnumerationTypeMonitoring where monitoring_level=1 or monitoring_level=2")
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

          house + "\u0001" + table + "\u0001" + s"$field" + "\u0001" + value + "\u0001" + "正确" + "\u0001" + 0

        } else {

          house + "\u0001" + table + "\u0001" + s"$field" + "\u0001" + value + "\u0001" + "新增类型" + "\u0001" + 2

        }

        val split = payStatusRes.split("\u0001")
        //监控字段+当前值+正确值+预警+预警描述+监控来源
        (split(0), split(1), split(2), split(3), split(4), split(5))
      }).toDF("monitoring_house", "monitoring_table", "monitoring_field", "monitoring_text", "rule_desc", "monitoring_level")
    val resTable = resTemp.
      selectExpr(
        "monitoring_house",
        "monitoring_table",
        "monitoring_field",
        "monitoring_text",
        "monitoring_level",
        "rule_desc",
        "getNow() as create_time",
        "getNow() as update_time")
    resTable.registerTempTable("EnumerationTypeMonitoring")
    val res = SQLContext.sql("select * from EnumerationTypeMonitoring where monitoring_level=1 or monitoring_level=2")
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

          house + "\u0001" + table + "\u0001" + s"$field" + "\u0001" + value + "\u0001" + "正确" + "\u0001" + 0

        } else {

          house + "\u0001" + table + "\u0001" + s"$field" + "\u0001" + value + "\u0001" + "新增类型" + "\u0001" + 2

        }

        val split = payStatusRes.split("\u0001")
        //监控字段+当前值+正确值+预警+预警描述+监控来源
        (split(0), split(1), split(2), split(3), split(4), split(5))
      }).toDF("monitoring_house", "monitoring_table", "monitoring_field", "monitoring_text", "rule_desc", "monitoring_level")
    val resTable = resTemp.
      selectExpr(
        "monitoring_house",
        "monitoring_table",
        "monitoring_field",
        "monitoring_text",
        "monitoring_level",
        "rule_desc",
        "getNow() as create_time",
        "getNow() as update_time")
    resTable.registerTempTable("EnumerationTypeMonitoring")
    val res = SQLContext.sql("select * from EnumerationTypeMonitoring where monitoring_level=1 or monitoring_level=2")
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