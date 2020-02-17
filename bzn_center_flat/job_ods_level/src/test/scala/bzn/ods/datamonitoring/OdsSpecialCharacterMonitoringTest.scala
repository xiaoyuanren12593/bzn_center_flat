package bzn.ods.datamonitoring

import java.text.SimpleDateFormat
import java.util.Date

import bzn.ods.datamonitoring.OdsRateRuleMonitoringTest.saveASMysqlTable
import bzn.job.common.{MysqlUntil, Until}
import bzn.other.OdsSpecialCharacterMonitoringTest.{LinefeedMatching, SpaceMatching, readMysqlTable, sparkConfInfo}
import bzn.util.SparkUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext

object OdsSpecialCharacterMonitoringTest extends SparkUtil with Until with MysqlUntil {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "local[*]")

    val sc = sparkConf._2
    val sqlContext = sparkConf._3
    val hiveContext = sparkConf._4

    // MysqlPecialCharacter(sqlContext, "b_policy", "policy_no", "mysql.username.103", "mysql.password.103", "mysql.driver", "mysql_url.103.odsdb")
    //HivePecialCharacter(hiveContext,"odsdb.ods_policy_detail","policy_code")
    //MysqlRateRules(sqlContext, "b_policy_product_plan", "brokerage_percent", "mysql.username.103", "mysql.password.103", "mysql.driver", "mysql_url.103.odsdb")
    //HiveRateRules(hiveContext, "odsdb.ods_policy_product_plan_detail", "commission_rate")
    //source保单


    //监控1.0保单特殊字符
    val bPolicyOne =
      MysqlPecialCharacter(sqlContext, "sourced",
        "b_policy_bzncen", "insurance_policy_no",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")


    val openEmployerPolicyBznopen1 =
      MysqlPecialCharacter(sqlContext, "sourced",
        "open_employer_policy_bznopen", "policy_no",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")

    val openEmployerPolicyBznopen2 =
      MysqlPecialCharacter(sqlContext, "sourced",
        "open_employer_policy_bznopen", "holder_name",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")

    val openEmployerPolicyBznopen3 =
      MysqlPecialCharacter(sqlContext, "sourced",
        "open_employer_policy_bznopen", "holder_cert_no",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")

    val openEmployerPolicyBznope4 =
      MysqlPecialCharacter(sqlContext, "sourced",
        "open_employer_policy_bznopen", "insured_name",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")

    val openEmployerPolicyBznope5 =
      MysqlPecialCharacter(sqlContext, "sourced",
        "open_employer_policy_bznopen", "insured_contact_phone",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")

    val odrPolicyBznprd1 =
      MysqlPecialCharacter(sqlContext, "sourced",
        "odr_policy_bznprd", "policy_code",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")

    val odrPolicyHolderBznprd1 =
      MysqlPecialCharacter(sqlContext, "sourced",
        "odr_policy_holder_bznprd", "policy_code",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")

    val odrPolicyHolderBznprd2 =
      MysqlPecialCharacter(sqlContext, "sourced",
        "odr_policy_holder_bznprd", "name",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")

    val odrPolicyHolderBznprd3 =
      MysqlPecialCharacter(sqlContext, "sourced",
        "odr_policy_holder_bznprd", "cert_no",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")

    val odrPolicyHolderBznprd4 =
      MysqlPecialCharacter(sqlContext, "sourced",
        "odr_policy_holder_bznprd", "mobile",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")


    val odrPolicyHolderBznprd5 =
      MysqlPecialCharacter(sqlContext, "sourced",
        "odr_policy_holder_bznprd", "policy_id",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")


    val odrPolicyInsurantBznprd1 =
      MysqlPecialCharacter(sqlContext, "sourced",
        "odr_policy_insurant_bznprd", "policy_code",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")


    val odrPolicyInsurantBznprd2 =
      MysqlPecialCharacter(sqlContext, "sourced",
        "odr_policy_insurant_bznprd", "policy_id",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")

    val odrPolicyInsurantBznprd3 =
      MysqlPecialCharacter(sqlContext, "sourced",
        "odr_policy_insurant_bznprd", "name",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")


    val odrPolicyInsurantBznprd4 =
      MysqlPecialCharacter(sqlContext, "sourced",
        "odr_policy_insurant_bznprd", "cert_no",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")


    val odrPolicyInsurantBznprd5 =
      MysqlPecialCharacter(sqlContext, "sourced",
        "odr_policy_insurant_bznprd", "work_type",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")


    val odrPolicyInsurantBznprd6 =
      MysqlPecialCharacter(sqlContext, "sourced",
        "odr_policy_insurant_bznprd", "mobile",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")


    val pdtProductBznprd1 =
      MysqlPecialCharacter(sqlContext, "sourced",
        "pdt_product_bznprd", "code",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")


    val pdtProductBznprd2 =
      MysqlPecialCharacter(sqlContext, "sourced",
        "pdt_product_bznprd", "name",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")


    val plcPolicyPreserveBznprd1 =
      MysqlPecialCharacter(sqlContext, "sourced",
        "plc_policy_preserve_bznprd", "policy_id",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")


    val plcPolicyPreserveBznprd2 =
      MysqlPecialCharacter(sqlContext, "sourced",
        "plc_policy_preserve_bznprd", "policy_code",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")


    val plcPolicyPreserveBznprd3 =
      MysqlPecialCharacter(sqlContext, "sourced",
        "plc_policy_preserve_bznprd", "add_batch_code",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")


    val plcPolicyPreserveBznprd4 =
      MysqlPecialCharacter(sqlContext, "sourced",
        "plc_policy_preserve_bznprd", "del_batch_code",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")


    val bPolicyBzncen1 =
      MysqlPecialCharacter(sqlContext, "sourced",
        "b_policy_bzncen", "policy_no",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")

    val bPolicyBzncen5 =
      MysqlPecialCharacter(sqlContext, "sourced",
        "b_policy_bzncen", "insurance_policy_no",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")

    val bPolicyBzncen2 =
      MysqlPecialCharacter(sqlContext, "sourced",
        "b_policy_bzncen", "sum_premium",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")

    val bPolicyBzncen3 =
      MysqlPecialCharacter(sqlContext, "sourced",
        "b_policy_bzncen", "operation_user_name",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")

    val bPolicyBzncen4 =
      MysqlPecialCharacter(sqlContext, "sourced",
        "b_policy_bzncen", "sell_channel_name",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")


    val bPolicyHolderPersonBzncen1 =
      MysqlPecialCharacter(sqlContext, "sourced",
        "b_policy_holder_person_bzncen", "policy_no",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")

    val bPolicyHolderPersonBzncen2 =
      MysqlPecialCharacter(sqlContext, "sourced",
        "b_policy_holder_person_bzncen", "name",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")

    val bPolicyHolderPersonBzncen3 =
      MysqlPecialCharacter(sqlContext, "sourced",
        "b_policy_holder_person_bzncen", "cert_no",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")


    val bPolicyProductPlanBzncen1 =
      MysqlPecialCharacter(sqlContext, "sourced",
        "b_policy_product_plan_bzncen", "policy_no",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")


    val bPolicyProductPlanBzncen2 =
      MysqlPecialCharacter(sqlContext, "sourced",
        "b_policy_product_plan_bzncen", "plan_name",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")


    val bPolicyProductPlanBzncen3 =
      MysqlPecialCharacter(sqlContext, "sourced",
        "b_policy_product_plan_bzncen", "plan_code",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")


    val bPolicySubjectPersonSlaveBzncen1 =
      MysqlPecialCharacter(sqlContext, "sourced",
        "b_policy_subject_person_slave_bzncen", "policy_no",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")

    val bPolicySubjectPersonSlaveBzncen2 =
      MysqlPecialCharacter(sqlContext, "sourced",
        "b_policy_subject_person_slave_bzncen", "cert_no",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")

    val bPolicySubjectPersonSlaveBzncen3 =
      MysqlPecialCharacter(sqlContext, "sourced",
        "b_policy_subject_person_slave_bzncen", "name",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")

    val bPolicySubjectPersonSlaveBzncen4 =
      MysqlPecialCharacter(sqlContext, "sourced",
        "b_policy_subject_person_slave_bzncen", "tel",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")


    val bsChannelBznmana1 =
      MysqlPecialCharacter(sqlContext, "sourced",
        "bs_channel_bznmana", "name",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")


    val bsChannelBznmana2 =
      MysqlPecialCharacter(sqlContext, "sourced",
        "bs_channel_bznmana", "contact_name",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")


    val bsChannelBznmana3 =
      MysqlPecialCharacter(sqlContext, "sourced",
        "bs_channel_bznmana", "contact_tel",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")


    val bsChannelBznmana4 =
      MysqlPecialCharacter(sqlContext, "sourced",
        "bs_channel_bznmana", "channel_id",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")


    val tProposalBznbusi1 =
      MysqlPecialCharacter(sqlContext, "sourced",
        "t_proposal_bznbusi", "insurance_proposal_no",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")


    val tProposalBznbusi2 =
      MysqlPecialCharacter(sqlContext, "sourced",
        "t_proposal_bznbusi", "policy_no",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")


    val tProposalBznbusi3 =
      MysqlPecialCharacter(sqlContext, "sourced",
        "t_proposal_bznbusi", "sum_premium",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")


    val tProposalBznbusi4 =
      MysqlPecialCharacter(sqlContext, "sourced",
        "t_proposal_bznbusi", "sell_channel_name",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")


    val tProposalHolderCompanyBznbusi1 =
      MysqlPecialCharacter(sqlContext, "sourced",
        "t_proposal_holder_company_bznbusi", "contact_name",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")


    val tProposalHolderCompanyBznbusi2 =
      MysqlPecialCharacter(sqlContext, "sourced",
        "t_proposal_holder_company_bznbusi", "contact_tel",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")


    val tProposalHolderCompanyBznbusi3 =
      MysqlPecialCharacter(sqlContext, "sourced",
        "t_proposal_holder_company_bznbusi", "proposal_no",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")


    val resTemp1 = bPolicyOne.unionAll(openEmployerPolicyBznopen1).unionAll(openEmployerPolicyBznopen2)
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



    //






    /* val bPolicyTwo =
   MysqlPecialCharacterDetail(sqlContext, "sourced",
     "odr_policy_bznprd", "policy_code",
     "mysql.username.106", "mysql.password.106",
     "mysql.driver", "mysql.url.106")*/


    //写入错误明细数据
    /*saveASMysqlTable(bPolicyTwo, "dm_warning_interdict_monitoring_detail", SaveMode.Overwrite,
      "mysql.username.103",
      "mysql.password.103",
      "mysql.driver",
      "mysql.url.103.dmdb")*/


    //监控1.0保单特殊字符
    /* val policyOne = MysqlPecialCharacter(sqlContext, "odr_policy_bznprd", "policy_code", "mysql.username.106", "mysql.password.106", "mysql.driver", "mysql.url.106")
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
     res.printSchema()*/
  }


  //mysql表字符串匹配特殊字符
  def MysqlPecialCharacter(SQLContext: SQLContext, houseName: String, tableName: String, fieldType: String, user: String, pass: String, driver: String, url: String): DataFrame = {
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
          house + "\u0001" + table + "\u0001" + s"$field" + "\u0001" + fieldInfo + "\u0001" + 2
        } else if (Linefeed == true) {
          house + "\u0001" + table + "\u0001" + s"$field" + "\u0001" + fieldInfo + "\u0001" + 1
        } else {
          house + "\u0001" + table + "\u0001" + s"$field" + "\u0001" + fieldInfo + "\u0001" + 0
        }
        val strings = str.split("\u0001")
        (strings(0), strings(1), strings(2), strings(3), strings(4))
      }).toDF("monitoring_house", "monitoring_table", "monitoring_field", "monitoring_text", "special_character_monitoring")
    val resTable = resTemp.
      selectExpr(
        "monitoring_house",
        "monitoring_table",
        "monitoring_field",
        "monitoring_text",
        "special_character_monitoring",
        "getNow() as create_time",
        "getNow() as update_time")
    resTable.registerTempTable("SpecialCharacterMonitoring")
    val res = SQLContext.sql("select monitoring_house,monitoring_table,monitoring_field,special_character_monitoring,count(1) as level_counts from SpecialCharacterMonitoring group by monitoring_house,monitoring_table,monitoring_field,special_character_monitoring")
    res
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
