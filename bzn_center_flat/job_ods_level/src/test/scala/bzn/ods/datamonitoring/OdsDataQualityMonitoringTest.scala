package bzn.ods.datamonitoring

import java.text.SimpleDateFormat
import java.util.Date

import bzn.job.common.{MysqlUntil, Until}
import bzn.ods.datamonitoring.OdsDataQualityMonitoringDetail.{LinefeedMatching, SpaceMatching}
import bzn.ods.datamonitoring.OdsEnumerationTypeMonitoringTest.{readMysqlTable, saveASMysqlTable, sparkConfInfo}
import bzn.ods.datamonitoring.OdsRateRuleMonitoringTest.{MysqlRateRules, readMysqlTable}
import bzn.ods.datamonitoring.OdsSpecialCharacterMonitoringTest.{LinefeedMatching, MysqlPecialCharacter, SpaceMatching, readMysqlTable}
import bzn.util.SparkUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext

object OdsDataQualityMonitoringTest extends SparkUtil with Until with MysqlUntil {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "local[*]")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    val sqlContext = sparkConf._3


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

    //支付状态监控
    val bPlicyPreserVationPayStatus = MysqlPayStatusMonitoring(sqlContext, "sourcedb",
      "b_policy_preservation_bzncen",
      "pay_status", "mysql.username.106",
      "mysql.password.106", "mysql.driver",
      "mysql.url.106")

    //方案类别
    /*   val bPolicyBzncenPayMent = MysqlPaymentTypeMonitorings(sqlContext,
         "sourcedb", "b_policy_bzncen",
         "payment_type", "mysql.username.106",
         "mysql.password.106", "mysql.driver",
         "mysql.url.106")
   */
    //团单个单
    val odrPolicyBznprdPolicyType = MysqlPolicyTypeMonitorings(sqlContext, "sourcedb", "odr_policy_bznprd",
      "policy_type", "mysql.username.106",
      "mysql.password.106", "mysql.driver",
      "mysql.url.106")


    val resTemp2 = OnePolicyStatus
      .unionAll(twoPolicyStatus)
      .unionAll(twoPreserveStatus)
      .unionAll(onePreserveStatus)
      .unionAll(bPlicyPreserVationPayStatus)
      .unionAll(odrPolicyBznprdPolicyType)


    //特殊字符监控
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


    //2.0被保人表
    val bPolicySubjectPersonMasterBzncen1 = HivePecialCharacter(hiveContext, "sourced",
      "sourcedb.b_policy_subject_person_master_bzncen", "policy_no")

    val bPolicySubjectPersonMasterBzncen2 = HivePecialCharacter(hiveContext, "sourced",
      "sourcedb.b_policy_subject_person_master_bzncen", "name")

    val bPolicySubjectPersonMasterBzncen3 = HivePecialCharacter(hiveContext, "sourced",
      "sourcedb.b_policy_subject_person_master_bzncen", "cert_no")

    val bPolicySubjectPersonMasterBzncen4 = HivePecialCharacter(hiveContext, "sourced",
      "sourcedb.b_policy_subject_person_master_bzncen", "tel")
    //1.0被保人表

    val odrPolicyInsuredBznprd1 = HivePecialCharacter(hiveContext, "sourced",
      "sourcedb.odr_policy_insured_bznprd", "policy_code")


    val odrPolicyInsuredBznprd2 = HivePecialCharacter(hiveContext, "sourced",
      "sourcedb.odr_policy_insured_bznprd", "name")


    val odrPolicyInsuredBznprd3 = HivePecialCharacter(hiveContext, "sourced",
      "sourcedb.odr_policy_insured_bznprd", "mobile")

    val odrPolicyInsuredBznprd4 = HivePecialCharacter(hiveContext, "sourced",
      "sourcedb.odr_policy_insured_bznprd", "cert_no")

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
      .unionAll(bPolicySubjectPersonMasterBzncen1).unionAll(bPolicySubjectPersonMasterBzncen2)
      .unionAll(bPolicySubjectPersonMasterBzncen3).unionAll(bPolicySubjectPersonMasterBzncen4)
      .unionAll(odrPolicyInsuredBznprd1).unionAll(odrPolicyInsuredBznprd2).unionAll(odrPolicyInsuredBznprd3)
      .unionAll(odrPolicyInsuredBznprd4)

    //费率监控
    //经纪费
    val bPolicyProductPlanBzncenRate1 = MysqlRateRules(sqlContext, "sourced", "b_policy_product_plan_bzncen",
      "brokerage_fee", "mysql.username.106",
      "mysql.password.106", "mysql.driver",
      "mysql.url.106")

    //技术服务费
    val bPolicyProductPlanBzncenRate2 = MysqlRateRules(sqlContext, "sourced", "b_policy_product_plan_bzncen",
      "technology_fee", "mysql.username.106",
      "mysql.password.106", "mysql.driver",
      "mysql.url.106")

    //返佣费
    val bPolicyProductPlanBzncenRate3 = MysqlRateRules(sqlContext, "sourced", "b_policy_product_plan_bzncen",
      "brokerage_percent", "mysql.username.106",
      "mysql.password.106", "mysql.driver",
      "mysql.url.106")
    val resTemp3 = bPolicyProductPlanBzncenRate1.unionAll(bPolicyProductPlanBzncenRate2).unionAll(bPolicyProductPlanBzncenRate3)

    val resTable = resTemp1.unionAll(resTemp2).unionAll(resTemp3)
    resTable.registerTempTable("RateRangeMonitoring")
    val res = sqlContext.sql("select monitoring_house,monitoring_table,monitoring_field,monitoring_level,monitoring_desc,count(1) as level_counts from RateRangeMonitoring group by monitoring_house,monitoring_table,monitoring_field,monitoring_level,monitoring_desc")
res.printSchema()


   /* //写入103
    saveASMysqlTable(res, "dm_data_quality_monitoring_detail", SaveMode.Overwrite,
      "mysql.username.103",
      "mysql.password.103",
      "mysql.driver",
      "mysql.url.103.dmdb")
    //写入106

    //106存储
    saveASMysqlTable(res, "dm_data_quality_monitoring_detail", SaveMode.Overwrite,
      "mysql.username.106",
      "mysql.password.106",
      "mysql.driver",
      "mysql.url.106.dmdb")*/


  }

  //Mysql方案类别监控
  def MysqlPaymentTypeMonitorings(SQLContext: SQLContext, houseName: String, tableName: String, fieldType: String, user: String, pass: String, driver: String, url: String): DataFrame = {
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
    val bussinessLineList = List(1, 2, 4, null)
    val resTemp: DataFrame = readMysqlTable(SQLContext, table, username, password, drivers, urls)
      .selectExpr(s"cast($field as Int) as field")
      .map(x => {
        val value = x.getAs[Int]("field")
        val payStatusRes = if (bussinessLineList.contains(value)) {

          name + "\u0001" + table + "\u0001" + s"$field " + "\u0001" + 0 + "\u0001" + 1

        } else {
          name + "\u0001" + table + "\u0001" + s"$field " + "\u0001" + 2 + "\u0001" + 1
        }

        val split = payStatusRes.split("\u0001")

        (split(0), split(1), split(2), split(3), split(4))
      }).toDF("monitoring_house", "monitoring_table", "monitoring_field", "monitoring_level", "monitoring_desc")
    val resTable = resTemp
      .selectExpr("monitoring_house",
        "monitoring_table",
        "monitoring_field",
        "monitoring_level",
        "monitoring_desc")
    resTable

  }


  //团单个单监控
  //Mysql方案类别监控
  def MysqlPolicyTypeMonitorings(SQLContext: SQLContext, houseName: String, tableName: String, fieldType: String, user: String, pass: String, driver: String, url: String): DataFrame = {
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
    val bussinessLineList = List(0, 1, 2, null)
    val resTemp: DataFrame = readMysqlTable(SQLContext, table, username, password, drivers, urls)
      .selectExpr(s"cast($field as Int) as field")
      .map(x => {
        val value = x.getAs[Int]("field")
        val payStatusRes = if (bussinessLineList.contains(value)) {

          name + "\u0001" + table + "\u0001" + s"$field " + "\u0001" + 0 + "\u0001" + 1

        } else {
          name + "\u0001" + table + "\u0001" + s"$field " + "\u0001" + 2 + "\u0001" + 1
        }

        val split = payStatusRes.split("\u0001")

        (split(0), split(1), split(2), split(3), split(4))
      }).toDF("monitoring_house", "monitoring_table", "monitoring_field", "monitoring_level", "monitoring_desc")
    val resTable = resTemp
      .selectExpr("monitoring_house",
        "monitoring_table",
        "monitoring_field",
        "monitoring_level",
        "monitoring_desc")
    resTable
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

          house + "\u0001" + table + "\u0001" + s"$field " + "\u0001" + 0 + "\u0001" + 1

        } else {
          house + "\u0001" + table + "\u0001" + s"$field " + "\u0001" + 2 + "\u0001" + 1
        }

        val split = payStatusRes.split("\u0001")

        (split(0), split(1), split(2), split(3), split(4))
      }).toDF("monitoring_house", "monitoring_table", "monitoring_field", "monitoring_level", "monitoring_desc")
    val resTable = resTemp
      .selectExpr("monitoring_house",
        "monitoring_table",
        "monitoring_field",
        "monitoring_level",
        "monitoring_desc")
    resTable

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
    val PolicyStatusList = List(null, 0, 1, 3, 5, 6, 7, 8, 9, 10)
    val resTemp: DataFrame = readMysqlTable(SQLContext, table, username, password, drivers, urls)
      .selectExpr(s"cast($field as Int) as field")
      .map(x => {
        val value = x.getAs[Int]("field")
        val payStatusRes = if (PolicyStatusList.contains(value)) {

          house + "\u0001" + table + "\u0001" + s"$field " + "\u0001" + 0 + "\u0001" + 1

        } else {
          house + "\u0001" + table + "\u0001" + s"$field " + "\u0001" + 2 + "\u0001" + 1
        }

        val split = payStatusRes.split("\u0001")

        (split(0), split(1), split(2), split(3), split(4))
      }).toDF("monitoring_house", "monitoring_table", "monitoring_field", "monitoring_level", "monitoring_desc")
    val resTable = resTemp
      .selectExpr("monitoring_house",
        "monitoring_table",
        "monitoring_field",
        "monitoring_level",
        "monitoring_desc")
    resTable

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
    val payStatusList = List(1, 2, 3)
    val resTemp: DataFrame = readMysqlTable(SQLContext, table, username, password, drivers, urls)
      .selectExpr(s"cast($field as Int) as field")
      .map(x => {
        val value = x.getAs[Int]("field")
        val payStatusRes = if (payStatusList.contains(value)) {

          house + "\u0001" + table + "\u0001" + s"$field " + "\u0001" + 0 + "\u0001" + 1

        } else {
          house + "\u0001" + table + "\u0001" + s"$field " + "\u0001" + 2 + "\u0001" + 1
        }

        val split = payStatusRes.split("\u0001")

        (split(0), split(1), split(2), split(3), split(4))
      }).toDF("monitoring_house", "monitoring_table", "monitoring_field", "monitoring_level", "monitoring_desc")
    val resTable = resTemp
      .selectExpr("monitoring_house",
        "monitoring_table",
        "monitoring_field",
        "monitoring_level",
        "monitoring_desc")
    resTable

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
    val PolicyStatusList = List(2, 3, 4, 5, 6)
    val resTemp: DataFrame = readMysqlTable(SQLContext, table, username, password, drivers, urls)
      .selectExpr(s"cast($field as Int) as field")
      .map(x => {
        val value = x.getAs[Int]("field")
        val payStatusRes = if (PolicyStatusList.contains(value)) {

          house + "\u0001" + table + "\u0001" + s"$field " + "\u0001" + 0 + "\u0001" + 1

        } else {
          house + "\u0001" + table + "\u0001" + s"$field " + "\u0001" + 2 + "\u0001" + 1
        }

        val split = payStatusRes.split("\u0001")

        (split(0), split(1), split(2), split(3), split(4))
      }).toDF("monitoring_house", "monitoring_table", "monitoring_field", "monitoring_level", "monitoring_desc")
    val resTable = resTemp
      .selectExpr("monitoring_house",
        "monitoring_table",
        "monitoring_field",
        "monitoring_level",
        "monitoring_desc")
    resTable

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

          house + "\u0001" + table + "\u0001" + s"$field " + "\u0001" + 0 + "\u0001" + 1

        } else {
          house + "\u0001" + table + "\u0001" + s"$field " + "\u0001" + 2 + "\u0001" + 1
        }

        val split = payStatusRes.split("\u0001")

        (split(0), split(1), split(2), split(3), split(4))
      }).toDF("monitoring_house", "monitoring_table", "monitoring_field", "monitoring_level", "monitoring_desc")
    val resTable = resTemp
      .selectExpr("monitoring_house",
        "monitoring_table",
        "monitoring_field",
        "monitoring_level",
        "monitoring_desc")
    resTable

  }

  //mysql表字符串匹配特殊字符
  def MysqlPecialCharacter(SQLContext: SQLContext, houseName: String, tableName: String,
                           fieldType: String, user: String, pass: String, driver: String, url: String): DataFrame = {
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
          house + "\u0001" + table + "\u0001" + s"$field" + "\u0001" + 2 + "\u0001" + 2
        } else if (Linefeed == true) {
          house + "\u0001" + table + "\u0001" + s"$field" + "\u0001" + 1 + "\u0001" + 2
        } else {
          house + "\u0001" + table + "\u0001" + s"$field" + "\u0001" + 0 + "\u0001" + 2
        }
        val split = str.split("\u0001")
        (split(0), split(1), split(2), split(3), split(4))
      }).toDF("monitoring_house", "monitoring_table", "monitoring_field", "monitoring_level", "monitoring_desc")
    val resTable = resTemp
      .selectExpr("monitoring_house",
        "monitoring_table",
        "monitoring_field",
        "monitoring_level",
        "monitoring_desc")
    resTable
  }


  //hive监控特殊字符字段

  def HivePecialCharacter(hiveContext: HiveContext, houseName: String, tableName: String,
                          fieldType: String): DataFrame = {
    hiveContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") //设置日期格式
      val date = df.format(new Date()) // new Date()为获取当前系统时间
      date + ""
    })
    import hiveContext.implicits._
    val table = tableName
    var field = fieldType
    val house = houseName


    val resTemp: DataFrame = hiveContext.sql(s"select cast($field as string) as field from $table")
      .map(x => {
        val fieldInfo = x.getAs[String]("field")
        //匹配空格
        val Space: Boolean = SpaceMatching(fieldInfo)
        //匹配换行
        val Linefeed = LinefeedMatching(fieldInfo)
        val str = if (Space == true) {
          house + "\u0001" + s"$table" + "\u0001" + s"$field" + "\u0001" + 2 + "\u0001" + 2
        } else if (Linefeed == true) {
          house + "\u0001" + s"$table" + "\u0001" + s"$field" + "\u0001" + 1 + "\u0001" + 2
        } else {
          house + "\u0001" + s"$table" + "\u0001" + s"$field" + "\u0001" + 0 + "\u0001" + 2
        }
        val split = str.split("\u0001")
        (split(0), split(1), split(2), split(3), split(4))
      }).toDF("monitoring_house", "monitoring_table", "monitoring_field", "monitoring_level", "monitoring_desc")
    val resTable = resTemp
      .selectExpr("monitoring_house",
        "monitoring_table",
        "monitoring_field",
        "monitoring_level",
        "monitoring_desc")
    resTable
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
        val rateStr = if (rate / 100 >= 0 && rate / 100 < 1) {
          house + "\u0001" + s"$table" + "\u0001" + s"$field" + "\u0001" + 0 + "\u0001" + 3
        } else if (rate / 100 >= 1) {
          house + "\u0001" + s"$table" + "\u0001" + s"$field" + "\u0001" + 2 + "\u0001" + 3
        } else if (rate / 100 == null) {
          house + "\u0001" + s"$table" + "\u0001" + s"$field" + "\u0001" + 0 + "\u0001" + 3
        } else {
          house + "\u0001" + s"$table" + "\u0001" + s"$field" + "\u0001" + 0 + "\u0001" + 3
        }
        val split: Array[String] = rateStr.split("\u0001")
        (split(0), split(1), split(2), split(3), split(4))
      }).toDF("monitoring_house", "monitoring_table", "monitoring_field", "monitoring_level", "monitoring_desc")
    val resTable = resTemp
      .selectExpr("monitoring_house",
        "monitoring_table",
        "monitoring_field",
        "monitoring_level",
        "monitoring_desc")
    resTable
  }



  //匹配空格
  def SpaceMatching(Temp: String): Boolean = {
    if (Temp != null) {
      "^\\s|\\s+$  ".r.findFirstIn(Temp).isDefined
    } else false
  }

  //匹配换行符
  def LinefeedMatching(Temp: String): Boolean = {
    if (Temp != null) {
      "^\\n|\\n+$".r.findFirstIn(Temp).isDefined
    } else false
  }


}