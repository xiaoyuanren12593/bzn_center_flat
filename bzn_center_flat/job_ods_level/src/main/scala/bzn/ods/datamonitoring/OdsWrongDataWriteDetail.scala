package bzn.ods.datamonitoring

import java.text.SimpleDateFormat
import java.util.Date

import bzn.job.common.{MysqlUntil, Until}
import bzn.ods.datamonitoring.OdsDataQualityMonitoringDetail.{LinefeedMatching, MysqlPecialCharacter, SpaceMatching}
import bzn.util.SparkUtil
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

object OdsWrongDataWriteDetail extends SparkUtil with Until with MysqlUntil {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "")

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


    val odrPolicyInsurantBznprd7 =
      MysqlPecialCharacterDetail(sqlContext, "sourced",
        "odr_policy_insurant_bznprd", "business_nature",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")

    val odrPolicyInsurantBznprd8 =
      MysqlPecialCharacterDetail(sqlContext, "sourced",
        "odr_policy_insurant_bznprd", "business_line",
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


    val bPolicyHolderCompanyBzncen1 =
      MysqlPecialCharacterDetail(sqlContext, "sourced",
        "b_policy_holder_company_bzncen", "industry_name",
        "mysql.username.106", "mysql.password.106",
        "mysql.driver", "mysql.url.106")


    val bPolicyHolderCompanyBzncen2 =
      MysqlPecialCharacterDetail(sqlContext, "sourced",
        "b_policy_holder_company_bzncen", "industry_code",
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
      .unionAll(bPolicySubjectPersonMasterBzncen1).unionAll(bPolicySubjectPersonMasterBzncen2)
      .unionAll(bPolicySubjectPersonMasterBzncen3).unionAll(bPolicySubjectPersonMasterBzncen4)
      .unionAll(odrPolicyInsuredBznprd1).unionAll(odrPolicyInsuredBznprd2).unionAll(odrPolicyInsuredBznprd3)
      .unionAll(odrPolicyInsuredBznprd4).unionAll(odrPolicyInsurantBznprd7).unionAll(odrPolicyInsurantBznprd8)
      .unionAll(bPolicyHolderCompanyBzncen1).unionAll(bPolicyHolderCompanyBzncen2)


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
    val bPlicyPreserVationPayStatus = MysqlPayStatusMonitoring(sqlContext, "sourcedb", "b_policy_preservation_bzncen",
      "pay_status", "mysql.username.106",
      "mysql.password.106", "mysql.driver",
      "mysql.url.106")

    //方案类别
    val bPolicyBzncenPayMent = MysqlPaymentTypeMonitorings(sqlContext, "sourcedb", "b_policy_bzncen",
      "payment_type", "mysql.username.106",
      "mysql.password.106", "mysql.driver",
      "mysql.url.106")

    //团单个单
    val odrPolicyBznprdPolicyType = MysqlPolicyTypeMonitorings(sqlContext, "sourcedb", "odr_policy_bznprd",
      "policy_type", "mysql.username.106",
      "mysql.password.106", "mysql.driver",
      "mysql.url.106")


    val res2 = OnePolicyStatus.unionAll(twoPolicyStatus)
      .unionAll(twoPreserveStatus).unionAll(onePreserveStatus).unionAll(bPlicyPreserVationPayStatus)
      .unionAll(odrPolicyBznprdPolicyType)

    //费率错误数据明细监控
    val bPolicyProductPlanBzncenDetail1 = MysqlRateRulesDetail(sqlContext, "sourced", "b_policy_product_plan_bzncen",
      "brokerage_fee", "mysql.username.106",
      "mysql.password.106", "mysql.driver",
      "mysql.url.106")

    //技术服务费
    val bPolicyProductPlanBzncenDetail2 = MysqlRateRulesDetail(sqlContext, "sourced", "b_policy_product_plan_bzncen",
      "technology_fee", "mysql.username.106",
      "mysql.password.106", "mysql.driver",
      "mysql.url.106")

    //返佣费
    val bPolicyProductPlanBzncenDetail3 = MysqlRateRulesDetail(sqlContext, "sourced", "b_policy_product_plan_bzncen",
      "commission_rate", "mysql.username.106",
      "mysql.password.106", "mysql.driver",
      "mysql.url.106")

    val res3 = bPolicyProductPlanBzncenDetail1.unionAll(bPolicyProductPlanBzncenDetail2).unionAll(bPolicyProductPlanBzncenDetail3)


    val res = res1.unionAll(res2).unionAll(res3)


    //103存储
    saveASMysqlTable(res, "dm_warning_interdict_monitoring_detail", SaveMode.Overwrite,
      "mysql.username.103",
      "mysql.password.103",
      "mysql.driver",
      "mysql.url.103.dmdb")

    //106存储
    saveASMysqlTable(res, "dm_warning_interdict_monitoring_detail", SaveMode.Overwrite,
      "mysql.username.106",
      "mysql.password.106",
      "mysql.driver",
      "mysql.url.106.dmdb")


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
      .selectExpr(s"cast($field as string) as field")
      .map(x => {
        val fieldInfo = x.getAs[String]("field")
        //匹配空格
        val Space: Boolean = SpaceMatching(fieldInfo)
        //匹配换行
        val Linefeed = LinefeedMatching(fieldInfo)
        val str = if (Space == true) {
          house + "\u0001" + table + "\u0001" + s"$field" + "\u0001" + fieldInfo + "\u0001" + "有空格" + "\u0001" + "是否含有特殊字符" + "\u0001" + 2
        } else if (Linefeed == true) {
          house + "\u0001" + table + "\u0001" + s"$field" + "\u0001" + fieldInfo + "\u0001" + "有换行" + "\u0001" + "是否含有特殊字符" + "\u0001" + 1
        } else {
          house + "\u0001" + table + "\u0001" + s"$field" + "\u0001" + fieldInfo + "\u0001" + "正确" + "\u0001" + "是否含有特殊字符" + "\u0001" + 0
        }
        val strings = str.split("\u0001")
        (strings(0), strings(1), strings(2), strings(3), strings(4), strings(5), strings(6))
      }).toDF("monitoring_house", "monitoring_table", "monitoring_field", "monitoring_text", "warn_desc", "rule_desc", "monitoring_level")
    val resTable = resTemp.
      selectExpr(
        "monitoring_house",
        "monitoring_table",
        "monitoring_field",
        "monitoring_text",
        "monitoring_level",
        "warn_desc",
        "rule_desc",
        "getNow() as create_time",
        "getNow() as update_time")

    resTable.registerTempTable("SpecialCharacterMonitorings")
    val res = SQLContext.sql("select * from SpecialCharacterMonitorings where monitoring_level=1 or monitoring_level=2")

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

          house + "\u0001" + table + "\u0001" + s"$field" + "\u0001" + value + "\u0001" + "正确" + "\u0001" + "枚举字段是否新增" + "\u0001" + 0

        } else {

          house + "\u0001" + table + "\u0001" + s"$field" + "\u0001" + value + "\u0001" + "新增类型" + "\u0001" + "枚举字段是否新增" + "\u0001" + 2

        }

        val split = payStatusRes.split("\u0001")
        //监控字段+当前值+正确值+预警+预警描述+监控来源
        (split(0), split(1), split(2), split(3), split(4), split(5), split(6))
      }).toDF("monitoring_house", "monitoring_table", "monitoring_field", "monitoring_text", "warn_desc", "rule_desc", "monitoring_level")
    val resTable = resTemp.
      selectExpr(
        "monitoring_house",
        "monitoring_table",
        "monitoring_field",
        "monitoring_text",
        "monitoring_level",
        "warn_desc", "rule_desc",
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
    val PolicyStatusList = List(null, 0, 1, 3, 5, 6, 7, 8, 9, 10)
    val resTemp: DataFrame = readMysqlTable(SQLContext, table, username, password, drivers, urls)
      .selectExpr(s"cast($field as Int) as field")
      .map(x => {
        val value = x.getAs[Int]("field")
        val payStatusRes = if (PolicyStatusList.contains(value)) {

          house + "\u0001" + table + "\u0001" + s"$field" + "\u0001" + value + "\u0001" + "正确" + "\u0001" + "枚举字段是否新增" + "\u0001" + 0

        } else {

          house + "\u0001" + table + "\u0001" + s"$field" + "\u0001" + value + "\u0001" + "新增类型" + "\u0001" + "枚举字段是否新增" + "\u0001" + 2

        }

        val split = payStatusRes.split("\u0001")
        //监控字段+当前值+正确值+预警+预警描述+监控来源
        (split(0), split(1), split(2), split(3), split(4), split(5), split(6))
      }).toDF("monitoring_house", "monitoring_table", "monitoring_field", "monitoring_text", "warn_desc", "rule_desc", "monitoring_level")
    val resTable = resTemp.
      selectExpr(
        "monitoring_house",
        "monitoring_table",
        "monitoring_field",
        "monitoring_text",
        "monitoring_level",
        "warn_desc", "rule_desc",
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
    val payStatusList = List(1, 2, 3)
    val resTemp: DataFrame = readMysqlTable(SQLContext, table, username, password, drivers, urls)
      .selectExpr(s"cast($field as Int) as field")
      .map(x => {
        val value = x.getAs[Int]("field")
        val payStatusRes = if (payStatusList.contains(value)) {
          house + "\u0001" + table + "\u0001" + s"$field" + "\u0001" + value + "\u0001" + "正确" + "\u0001" + "枚举字段是否新增" + "\u0001" + 0

        } else {

          house + "\u0001" + table + "\u0001" + s"$field" + "\u0001" + value + "\u0001" + "新增类型" + "\u0001" + "枚举字段是否新增" + "\u0001" + 2

        }

        val split = payStatusRes.split("\u0001")
        //监控字段+当前值+正确值+预警+预警描述+监控来源
        (split(0), split(1), split(2), split(3), split(4), split(5), split(6))
      }).toDF("monitoring_house", "monitoring_table", "monitoring_field", "monitoring_text", "warn_desc", "rule_desc", "monitoring_level")
    val resTable = resTemp.
      selectExpr(
        "monitoring_house",
        "monitoring_table",
        "monitoring_field",
        "monitoring_text",
        "monitoring_level",
        "warn_desc", "rule_desc",
        "getNow() as create_time",
        "getNow() as update_time")
    resTable.registerTempTable("EnumerationTypeMonitoring")
    val res = SQLContext.sql("select * from EnumerationTypeMonitoring where monitoring_level=1 or monitoring_level=2")
    res
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
    val house = houseName
    var username = user
    var password = pass
    var drivers = driver
    var urls = url
    val ProfessionsTypeList = List(1, 2, 4, null)
    val resTemp: DataFrame = readMysqlTable(SQLContext, table, username, password, drivers, urls)
      .selectExpr(s"cast($field as Int) as field")
      .map(x => {
        val value = x.getAs[Int]("field")
        val payStatusRes = if (ProfessionsTypeList.contains(value)) {
          house + "\u0001" + table + "\u0001" + s"$field" + "\u0001" + value + "\u0001" + "正确" + "\u0001" + "枚举字段是否新增" + "\u0001" + 0

        } else {

          house + "\u0001" + table + "\u0001" + s"$field" + "\u0001" + value + "\u0001" + "新增类型" + "\u0001" + "枚举字段是否新增" + "\u0001" + 2

        }

        val split = payStatusRes.split("\u0001")
        //监控字段+当前值+正确值+预警+预警描述+监控来源
        (split(0), split(1), split(2), split(3), split(4), split(5), split(6))
      }).toDF("monitoring_house", "monitoring_table", "monitoring_field", "monitoring_text", "warn_desc", "rule_desc", "monitoring_level")
    val resTable = resTemp.
      selectExpr(
        "monitoring_house",
        "monitoring_table",
        "monitoring_field",
        "monitoring_text",
        "monitoring_level",
        "warn_desc", "rule_desc",
        "getNow() as create_time",
        "getNow() as update_time")
    resTable.registerTempTable("EnumerationTypeMonitoring")
    val res = SQLContext.sql("select * from EnumerationTypeMonitoring where monitoring_level=1 or monitoring_level=2")
    res
  }

  //团单个单


  def MysqlPolicyTypeMonitorings(SQLContext: SQLContext, houseName: String, tableName: String, fieldType: String, user: String, pass: String, driver: String, url: String): DataFrame = {
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
    val ProfessionsTypeList = List(0, 1, 2, null)
    val resTemp: DataFrame = readMysqlTable(SQLContext, table, username, password, drivers, urls)
      .selectExpr(s"cast($field as Int) as field")
      .map(x => {
        val value = x.getAs[Int]("field")
        val payStatusRes = if (ProfessionsTypeList.contains(value)) {
          house + "\u0001" + table + "\u0001" + s"$field" + "\u0001" + value + "\u0001" + "正确" + "\u0001" + "枚举字段是否新增" + "\u0001" + 0

        } else {

          house + "\u0001" + table + "\u0001" + s"$field" + "\u0001" + value + "\u0001" + "新增类型" + "\u0001" + "枚举字段是否新增" + "\u0001" + 2

        }

        val split = payStatusRes.split("\u0001")
        //监控字段+当前值+正确值+预警+预警描述+监控来源
        (split(0), split(1), split(2), split(3), split(4), split(5), split(6))
      }).toDF("monitoring_house", "monitoring_table", "monitoring_field", "monitoring_text", "warn_desc", "rule_desc", "monitoring_level")
    val resTable = resTemp.
      selectExpr(
        "monitoring_house",
        "monitoring_table",
        "monitoring_field",
        "monitoring_text",
        "monitoring_level",
        "warn_desc", "rule_desc",
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
    val PolicyStatusList = List(2, 3, 4, 5, 6)
    val resTemp: DataFrame = readMysqlTable(SQLContext, table, username, password, drivers, urls)
      .selectExpr(s"cast($field as Int) as field")
      .map(x => {
        val value = x.getAs[Int]("field")
        val payStatusRes = if (PolicyStatusList.contains(value)) {

          house + "\u0001" + table + "\u0001" + s"$field" + "\u0001" + value + "\u0001" + "正确" + "\u0001" + "枚举字段是否新增" + "\u0001" + 0

        } else {

          house + "\u0001" + table + "\u0001" + s"$field" + "\u0001" + value + "\u0001" + "新增类型" + "\u0001" + "枚举字段是否新增" + "\u0001" + 2

        }

        val split = payStatusRes.split("\u0001")
        //监控字段+当前值+正确值+预警+预警描述+监控来源
        (split(0), split(1), split(2), split(3), split(4), split(5), split(6))
      }).toDF("monitoring_house", "monitoring_table", "monitoring_field", "monitoring_text", "warn_desc", "rule_desc", "monitoring_level")
    val resTable = resTemp.
      selectExpr(
        "monitoring_house",
        "monitoring_table",
        "monitoring_field",
        "monitoring_text",
        "monitoring_level",
        "warn_desc", "rule_desc",
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

          house + "\u0001" + table + "\u0001" + s"$field" + "\u0001" + value + "\u0001" + "正确" + "\u0001" + "枚举字段是否新增" + "\u0001" + 0

        } else {

          house + "\u0001" + table + "\u0001" + s"$field" + "\u0001" + value + "\u0001" + "新增类型" + "\u0001" + "枚举字段是否新增" + "\u0001" + 2

        }

        val split = payStatusRes.split("\u0001")
        //监控字段+当前值+正确值+预警+预警描述+监控来源
        (split(0), split(1), split(2), split(3), split(4), split(5), split(6))
      }).toDF("monitoring_house", "monitoring_table", "monitoring_field", "monitoring_text", "warn_desc", "rule_desc", "monitoring_level")
    val resTable = resTemp.
      selectExpr(
        "monitoring_house",
        "monitoring_table",
        "monitoring_field",
        "monitoring_text",
        "monitoring_level",
        "warn_desc", "rule_desc",
        "getNow() as create_time",
        "getNow() as update_time")
    resTable.registerTempTable("EnumerationTypeMonitoring")
    val res = SQLContext.sql("select * from EnumerationTypeMonitoring where monitoring_level=1 or monitoring_level=2")
    res
  }


  //Mysql表费率超过正常范围值明细数据
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
        val rateStr = if (rate / 100 >= 0 && rate / 100 <= 1) {
          house + "\u0001" + s"$table" + "\u0001" + s"$field" + "\u0001" + rate + "\u0001" + "费率正常" + "\u0001" + "费率超过正常范围" + "\u0001" + 0
        } else if (rate / 100 > 1) {
          house + "\u0001" + s"$table" + "\u0001" + s"$field" + "\u0001" + rate + "\u0001" + "费率超过1" + "\u0001" + "费率超过正常范围" + "\u0001" + 2
        } else if (rate / 100 == null) {
          house + "\u0001" + s"$table" + "\u0001" + s"$field" + "\u0001" + rate + "\u0001" + "费率为空" + "\u0001" + "费率超过正常范围" + "\u0001" + 0
        } else {
          house + "\u0001" + s"$table" + "\u0001" + s"$field" + "\u0001" + rate + "\u0001" + "费率小于1" + "\u0001" + "费率超过正常范围" + "\u0001" + 1
        }
        val reString: Array[String] = rateStr.split("\u0001")
        (reString(0), reString(1), reString(2), reString(3), reString(4), reString(5), reString(6))
      }).toDF("monitoring_house", "monitoring_table", "monitoring_field", "monitoring_text", "warn_desc", "rule_desc", "monitoring_level")
    val resTable = resTemp.
      selectExpr(
        "monitoring_house",
        "monitoring_table",
        "monitoring_field",
        "monitoring_text",
        "monitoring_level",
        "warn_desc", "rule_desc",
        "getNow() as create_time",
        "getNow() as update_time")
    resTable.registerTempTable("EnumerationTypeMonitoring")
    val res = SQLContext.sql("select * from EnumerationTypeMonitoring where monitoring_level=1 or monitoring_level=2")
    res
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
    val field = fieldType
    val house = houseName


    val resTemp: DataFrame = hiveContext.sql(s"select cast($field as string) as field from $table")
      .selectExpr("field")
      .map(x => {
        val fieldInfo = x.getAs[String]("field")
        //匹配空格
        val Space: Boolean = SpaceMatching(fieldInfo)
        //匹配换行
        val Linefeed = LinefeedMatching(fieldInfo)
        val str = if (Space == true) {
          house + "\u0001" + table + "\u0001" + s"$field" + "\u0001" + fieldInfo + "\u0001" + "有空格" + "\u0001" + "是否含有特殊字符" + "\u0001" + 2
        } else if (Linefeed == true) {
          house + "\u0001" + table + "\u0001" + s"$field" + "\u0001" + fieldInfo + "\u0001" + "有换行" + "\u0001" + "是否含有特殊字符" + "\u0001" + 1
        } else {
          house + "\u0001" + table + "\u0001" + s"$field" + "\u0001" + fieldInfo + "\u0001" + "正确" + "\u0001" + "是否含有特殊字符" + "\u0001" + 0
        }
        val strings = str.split("\u0001")
        (strings(0), strings(1), strings(2), strings(3), strings(4), strings(5), strings(6))
      }).toDF("monitoring_house", "monitoring_table", "monitoring_field", "monitoring_text", "warn_desc", "rule_desc", "monitoring_level")
    val resTable = resTemp.
      selectExpr(
        "monitoring_house",
        "monitoring_table",
        "monitoring_field",
        "monitoring_text",
        "monitoring_level",
        "warn_desc",
        "rule_desc",
        "getNow() as create_time",
        "getNow() as update_time")

    resTable.registerTempTable("SpecialCharacterMonitorings")
    val res = hiveContext.sql("select * from SpecialCharacterMonitorings where monitoring_level=1 or monitoring_level=2")

    res
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