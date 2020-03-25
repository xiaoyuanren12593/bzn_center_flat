package bzn.ods.policy

import bzn.job.common.{MysqlUntil, Until}
import bzn.util.SparkUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext

object OdsClaimPhoenixclaim extends SparkUtil with Until with MysqlUntil {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName: String = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "")

    val sc: SparkContext = sparkConf._2
    val sqlContext = sparkConf._3
    val hiveContext: HiveContext = sparkConf._4

    val res = ClaimPhoenixclaim(hiveContext)
    saveASMysqlTable(res, "ods_claim_detail_temp", SaveMode.Overwrite, "mysql.username", "mysql.password", "mysql.driver", "mysql.url")

    sc.stop()
  }

  def ClaimPhoenixclaim(hiveContext: HiveContext): DataFrame = {
    import hiveContext.implicits._
    val user_106 = "mysql.username.106"
    val ps_106 = "mysql.password.106"
    val driver_106 = "mysql.driver"
    val sourcce_url_106 = "mysql.url.106"


    //读取理赔表
    val claimPhoenixclaim = readMysqlTable(hiveContext, "claim_phoenixclaim", user_106, ps_106, driver_106, sourcce_url_106)
      .selectExpr("case when fnol_no is null then claim_no else fnol_no end as fnol_no",
        "claim_no",
        "end_date",
        "adjuster_name",
        "update_time",
        "status",
        "compensation_amount",
        "apply_amount").where("status !=-1")

    //读取理赔申请责任表,行转列
    val claimApplyLiabilityTemp = readMysqlTable(hiveContext, "claim_apply_liability_phoenixclaim", user_106, ps_106, driver_106, sourcce_url_106)
      .selectExpr("claim_no as claim_no_temp",
        "case when liability_code='2000000224' then compensation_amount else 0 end as hos_benefits",
        "case when liability_code='2000000217' then compensation_amount else 0 end as medical_coverage_1",
        "case when liability_code='2000000216' then compensation_amount else 0 end as medical_coverage_2",
        "case when liability_code='2000000219' then compensation_amount else 0 end as medical_coverage_3",
        "case when liability_code='2000000220' then compensation_amount else 0 end as medical_coverage_4",
        "case when liability_code='2000000256' then compensation_amount else 0 end as medical_coverage_5",
        "case when liability_code='2000000255' then compensation_amount else 0 end as medical_coverage_6",
        "case when liability_code='2000000229' then compensation_amount else 0 end as delay_payment",
        "case when liability_code='2000000209' then compensation_amount else 0 end as disable_death_payment_1",
        "case when liability_code='2000000213' then compensation_amount else 0 end as disable_death_payment_2"
      )

    //注册临时表
    claimApplyLiabilityTemp.registerTempTable("tableTemp")
    val claimApplyLiability = hiveContext.sql("select claim_no_temp,sum(hos_benefits) as hos_benefits," +
      "sum(medical_coverage_1+medical_coverage_2+medical_coverage_3+medical_coverage_4+medical_coverage_5+medical_coverage_6) as medical_coverage," +
      "sum(delay_payment) as delay_payment," +
      "sum(disable_death_payment_1+disable_death_payment_2) as disable_death_payment  from tableTemp group by claim_no_temp")

    //理赔表关联责任表
    val resTemp = claimPhoenixclaim.join(claimApplyLiability, 'claim_no === 'claim_no_temp, "leftouter")
      .selectExpr("fnol_no",
        "claim_no",
        "end_date",
        "adjuster_name",
        "update_time",
        "status",
        "compensation_amount",
        "apply_amount",
        "hos_benefits", "medical_coverage", "delay_payment", "disable_death_payment")

    val res = resTemp.selectExpr("fnol_no",
      "claim_no",
      "medical_coverage as MEDICAL_FEE",
      "delay_payment as WORKING_LOSS_FEE",
      "hos_benefits as HOSP_BENEFIT",
      "disable_death_payment as DEATH_BENEFIT",
      "compensation_amount as PAYOUT_AMOUNT",
      "end_date as END_DATE",
      "adjuster_name as OPERATOR_CODE", "update_time as UPDATE_TIME",
      "case status " +
        "when 0 then '报案-审核通过' " +
        "when 101  then '电子资料审核-初审待审核' " +
        "when 102 then '电子资料审核-初审被退回' " +
        "when 103 then '电子资料审核-保司复审中' " +
        "when 104 then '电子资料审核-被保司复审退回' " +
        "when 201 then '纸质资料审核-纸质资料待签收' " +
        "when 202 then '纸质资料审核-纸质资料待签收' " +
        "when 203 then '纸质资料审核-保司审核中' " +
        "when 204 then '纸质资料审核-被保司退回' " +
        "when 205 then '纸质资料审核-补充资料待初审' " +
        "when 503 then '撤案' " +
        "when 601 then '结案' " +
        "when 701 then '关闭' " +
        "else '其他' end as CASE_STATUS")
    res

  }
}