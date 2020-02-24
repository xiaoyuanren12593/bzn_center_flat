package bzn.dm.monitor

import bzn.dm.util.SparkUtil
import bzn.job.common.DataBaseUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

/**
  * author:xiaoYuanRen
  * Date:2020/2/20
  * Time:15:49
  * describe: ods - dm 关键指标一致性统计
  **/
object DmOds2DmLevelKeyIndictorDetailTest extends SparkUtil with DataBaseUtil{
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "local[*]")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    val sqlContext = sparkConf._3

    val user103 = "mysql.username.103"
    val pass103 = "mysql.password.103"
    val url103 = "mysql_url.103.dmdb"
    val driver = "mysql.driver"
    val tableName1 = "dm_aegis_gsc_risk_target_tracking_info_detail"
    val tableName2 = ""
    val user106 = "mysql.username.106"
    val pass106 = "mysql.password.106"
    val url106 = "mysql.url.106.dmdb"

    getOds2DmlevelKeyData(hiveContext)
    //    saveASMysqlTable(res1: DataFrame, tableName1: String, SaveMode.Overwrite,user106:String,pass106:String,driver:String,url106:String)

    sc.stop()
  }

  /**
    * 得到ods-dm层每一层数据
    * @param sqlContext
    */
  def getOds2DmlevelKeyData(sqlContext:HiveContext) = {

    /**
      * dm层 神盾系统的 全局风险监控
      */
    val tableNameAegisKriName = "emp_risk_monitor_kri_detail"
    val ckUser = "clickhouse.username"
    val ckPass = "clickhouse.password"
    val ckUrl = "clickhouse.url"
    val tableNameAegisClaimName = "dm_aegis_emp_claim_info_detail"
    val tableNameBThrouthPolicyContinueName = "emp_continue_policy_all_info_detail"
    val user106 = "mysql.username.106"
    val pass106 = "mysql.password.106"
    val url106 = "mysql.url.106.dmdb"
    val mysqlDriver = "mysql.driver"

    readClickHouseTable(sqlContext,tableNameAegisKriName: String,ckUrl:String,ckUser:String,ckPass:String)
      .registerTempTable("emp_risk_monitor_kri_detail1")

    val aegisKriData =sqlContext.sql(
      """
        |SELECT SUM(d.curr_insured) as currInsured,
        |SUM(d.acc_case_num) as caseNum,
        |sum(d.acc_sum_premium) as sum_premium,
        |365*sum(d.acc_sum_premium)/SUM(d.acc_curr_insured) as riskRate,
        |SUM(d.acc_prepare_claim_premium) as prepareClaimPremium,
        |sum(acc_charge_premium) as charge_premium,
        |round(SUM(d.acc_expire_premium),2) expirePremium,
        |SUM(d.acc_expire_claim_premium) as expireClaimPremium,
        |SUM(d.acc_settled_claim_premium) as settledClaimPremium
        |from emp_risk_monitor_kri_detail1 d
        |WHERE  d.day_id=SUBSTRING(cast(now() as String),1,10) and d.insurance_company_short_name in ('中华联合','众安','国寿财','泰康在线')
      """.stripMargin).cache()

    val currInsured = aegisKriData.selectExpr("'dm' as dw_level","'神盾' as system","'风险监控报表' as function_name","'ck' as house","'odsdb' as house_name","'emp_risk_monitor_kri_detail1' as table_name","'当前在保人' as indicator","currInsured as dm_indicator_value")
    val caseNum = aegisKriData.selectExpr("'dm' as dw_level","'神盾' as system","'风险监控报表' as function_name","'ck' as house","'odsdb' as house_name","'emp_risk_monitor_kri_detail1' as table_name","'当前在保人' as indicator","caseNum as dm_indicator_value")
    val sum_premium = aegisKriData.selectExpr("'dm' as dw_level","'神盾' as system","'风险监控报表' as function_name","'ck' as house","'odsdb' as house_name","'emp_risk_monitor_kri_detail1' as table_name","'当前在保人' as indicator","sum_premium as dm_indicator_value")
    val riskRate = aegisKriData.selectExpr("'dm' as dw_level","'神盾' as system","'风险监控报表' as function_name","'ck' as house","'odsdb' as house_name","'emp_risk_monitor_kri_detail1' as table_name","'当前在保人' as indicator","riskRate as dm_indicator_value")
    val prepareClaimPremium = aegisKriData.selectExpr("'dm' as dw_level","'神盾' as system","'风险监控报表' as function_name","'ck' as house","'odsdb' as house_name","'emp_risk_monitor_kri_detail1' as table_name","'当前在保人' as indicator","prepareClaimPremium as dm_indicator_value")
    val charge_premium = aegisKriData.selectExpr("'dm' as dw_level","'神盾' as system","'风险监控报表' as function_name","'ck' as house","'odsdb' as house_name","'emp_risk_monitor_kri_detail1' as table_name","'当前在保人' as indicator","charge_premium as dm_indicator_value")
    val expirePremium = aegisKriData.selectExpr("'dm' as dw_level","'神盾' as system","'风险监控报表' as function_name","'ck' as house","'odsdb' as house_name","'emp_risk_monitor_kri_detail1' as table_name","'当前在保人' as indicator","expirePremium as dm_indicator_value")
    val expireClaimPremium = aegisKriData.selectExpr("'dm' as dw_level","'神盾' as system","'风险监控报表' as function_name","'ck' as house","'odsdb' as house_name","'emp_risk_monitor_kri_detail1' as table_name","'当前在保人' as indicator","expireClaimPremium as dm_indicator_value")
    val settledClaimPremium = aegisKriData.selectExpr("'dm' as dw_level","'神盾' as system","'风险监控报表' as function_name","'ck' as house","'odsdb' as house_name","'emp_risk_monitor_kri_detail1' as table_name","'当前在保人' as indicator","settledClaimPremium as dm_indicator_value")

    val aegisKriDataRes = currInsured.unionAll(caseNum).unionAll(sum_premium).unionAll(riskRate).unionAll(prepareClaimPremium).unionAll(charge_premium)
      .unionAll(expirePremium).unionAll(expireClaimPremium).unionAll(settledClaimPremium)


    /**
      * 全局风险监控 伤残案件数与死亡案件数
      */
    readMysqlTable(sqlContext: SQLContext, tableNameAegisClaimName: String,user106:String,pass106:String,mysqlDriver:String,url106:String)
      .registerTempTable("dm_aegis_emp_claim_info_detail")

    val aegisClaimData = sqlContext.sql(
      """
        |select sum(case when case_type = '死亡' then 1 else 0 end) as dead_case_no,
        |sum(case when case_type = '伤残' then 1 else 0 end) as disable_case_no
        |from dm_aegis_emp_claim_info_detail
      """.stripMargin)

    val deadCaseNo = aegisClaimData.selectExpr("'dm' as dw_level","'神盾' as system","'风险监控报表' as function_name","'mysql' as house","'dmdb' as house_name","'emp_risk_monitor_kri_detail1' as table_name","'当前在保人' as indicator","dead_case_no as dm_indicator_value")
    val disableCseNo = aegisClaimData.selectExpr("'dm' as dw_level","'神盾' as system","'风险监控报表' as function_name","'mysql' as house","'dmdb' as house_name","'emp_risk_monitor_kri_detail1' as table_name","'当前在保人' as indicator","disable_case_no as dm_indicator_value")

    val aegisClaimDataRes = deadCaseNo.unionAll(disableCseNo)

    /**
      * b点通 续投趋势  在保人数和b端客户数
      */
    readClickHouseTable(sqlContext,tableNameBThrouthPolicyContinueName: String,ckUrl:String,ckUser:String,ckPass:String)
      .registerTempTable("emp_continue_policy_all_info_detail")

    val bThroughPolicyContinueData = sqlContext.sql(
      """
        |select sum(curr_insured) as curr_insured,
        |COUNT(DISTINCT channel_name) as ent_count
        |from emp_continue_policy_all_info_detail
        |where day_id = SUBSTRING(cast(now() as String),1,10)
      """.stripMargin)

    val currInsuredBClickThrough = bThroughPolicyContinueData.selectExpr("'dm' as dw_level","'b_click_through' as system","'续投追踪分析' as function_name","'ck' as house","'odsdb' as house_name","'emp_continue_policy_all_info_detail' as table_name","'当前在保人' as indicator","curr_insured as dm_indicator_value")
    val entCountBClickThrough = bThroughPolicyContinueData.selectExpr("'dm' as dw_level","'b_click_through' as system","'续投追踪分析' as function_name","'ck' as house","'odsdb' as house_name","'emp_continue_policy_all_info_detail' as table_name","'企业客户数' as indicator","ent_count as dm_indicator_value")

    val bThroughPolicyContinueDataRes = currInsuredBClickThrough.unionAll(entCountBClickThrough)

    bThroughPolicyContinueDataRes.show()
  }
}
