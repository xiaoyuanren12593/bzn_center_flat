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
object DmOds2DmLevelKeyIndictorDetail extends SparkUtil with DataBaseUtil{
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
    val tableNameAgiesKriName = "emp_risk_monitor_kri_detail"
    val ckUser = "mysql.username.106"
    val ckPass = "mysql.password.106"
    val ckUrl = "mysql.url.106.dmdb"
    readClickHouseTable(sqlContext,tableNameAgiesKriName: String,ckUrl:String,ckUser:String,ckPass:String)
      .registerTempTable("emp_risk_monitor_kri_detail")

    val aegisKriData =sqlContext.sql(
      """
        |SELECT SUM(d.curr_insured) as currInsured,
        |SUM(d.acc_case_num) as caseNum,
        |sum(d.acc_sum_premium) as sum_premium,
        |intDivOrZero(toDecimal64(365*caseNum,4),SUM(d.acc_curr_insured)) as riskRate,
        |SUM(d.acc_prepare_claim_premium) as prepareClaimPremium,
        |sum(acc_charge_premium) as charge_premium,
        |round(SUM(d.acc_expire_premium),2) expirePremium,
        |SUM(d.acc_expire_claim_premium) as expireClaimPremium,
        |SUM(d.acc_settled_claim_premium) as settledClaimPremium,
        |from emp_risk_monitor_kri_detail d
        |WHERE  d.day_id=SUBSTRING(cast(now() as String),1,10) and d.insurance_company_short_name in ('中华联合','众安','国寿财','泰康在线');
        |
      """.stripMargin)

    aegisKriData.printSchema()
//    /**
//      * dm层 神盾系统的 单独某个企业风险监控
//      */
//
//    /**
//      * 全局工种分析
//      */


  }
}
