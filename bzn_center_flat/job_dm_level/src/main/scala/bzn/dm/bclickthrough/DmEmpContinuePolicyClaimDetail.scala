package bzn.dm.bclickthrough

import bzn.dm.util.SparkUtil
import bzn.job.common.{ClickHouseUntil, Until}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * author:xiaoYuanRen
  * Date:2019/12/16
  * Time:17:12
  * describe: 续投追踪的理赔数据
  **/
object DmEmpContinuePolicyClaimDetail extends SparkUtil with Until with ClickHouseUntil{
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName,"")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4

    val tableName1 = "emp_risk_monitor_kri_detail"
    val tableName2 = "emp_continue_policy_claim_detail"
    val urlTest = "clickhouse.url.odsdb.test"
    val urlOfficial = "clickhouse.url"
    val user = "clickhouse.username"
    val possWord = "clickhouse.password"
    val driver = "clickhouse.driver"

    val ckData = getCKRiskMonitorData(hiveContext:HiveContext,tableName1: String,urlOfficial:String,user:String,possWord:String)
    writeClickHouseTable(ckData:DataFrame,tableName2: String,SaveMode.Overwrite,urlTest:String,user:String,possWord:String,driver:String)

    sc.stop()
  }

  /**
    * 获得监控数据，得到每天的每个渠道每个保险公司的理赔数据
    * @param sqlContext 上下文
    */
  def getCKRiskMonitorData(sqlContext:HiveContext,tableName: String,url:String,user:String,possWord:String): DataFrame = {
    sqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    /**
      * 读取神盾监控kri全局指标数据
      */
    readClickHouseTable(sqlContext:SQLContext,tableName: String,url:String,user:String,possWord:String)
      .selectExpr("channel_id","channel_name","insurance_company_short_name","day_id","acc_charge_premium","acc_prepare_claim_premium")
      .registerTempTable("emp_risk_monitor_kri_detail_temp")

    val readClickHouseTableRes = sqlContext.sql(
      """
        |select channel_id,channel_name,insurance_company_short_name,day_id,
        |sum(acc_charge_premium) as acc_charge_premium,
        |sum(acc_prepare_claim_premium) as acc_prepare_claim_premium
        |from emp_risk_monitor_kri_detail_temp
        |GROUP by channel_id,channel_name,insurance_company_short_name,day_id
      """.stripMargin)

    val res = readClickHouseTableRes.selectExpr(
      "getUUID() as id",
      "channel_id",
      "channel_name",
      "insurance_company_short_name",
      "day_id",
      "acc_charge_premium",
      "acc_prepare_claim_premium",
      "date_format(now(),'yyyy-MM-dd HH:mm:ss') as create_time",
      "date_format(now(),'yyyy-MM-dd HH:mm:ss') as update_time"
    )

    res
  }
}
