package bzn.dm.aegis

import bzn.dm.util.SparkUtil
import bzn.job.common.{DataBaseUtil, Until}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext

/**
  * author:xiaoYuanRen
  * Date:2020/4/8
  * Time:15:21
  * describe: this is new class
  **/
object DmAegisMonotorAllBusinessDetailTest extends SparkUtil with DataBaseUtil with Until{
  def main(args: Array[String]): Unit = {
    System.setProperty ("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo (appName, "local[8]")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    val allBusinessOverallMonitorData = getAllBusinessOverallMonitorData(hiveContext)
    val tableAllBusinessOverallMonitor = "dm_monitor_all_business_overall_detail"
    val user103 = "mysql.username.103"
    val pass103 = "mysql.password.103"
    val url103 = "mysql_url.103.dmdb"
    val driver = "mysql.driver"

    val user106 = "mysql.username.106"
    val pass106 = "mysql.password.106"
    val url106 = "mysql_url.106.dmdb"
    saveASMysqlTable(allBusinessOverallMonitorData: DataFrame, tableAllBusinessOverallMonitor: String, SaveMode.Overwrite,user103:String,pass103:String,driver:String,url103:String)
    sc.stop ()
  }

  def getAllBusinessOverallMonitorData(sqlContext:HiveContext) = {

    val allData = sqlContext.sql(
      """
        |with dw_accounts_and_tmt_detail_one as (
        |select to_date(performance_accounting_day) as day_id,
        |sum(premium_total) as premium_total,
        |sum(if(economy_fee is null,0,economy_fee)+if(technical_service_fee is null,0,technical_service_fee)) as service_charge,
        |sum(if(brokerage_fee is null,0,brokerage_fee))  as brokerage_fee
        |from dwdb.dw_accounts_and_tmt_detail
        |where to_date(performance_accounting_day) <= to_date(now()) and performance_accounting_day is not null
        |group by to_date(performance_accounting_day)
        |)
        |
        |select cast(b.premium_total as decimal(14,4)) as premium_total,cast((b.premium_total-a.last_premium_total)/a.last_premium_total     as decimal(14,4)) as homochronous_pt_rate, --保费同期占比
        |cast(b.service_charge as decimal(14,4)) as service_charge,     cast((b.service_charge-a.last_service_charge)/a.last_service_charge  as decimal(14,4)) as homochronous_sc_rate,--服务费同期占比
        |cast(b.brokerage_fee as decimal(14,4)) as brokerage_fee,       cast((b.brokerage_fee-a.last_brokerage_fee)/a.last_brokerage_fee     as decimal(14,4)) as homochronous_bf_rate,--返佣同期占比
        |to_date(now()) as dw_create_time
        |from (
        |    --上个月同期 总保费，手续费，返佣费
        |    select sum(premium_total) as last_premium_total,sum(service_charge) as last_service_charge,sum(brokerage_fee)  as last_brokerage_fee
        |    from dw_accounts_and_tmt_detail_one
        |    where month(day_id) = month(add_months(now(),-1)) and year(day_id) = year(add_months(now(),-1)) and  day_id <= to_date(add_months(now(),-1))
        |) a
        |left join
        |(
        |    --当前月的总保费，手续费，返佣费
        |    select sum(premium_total) as premium_total,sum(service_charge) as service_charge,sum(brokerage_fee)  as brokerage_fee
        |    from dw_accounts_and_tmt_detail_one
        |    where month(day_id) = month(now()) and year(day_id) = year(now())
        |) b
        |on 1=1
      """.stripMargin)
    allData.printSchema()
    allData
  }
}
