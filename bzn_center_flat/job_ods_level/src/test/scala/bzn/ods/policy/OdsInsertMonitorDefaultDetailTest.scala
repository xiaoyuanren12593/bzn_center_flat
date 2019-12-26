package bzn.ods.policy

import java.sql.Timestamp

import bzn.job.common.{DataBaseUtil, Until}
import bzn.util.SparkUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext

/**
  * author:xiaoYuanRen
  * Date:2019/12/17
  * Time:17:49
  * describe: 插入默认的监控数据，默认为执行失败
  **/
object OdsInsertMonitorDefaultDetailTest extends SparkUtil with DataBaseUtil with  Until{
  case class DmbBatchingMonitoringDetail(id: String,source:String,project_name:String,warehouse_level:String,house_name:String,table_name:String,
                                         status:Int,remark:String,create_time:Timestamp,update_time:Timestamp)
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName,"local[*]")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4

    import hiveContext.implicits._

    val tableMysqlName = "dm_batching_monitoring_detail"
    val updateColumns: Array[String] = Array("status","remark","update_time")
    val urlFormatOfficial = "mysql.url.106.dmdb"
    val userFormatOfficial = "mysql.username.106"
    val possWordFormatOfficial = "mysql.password.106"

    val urlFormat = "mysql.url.103.dmdb"
    val userFormat = "mysql.username.103"
    val possWordFormat = "mysql.password.103"

    val driverFormat = "mysql.driver"
    val nowTime = getNowTime().substring(0,10)

    val dataMonitor =
      Seq(
        DmbBatchingMonitoringDetail(nowTime+"`dm_saleeasy_sports_detail`"+"clickhouse","clickhouse","销售易体育数据清洗","dw","odsdb","dm_saleeasy_sports_detail",0,"销售易体育数据清洗-失败",new Timestamp(System.currentTimeMillis()),new Timestamp(System.currentTimeMillis())),
        DmbBatchingMonitoringDetail(nowTime+"`dm_saleeasy_operation_daily_policy_detail`"+"mysql","mysql","运营日报-在保人详情页","dm","dmdb","dm_saleeasy_operation_daily_policy_detail",0,"运营日报-在保人详情页-失败",new Timestamp(System.currentTimeMillis()),new Timestamp(System.currentTimeMillis())),
        DmbBatchingMonitoringDetail(nowTime+"`dm_saleeasy_operation_daily_pro_and_pre_detail`"+"mysql","mysql","运营日报-新投保单和批单详情数据 ","dm","dmdb","dm_saleeasy_operation_daily_pro_and_pre_detail",0,"运营日报-新投保单和批单详情数据-失败",new Timestamp(System.currentTimeMillis()),new Timestamp(System.currentTimeMillis()))
      ).toDF()

    insertOrUpdateDFtoDBUsePoolNew(tableMysqlName: String, dataMonitor: DataFrame, updateColumns: Array[String],
      urlFormat:String,userFormat:String,possWordFormat:String,driverFormat:String)

    insertOrUpdateDFtoDBUsePoolNew(tableMysqlName: String, dataMonitor: DataFrame, updateColumns: Array[String],
      urlFormatOfficial:String,userFormatOfficial:String,possWordFormatOfficial:String,driverFormat:String)

    sc.stop()
  }
}