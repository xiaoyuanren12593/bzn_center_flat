package bzn.ods.policy

import java.sql.Timestamp

import bzn.job.common.{DataBaseUtil, Until}
import bzn.util.SparkUtil
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * author:xiaoYuanRen
  * Date:2019/12/17
  * Time:17:49
  * describe: 插入默认的监控数据，默认为执行失败
  **/
object OdsInsertMonitorDefaultDetail extends SparkUtil with DataBaseUtil with  Until{
  case class DmbBatchingMonitoringDetail(id: String,project_name:String,warehouse_leve:String,house_name:String,table_name:String,
                                         status:Int,remark:String,create_time:Timestamp,update_time:Timestamp)
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName,"")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4

    import hiveContext.implicits._

    val tableMysqlName = "dm_batching_monitoring_detail"
    val updateColumns: Array[String] = Array("status","remark")
    val urlFormat = "mysql.url.103.dmdb"
    val userFormat = "mysql.username.103"
    val possWordFormat = "mysql.password.103"
    val driverFormat = "mysql.driver"
    val nowTime = getNowTime().substring(0,10)

    val dataMonitor =
      Seq(
        DmbBatchingMonitoringDetail(nowTime+"dm_batching_monitoring_detail","销售易体育数据清洗","dm","dmdb","dm_batching_monitoring_detail",0,"销售易体育数据清洗-失败",new Timestamp(System.currentTimeMillis()),new Timestamp(System.currentTimeMillis()))
      ).toDF()

    insertOrUpdateDFtoDBUsePoolNew(tableMysqlName: String, dataMonitor: DataFrame, updateColumns: Array[String],
      urlFormat:String,userFormat:String,possWordFormat:String,driverFormat:String)

    sc.stop()
  }
}
