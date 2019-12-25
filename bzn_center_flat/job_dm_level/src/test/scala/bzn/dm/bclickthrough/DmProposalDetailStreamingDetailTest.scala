package bzn.dm.bclickthrough

import java.text.SimpleDateFormat
import java.util.Date

import bzn.dm.util.SparkUtil
import bzn.job.common.Until
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

/**
  * author:xiaoYuanRen
  * Date:2019/12/25
  * Time:17:17
  * describe: this is new class
  **/
object DmProposalDetailStreamingDetailTest extends SparkUtil with Until {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName,"local[*]")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    val res = getProposalData(hiveContext)
    //hiveContext.sql("truncate table odsdb.ods_proposal_operator_daily_detail")
    //res.repartition(10).write.mode(SaveMode.Append).saveAsTable("odsdb.ods_proposal_operator_daily_detail")

    sc.stop()
  }

  def getProposalData(sqlContext:HiveContext) = {
    import sqlContext.implicits._
    sqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    sqlContext.udf.register("clean", (str: String) => clean(str))
    sqlContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")//设置日期格式
      val date = df.format(new Date())// new Date()为获取当前系统时间
      date + ""
    })

    /**
      * 读取投保单与批单的数据
      */
    val dwProposalOperatorDailyDetail = sqlContext.sql("select *,regexp_replace(subString(getNow(),1,10),'-','') as day_id from dwdb.dw_proposal_operator_daily_detail")
    dwProposalOperatorDailyDetail.show()
    /**
      * 读取雇主基础信息表
      */
//    sqlContext.sql("")
  }
}
