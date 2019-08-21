package bzn.dm.backup

import bzn.dm.util.SparkUtil
import bzn.job.common.Until
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * author:xiaoYuanRen
  * Date:2019/7/25
  * Time:14:19
  * describe: 备份每周的保单  人员明细  保全 人员明细  保费  每日在保人数据
  **/
object DmBackUpWeekDetail extends SparkUtil with Until{
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName,"")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4

    /**
      * 当前时间-1
      * 每周五存数据
      */
    val nowTime = currTimeFuction(getNowTime(),-1)
    val weekInt = getWeekOfDate(nowTime)
    if(weekInt == 5){
      odsBackUpWeekDetail(hiveContext)
      dwBackUpWeekDetail(hiveContext)
    }

    sc.stop()
  }

  /**
    * ods层备份每周数据
    * @param sqlContext
    */
  def odsBackUpWeekDetail(sqlContext:HiveContext) = {
    sqlContext.setConf("hive.exec.dynamic.partition.mode","nonstrict")
    sqlContext.udf.register("partiton_cul",() => {
      val randomInt = scala.util.Random.nextInt(10)
      randomInt
    })
    sqlContext.udf.register("getTime",() => {
      val nowTime =currTimeFuction(getNowTime(),-1).substring(0,10)
      nowTime
    })
    //#######################ods层的数据
    /**
      * 读取保单表
      */
    val odsPolicyDetail = sqlContext.sql("select * , getTime() as date_time from odsdb.ods_policy_detail").cache()
    /**
      * 读取被保人明细表
      */
    val odsPolicyInsuredDetail = sqlContext.sql("select * , getTime() as date_time from odsdb.ods_policy_insured_detail").cache()
    /**
      * 读取从属被保人明细表
      */
    val odsPolicyInsuredSlaveDetail = sqlContext.sql("select * , getTime() as date_time from odsdb.ods_policy_insured_slave_detail").cache()
    /**
      * 读取保全明细表
      */
    val odsPreservationDetail = sqlContext.sql("select * , getTime() as date_time from odsdb.ods_preservation_detail").cache()
    /**
      * 读取保全被保人明细表
      */
    val odsPreservationMasterDetail = sqlContext.sql("select * , getTime() as date_time from odsdb.ods_preservation_master_detail").cache()
    /**
      * 读取保全从属被保人明细表
      */
    val odsPreservationSlaveDetail = sqlContext.sql("select * , getTime() as date_time from odsdb.ods_preservation_slave_detail").cache()

    /**
      * 保存
      */
    odsPolicyDetail.repartition(10).write.mode(SaveMode.Append).format("parquet").partitionBy("date_time").saveAsTable("backupdb.ods_policy_detail_backup_week")
    odsPolicyInsuredDetail.repartition(10).write.mode(SaveMode.Append).format("parquet").partitionBy("date_time").saveAsTable("backupdb.ods_policy_insured_detail_backup_week")
    odsPolicyInsuredSlaveDetail.repartition(10).write.mode(SaveMode.Append).format("parquet").partitionBy("date_time").saveAsTable("backupdb.ods_policy_insured_slave_detail_backup_week")
    odsPreservationDetail.repartition(10).write.mode(SaveMode.Append).format("parquet").partitionBy("date_time").saveAsTable("backupdb.ods_preservation_detail_backup_week")
    odsPreservationMasterDetail.repartition(10).write.mode(SaveMode.Append).format("parquet").partitionBy("date_time").saveAsTable("backupdb.ods_preservation_master_detail_backup_week")
    odsPreservationSlaveDetail.repartition(10).write.mode(SaveMode.Append).format("parquet").partitionBy("date_time").saveAsTable("backupdb.ods_preservation_slave_detail_backup_week")
  }

  /**
    * dw层备份每周数据
    * @param sqlContext
    */
  def dwBackUpWeekDetail(sqlContext:HiveContext) = {
    sqlContext.setConf("hive.exec.dynamic.partition.mode","nonstrict")
    sqlContext.udf.register("getTime",() => {
      val nowTime =currTimeFuction(getNowTime(),-1).substring(0,10)
      nowTime
    })
    //#######################dw层的数据
    /**
      * 读取当前在保人明细表
      */
    val dwPolicyCurrInsuredDetail = sqlContext.sql("select * , getTime() as date_time from dwdb.dw_policy_curr_insured_detail").cache()
    /**
      * 读取每日已赚保费表
      */
    val dwPolicyEverydayPremiumDetail = sqlContext.sql("select * , getTime() as date_time from dwdb.dw_policy_everyday_premium_detail").cache()
    /**
      * 读取出单费用明细数据
      */
    val dwPolicyPremiumDetail = sqlContext.sql("select * , getTime() as date_time from dwdb.dw_policy_premium_detail").cache()

    /**
      * 保存
      */
    dwPolicyCurrInsuredDetail.repartition(10).write.mode(SaveMode.Append).format("parquet").partitionBy("date_time").saveAsTable("backupdb.dw_policy_curr_insured_detail_backup_week")
    dwPolicyEverydayPremiumDetail.repartition(10).write.mode(SaveMode.Append).format("parquet").partitionBy("date_time").saveAsTable("backupdb.dw_policy_everyDay_premium_detail_backup_week")
    dwPolicyPremiumDetail.repartition(10).write.mode(SaveMode.Append).format("parquet").partitionBy("date_time").saveAsTable("backupdb.dw_policy_premium_detail_backup_week")
  }
}
