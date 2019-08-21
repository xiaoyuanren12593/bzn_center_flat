package bzn.dm.backup

import bzn.job.common.Until
import bzn.dm.util.SparkUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext

/**
  * author:xiaoYuanRen
  * Date:2019/7/25
  * Time:10:53
  * describe: 备份每天的保单  人员明细  保全 人员明细  保费  每日在保人数据
  **/
object DmBackUpDayDetailTest extends SparkUtil with Until{
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName,"local[*]")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    odsBackUpDayDetail(hiveContext)
    dwBackUpDayDetail(hiveContext)
    sc.stop()
  }

  /**
    * 备份每日数据
    * @param sqlContext sql 上下文
    */
  def odsBackUpDayDetail(sqlContext:HiveContext) = {
    /**
      * 清空表
      */
    sqlContext.sql("truncate table backupdb.ods_policy_detail_backup_day")
    sqlContext.sql("truncate table backupdb.ods_policy_insured_detail_backup_day")
    sqlContext.sql("truncate table backupdb.ods_policy_insured_slave_detail_backup_day")
    sqlContext.sql("truncate table backupdb.ods_preservation_detail_backup_day")
    sqlContext.sql("truncate table backupdb.ods_preservation_master_detail_backup_day")
    sqlContext.sql("truncate table backupdb.ods_preservation_slave_detail_backup_day")

    //#######################ods层的数据
    /**
      * 读取保单表
      */
    val odsPolicyDetail = sqlContext.sql("select * from odsdb.ods_policy_detail").cache().printSchema()
    /**
      * 读取被保人明细表
      */
    val odsPolicyInsuredDetail = sqlContext.sql("select * from odsdb.ods_policy_insured_detail").cache().printSchema()
    /**
      * 读取从属被保人明细表
      */
    val odsPolicyInsuredSlaveDetail = sqlContext.sql("select * from odsdb.ods_policy_insured_slave_detail").cache().printSchema()
    /**
      * 读取保全明细表
      */
    val odsPreservationDetail = sqlContext.sql("select * from odsdb.ods_preservation_detail").cache().printSchema()
    /**
      * 读取保全被保人明细表
      */
    val odsPreservationMasterDetail = sqlContext.sql("select * from odsdb.ods_preservation_master_detail").cache().printSchema()
    /**
      * 读取保全从属被保人明细表
      */
    val odsPreservationSlaveDetail = sqlContext.sql("select * from odsdb.ods_preservation_slave_detail").cache().printSchema()

    /**
      * 保存
      */

//    odsPolicyDetail.write.mode(SaveMode.Append).saveAsTable("backupdb.ods_policy_detail_backup_day")
//    odsPolicyInsuredDetail.write.mode(SaveMode.Append).saveAsTable("backupdb.ods_policy_insured_detail_backup_day")
//    odsPolicyInsuredSlaveDetail.write.mode(SaveMode.Append).saveAsTable("backupdb.ods_policy_insured_slave_detail_backup_day")
//    odsPreservationDetail.write.mode(SaveMode.Append).saveAsTable("backupdb.ods_preservation_detail_backup_day")
//    odsPreservationMasterDetail.write.mode(SaveMode.Append).saveAsTable("backupdb.ods_preservation_master_detail_backup_day")
//    odsPreservationSlaveDetail.write.mode(SaveMode.Append).saveAsTable("backupdb.ods_preservation_slave_detail_backup_day")
  }

  /**
    * 备份每日数据
    * @param sqlContext sql 上下文
    */
  def dwBackUpDayDetail(sqlContext:HiveContext) = {
    /**
      * 清空表
      */
    sqlContext.sql("truncate table backupdb.dw_policy_curr_insured_detail_backup_day")
    sqlContext.sql("truncate table backupdb.dw_policy_everyDay_premium_detail_backup_day")
    sqlContext.sql("truncate table backupdb.dw_policy_premium_detail_backup_day")

    //#######################dw层的数据
    /**
      * 读取当前在保人明细表
      */
    val dwPolicyCurrInsuredDetail = sqlContext.sql("select * from dwdb.dw_policy_curr_insured_detail").cache().printSchema()
    /**
      * 读取每日已赚保费表
      */
    val dwPolicyEverydayPremiumDetail = sqlContext.sql("select * from dwdb.dw_policy_everyday_premium_detail").cache().printSchema()
    /**
      * 读取出单费用明细数据
      */
    val dwPolicyPremiumDetail = sqlContext.sql("select * from dwdb.dw_policy_premium_detail").cache().printSchema()

    /**
      * 保存
      */
//    dwPolicyCurrInsuredDetail.write.mode(SaveMode.Append).saveAsTable("backupdb.dw_policy_curr_insured_detail_backup_day")
//    dwPolicyEverydayPremiumDetail.write.mode(SaveMode.Append).saveAsTable("backupdb.dw_policy_everyDay_premium_detail_backup_day")
//    dwPolicyPremiumDetail.write.mode(SaveMode.Append).saveAsTable("backupdb.dw_policy_premium_detail_backup_day")
  }
}
