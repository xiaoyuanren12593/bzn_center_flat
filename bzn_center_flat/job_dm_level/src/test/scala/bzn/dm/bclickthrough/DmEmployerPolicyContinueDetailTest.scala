package bzn.dm.bclickthrough

import bzn.dm.util.SparkUtil
import bzn.job.common.{MysqlUntil, Until}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext

/**
  * author:xiaoYuanRen
  * Date:2019/10/10
  * Time:15:27
  * describe: 雇主续保功能
  **/
object DmEmployerPolicyContinueDetailTest extends SparkUtil with Until with MysqlUntil{
    def main (args: Array[String]): Unit = {
        System.setProperty ("HADOOP_USER_NAME", "hdfs")
        val appName = this.getClass.getName
        val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo (appName, "local[*]")

        val sc = sparkConf._2
        val hiveContext = sparkConf._4
        DmEmployerPolicyContinueDetail(hiveContext)
        sc.stop ()
    }

    /**
      * 雇主续投保单明细数据清洗
      * @param sqlContext
      */
    def DmEmployerPolicyContinueDetail(sqlContext:HiveContext) = {
        import sqlContext.implicits._
        /**
          * 读取雇主基础数据明细表
          */
        val dwEmployerBaseinfoPersonDetail = sqlContext.sql("select * from dwdb.dw_employer_baseinfo_person_detail")
          .where("policy_status in (0,1) and sku_charge_type = '2'")

        /**
          * 读取 时间维度数据
          */
        val odsTDayDimension =
            sqlContext.sql("select d_day,c_day_id,c_week_long_desc,c_month_long_desc,c_quarter_long_desc from odsdb.ods_t_day_dimension")

        /**
          * 读取当前在保人表
          */
        val dwolicyCurrInsuredDetail =
            sqlContext.sql("select policy_id,day_id,count from dwdb.dw_policy_curr_insured_detail")
          .where("day_id = regexp_replace(cast(current_date() as string),'-','')")

        val dwEmployerBaseinfoPersonDetailLeftRes = dwEmployerBaseinfoPersonDetail
          .selectExpr("policy_id","holder_name","policy_start_date","policy_end_date","start_date","end_date","insured_cert_no")
          .map(x => {
              val policyId = x.getAs[String]("policy_id")
              val holderName = x.getAs[String]("holder_name")
              val policyStartDate = x.getAs[java.sql.Timestamp]("policy_start_date")
              val policyEndDate = x.getAs[java.sql.Timestamp]("policy_end_date")
              val startDate = x.getAs[java.sql.Timestamp]("start_date")
              val endDate = x.getAs[java.sql.Timestamp]("end_date")
              val insuredCertNo = x.getAs[String]("insured_cert_no")
              val shouldContinuePolicyDate = if(policyEndDate != null ){
                java.sql.Timestamp.valueOf(currTimeFuction(getFormatDate(policyEndDate),1)) //应续保时间 保单结束时间+1D
              }else{
                null
              }

              var shouldContinuePolicyReferDate = if(policyEndDate != null ){
                java.sql.Timestamp.valueOf(currTimeFuction(getFormatDate(policyEndDate),-30)) //应续保人数的参照时间 保单结束时间-30D
              }else{
                null
              }

              val realyContinuePolicyDate = if(policyEndDate != null ){
                java.sql.Timestamp.valueOf(currTimeFuction(getFormatDate(policyEndDate),30)) //实续参照时间为  保单时间+30d
              }else{
                  null
              }

              if(shouldContinuePolicyReferDate != null ){
                  if (policyStartDate != null){
                      if(shouldContinuePolicyReferDate.compareTo(policyStartDate) < 0){
                        shouldContinuePolicyReferDate = policyStartDate
                      }
                  }
              }

              (policyId,holderName,policyStartDate,policyEndDate,startDate,endDate,insuredCertNo,shouldContinuePolicyDate,shouldContinuePolicyReferDate
              ,realyContinuePolicyDate)
          })
          .toDF("policy_id","holder_name","policy_start_date","policy_end_date","start_date","end_date","insured_cert_no",
          "should_continue_policy_date","should_continue_policy_refer_date","realy_continue_policy_date")

      val dwEmployerBaseinfoPersonDetailRigthRes = dwEmployerBaseinfoPersonDetail.selectExpr("holder_name as holder_name_right","start_date as start_date_right",
        "end_date as end_date_right","insured_cert_no as insured_cert_no_right")

      val dwEmployerBaseinfoPersonDetailRes = dwEmployerBaseinfoPersonDetailLeftRes
        .join(dwEmployerBaseinfoPersonDetailRigthRes,'holder_name==='holder_name_right and 'insured_cert_no==='insured_cert_no_right,"leftouter")
        .selectExpr("policy_id","holder_name","policy_start_date","policy_end_date","start_date","end_date","insured_cert_no",
          "should_continue_policy_date","should_continue_policy_refer_date","realy_continue_policy_date","holder_name_right","start_date_right","end_date_right","insured_cert_no_right")
        .where("start_date_right <= realy_continue_policy_date and end_date_right >= realy_continue_policy_date")
      dwEmployerBaseinfoPersonDetailRes.show()




    }
}
