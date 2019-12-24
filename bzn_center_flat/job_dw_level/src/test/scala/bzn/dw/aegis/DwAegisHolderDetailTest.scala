package bzn.dw.aegis

import bzn.dw.aegis.DwAegisCaseDetailTest.{CaseDetail, sparkConfInfo}
import bzn.dw.util.SparkUtil
import bzn.job.common.Until
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext

/*
* @Author:liuxiang
* @Date：2019/12/24
* @Describe:
*/

   object DwAegisHolderDetailTest extends SparkUtil with Until {

     /**
       * 获取配置信息
       *
       * @param args
       */
     def main(args: Array[String]): Unit = {

       System.setProperty("HADOOP_USER_NAME", "hdfs")
       val appName = this.getClass.getName
       val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "local[*]")

       val sc = sparkConf._2
       val hiveContext = sparkConf._4
       val res = HolderDetail(hiveContext)
       hiveContext.sql("truncate table dwdb.dw_guzhu_policy_holder_detail")
       res.write.mode(SaveMode.Append).saveAsTable("dwdb.dw_guzhu_policy_holder_detail")

       sc.stop()
     }


     def HolderDetail(hiveContext: HiveContext): DataFrame = {
       import hiveContext.implicits._

       //读取雇主基础信息表
       val dwCustomerHolder = hiveContext.sql("select holder_name,channel_name,sale_name as salesman,min(policy_start_date) as policy_start_date," +
         "biz_operator,consumer_category,business_source,holder_province as province,holder_city as city " +
         "from dwdb.dw_employer_baseinfo_detail group by holder_name,channel_name,sale_name,biz_operator," +
         "consumer_category,business_source,holder_province,holder_city")


       //读取再保人表
       val policyCurrInsured = hiveContext.sql("select policy_id as id,day_id,count from dwdb.dw_policy_curr_insured_detail")


       //读取客户信息表(保单级别)
       val dwEmpBaseInfo = hiveContext.sql("select policy_id,holder_name,channel_name from dwdb.dw_employer_baseinfo_detail")

       //在保人数
       val dwEmpBaseInfoAndInsuredRes = dwEmpBaseInfo.join(policyCurrInsured, 'policy_id === 'id, "leftouter")
         .selectExpr("policy_id", "holder_name",  "channel_name", "day_id", "count")
         .map(x => {
           val policyID = x.getAs[String]("policy_id")
           val holderName = x.getAs[String]("holder_name")
           val channelName = x.getAs[String]("channel_name")
           val dayID = x.getAs[String]("day_id")
           val count = x.getAs[Int]("count")
           //获取当前时间
           val nowTime = getNowTime().substring(0, 10).replaceAll("-", "")

           (policyID, holderName, channelName, dayID, count, nowTime)
         }).toDF("policy_id", "holder_name_salve", "holder_company", "day_id", "curr_insured",  "now_time")
         .where("day_id = now_time")

       dwEmpBaseInfoAndInsuredRes.registerTempTable("EmpBaseInfoAndInsuredRes")

       val insuredIntraday = hiveContext.sql("select holder_name_salve,sum(curr_insured) as curr_insured_count from EmpBaseInfoAndInsuredRes group by holder_name_salve")


       val resTemp1 = dwCustomerHolder.join(insuredIntraday,'holder_name==='holder_name_salve, "leftouter")
         .selectExpr("holder_name",
           "channel_name",
           "province",
           "city",
           "null as county",
           "null as registration_time",
           "null as registered_capital",
           "null as industry_involved",
           "policy_start_date",
           "curr_insured_count",
           "salesman",
           "biz_operator",
           "consumer_category",
           "business_source",
           "null as business_model",
           "null as customer_size",
           "null as customer_status",
           "null as old_new_status",
           "null as change_maJia")

       resTemp1.printSchema()
       resTemp1
     }
   }
