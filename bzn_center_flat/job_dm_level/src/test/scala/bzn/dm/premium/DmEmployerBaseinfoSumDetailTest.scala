package bzn.dm.premium

import java.text.SimpleDateFormat
import java.util.Date

import bzn.dm.util.SparkUtil
import bzn.job.common.Until
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/*
* @Author:liuxiang
* @Date：2019/9/25
* @Describe:雇主基础信息总表
*/
 object DmEmployerBaseinfoSumDetailTest extends SparkUtil with Until{
  /**
    * 获取相关的配置参数
    */
  def main(args: Array[String]): Unit = {
   System.setProperty("HADOOP_USER_NAME", "hdfs")
   val appName = this.getClass.getName
   val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext)
   = sparkConfInfo(appName, "local[*]")

   val sc = sparkConf._2
   val hiveContext = sparkConf._4
   val res = DmEmployerBaseinfoSum(hiveContext)
   res.printSchema()

  }


 def DmEmployerBaseinfoSum(hqlContext:HiveContext):DataFrame = {
   import hqlContext.implicits._
   hqlContext.udf.register("clean", (str: String) => clean(str))
   hqlContext.udf.register("getNow", () => {
     val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
     //设置日期格式
     val date = df.format(new Date()) // new Date()为获取当前系统时间
     (date + "")
   })

   /**
     * 读取雇主基础信息表
     */
   val dwEmployerBaseInfo = hqlContext.sql("select policy_id,policy_code,policy_start_date,policy_end_date,holder_name,insure_subject,product_code," +
     "one_level_pdt_cate,ent_id,ent_name,channel_id,channel_name," +
     "sale_name,team_name,sku_coverage,sku_append,sku_ratio,sku_price,sku_charge_type,tech_service_rate,economic_rate," +
     "commission_discount_rate,commission_rate,pre_com,final_payment,res_pay from dwdb.dw_employer_baseinfo_detail")

   /**
     * 读取每日已赚保费
     */
   val dwPolicyEverydayPremium = hqlContext.sql("select  policy_id,day_id,premium from dwdb.dw_policy_everyday_premium_detail")
     .map( x => {
       val policy_id = x.getAs[String]("policy_id")
       val day_id = x.getAs[String]("day_id")
       val premium = x.getAs[java.math.BigDecimal]("premium")
       //得到当前时间
       val nowTime = getNowTime().substring(0, 10).replaceAll("-", "")

       if (day_id < nowTime) policy_id

       (policy_id, premium)

     }).reduceByKey(_.add(_))
     .map(x => {
       (x._1, x._2)
     })
     .toDF("policy_id_temp", "premium")

   /**
     * 讲雇主信息表和和每日已赚保费关联
     */
   val restemp = dwEmployerBaseInfo.join(dwPolicyEverydayPremium, dwEmployerBaseInfo("policy_id") === dwPolicyEverydayPremium("policy_id_temp"), "leftouter")
    .selectExpr("policy_id", "policy_id_temp","policy_code", "policy_start_date","policy_end_date","holder_name", "insure_subject", "product_code", "one_level_pdt_cate", "ent_id",
     "ent_name", "channel_id", "channel_name",
     "sale_name", "team_name", "sku_coverage", "sku_append", "sku_ratio", "sku_price",
     "sku_charge_type", "tech_service_rate", "economic_rate",
     "commission_discount_rate", "commission_rate", "pre_com", "final_payment", "res_pay",
     "cast(if(policy_id_temp is null,cast(0.0000 as decimal(14,4)),cast(premium as decimal(14,4))) as decimal(14,4))as premium","getNow() as dw_create_time"
    )

   val res = restemp.selectExpr("policy_id", "policy_code","policy_start_date","policy_end_date", "holder_name", "insure_subject", "product_code", "one_level_pdt_cate", "ent_id",
     "ent_name", "channel_id", "channel_name",
     "sale_name", "team_name", "sku_coverage", "sku_append", "sku_ratio", "sku_price",
     "sku_charge_type", "tech_service_rate", "economic_rate",
     "commission_discount_rate", "commission_rate", "pre_com", "final_payment", "res_pay",
     "premium", "dw_create_time")

   res


 }

}
