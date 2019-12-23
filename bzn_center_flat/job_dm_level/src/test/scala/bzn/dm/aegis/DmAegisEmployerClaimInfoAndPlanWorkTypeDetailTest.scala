package bzn.dm.aegis

import bzn.dm.util.SparkUtil
import bzn.job.common.{ClickHouseUntil, MysqlUntil, Until}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext

/**
  * author:xiaoYuanRen
  * Date:2019/11/26
  * Time:17:06
  * describe: 神盾-赔付概况
  **/
object DmAegisEmployerClaimInfoAndPlanWorkTypeDetailTest extends SparkUtil with Until with MysqlUntil{
  def main (args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "local[*]")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    val aegisEmployerClaimInfoData = getAegisEmployerClaimInfoData(hiveContext)
    val aegisEmployerPlanAndWorkTypeInfoData = getAegisEmployerPlanAndWorkTypeInfoData(hiveContext)
    val tableAegisEmployerClaimInfoDataName = "dm_aegis_emp_claim_info_detail"
    val tableAegisEmployerPlanAndWorkTypeInfoDataName = "dm_aegis_emp_plan_work_type_detail"
    val url = "mysql_url.103.dmdb"
    val user = "mysql.username.103"
    val pass = "mysql.password.103"
    val driver = "mysql.driver"
    saveASMysqlTable(aegisEmployerClaimInfoData: DataFrame, tableAegisEmployerClaimInfoDataName: String, SaveMode.Overwrite,user:String,pass:String,driver:String,url:String)
    //saveASMysqlTable(aegisEmployerPlanAndWorkTypeInfoData: DataFrame, tableAegisEmployerPlanAndWorkTypeInfoDataName: String, SaveMode.Overwrite,user:String,pass:String,driver:String,url:String)
    sc.stop()
  }

  /**
    * 得到雇主赔付概况数据
    * @param sqlContext 上下文
    * @return
    */
  def getAegisEmployerClaimInfoData(sqlContext:HiveContext): DataFrame = {
    import sqlContext.implicits._
    /**
      * 读取理赔数据
      */
    val dwPolicyClaimDetail = sqlContext.sql("select channel_id,if(channel_name = '直客',ent_name,channel_name) as channel_name," +
      "risk_policy_code as policy_code,product_code,case_no,risk_date,report_date,risk_name," +
      "risk_cert_no,case_type,scene,case_status,pre_com as prepare_claim_premium,final_payment,case_close_date,res_pay as final_prepare_claim_premium," +
      "cast(now() as timestamp) as create_time,cast(now() as timestamp) as update_time from dwdb.dw_policy_claim_detail")

    val dwEmployerBaseinfoPersonDetailOne =
      sqlContext.sql("select  policy_code,insured_cert_no,work_type ,row_number() over (partition by policy_code,insured_cert_no order by policy_start_date desc) as rand1,product_code " +
        "from dwdb.dw_employer_baseinfo_person_detail where product_code not in ('LGB000001','17000001')")
      .where("rand1 = 1")
      .selectExpr("policy_code as policy_code_slave","insured_cert_no as insured_cert_no_slave","work_type")

    /**
      * 得到雇主的保单号用户筛选出雇主的理赔的数据
      */
    val empData = dwEmployerBaseinfoPersonDetailOne.selectExpr("policy_code_slave").distinct()

    val empRiskData = empData.join(dwPolicyClaimDetail,empData("policy_code_slave")===dwPolicyClaimDetail("policy_code"))
      .drop("policy_code_slave")

    val dwPolicyClaimAndBaseInfo = empRiskData.join(dwEmployerBaseinfoPersonDetailOne,'policy_code ==='policy_code_slave and 'risk_cert_no === 'insured_cert_no_slave ,"leftouter")
        .selectExpr(
          "channel_id",
          "channel_name",
          "policy_code",
          "product_code",
          "case_no",
          "risk_date",
          "report_date",
          "risk_name",
          "risk_cert_no",
          "case_type",
          "scene",
          "case_status",
          "prepare_claim_premium",
          "final_payment",
          "case_close_date",
          "final_prepare_claim_premium",
          "work_type",
          "create_time",
          "update_time"
        )
    dwPolicyClaimAndBaseInfo.printSchema()
    dwPolicyClaimAndBaseInfo.show()
    dwPolicyClaimAndBaseInfo
  }

  /**
    * 方案统计与工种占比
    * @param sqlContext 上下文
    */
  def getAegisEmployerPlanAndWorkTypeInfoData(sqlContext:HiveContext): DataFrame = {
    sqlContext.udf.register ("getUUID", () => (java.util.UUID.randomUUID () + "").replace ("-", ""))

    /**
      * 读取方案工种信息表，先对渠道，保险公司，方案以及工种信息进行分组，得到在保人数，保费；在对上述结果使用窗口函数，得到渠道对应的总人数，从而得到
      */
    val res = sqlContext.sql(
      """
        |select *,
        |cast(sum(curr_insured) over (partition by channel_id) as int) as all_curr_insured,
        |cast(now() as timestamp) as create_time,cast(now() as timestamp) as update_time
        |from
        |(
        |   select channel_id,channel_name,insure_company_short_name,profession_type,
        |   case when sku_charge_type = '1' then '月单' when sku_charge_type = '2' then '年单' else '其他' end as sku_charge_type,
        |   concat(cast(cast(sku_coverage as decimal(14,0)) as string),'万') as sku_coverage,
        |   case when sku_ratio = '1' then '5%' when sku_ratio = '2' then '10%' else null end as sku_ratio,
        |   cast(case when sku_charge_type = '2' then sku_price/12 else sku_price end as decimal(14,4)) as sku_price,work_type,
        |   case when bzn_work_risk = '1' then '一类'
        |        when bzn_work_risk = '2' then '二类'
        |        when bzn_work_risk = '3' then '三类'
        |        when bzn_work_risk = '4' then '四类'
        |        when bzn_work_risk = '5' then '五类'
        |        when bzn_work_risk = '6' then '六类'
        |        when bzn_work_risk = '7' then '七类'
        |        else null end as bzn_work_risk,
        |   cast(sum(case when start_date <= now() and end_date >= now() then 1 else 0 end) as int) as curr_insured,
        |   cast(sum(if(res_pay is null,0,res_pay)) as decimal(14,4)) as prepare_claim_premium,
        |   cast(sum(if(charge_premium is null,0,charge_premium)) as decimal(14,4)) as charge_premium
        |   from dwdb.dw_work_type_matching_claim_detail
        |   group by channel_id,channel_name,insure_company_short_name,profession_type,sku_charge_type,sku_coverage,sku_ratio,sku_price,work_type,bzn_work_risk
        |) t
      """.stripMargin)
    res.printSchema()

    res
  }
}