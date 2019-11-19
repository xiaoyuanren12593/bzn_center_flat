package bzn.dm.aegis

import bzn.dm.util.SparkUtil
import bzn.job.common.{MysqlUntil, Until}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * author:xiaoYuanRen
  * Date:2019/11/19
  * Time:14:35
  * describe: 雇主风险监控
  **/
object DmAegisEmployerRiskMonitoringDetailTest extends SparkUtil with Until with MysqlUntil{
  def main (args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "local[*]")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    getAegisEmployerRiskMonitoring(hiveContext)
    sc.stop()
  }

  /**
    * 得到雇主风控的基础数据
    * @param sqlContext 上下文
    */
  def getAegisEmployerRiskMonitoring(sqlContext:HiveContext) ={
    import sqlContext.implicits._

    /**
      * 在保人表
      */
    val dwPolicyCurrInsuredDetail =
      sqlContext.sql("select policy_id as policy_id_insured,policy_code as policy_code_insured,day_id as day_id_insured,count from dwdb.dw_policy_curr_insured_detail")

    /**
      * 已赚保费表
      */
    val dwPolicyEverydayPremiumDetail =
      sqlContext.sql("select policy_id as policy_id_premium,day_id as day_id_premium,premium from dwdb.dw_policy_everyday_premium_detail")

    /**
      * 累计保费表
      */
    val dwPolicyPremiumDetail =
      sqlContext.sql("select policy_id,policy_code,day_id,add_person_count,del_person_count,add_premium,del_premium," +
      "sum_premium from dwdb.dw_policy_premium_detail where one_level_pdt_cate = '蓝领外包'")

    /**
      * 雇主基础数据表
      */
    val dwEmployerBaseinfoDetail =
      sqlContext.sql("select policy_id as policy_id_master,policy_code as policy_code_master,channel_id,channel_name,policy_start_date," +
      "case when policy_end_date is null then policy_start_date else policy_end_date end as policy_end_date,insure_company_name," +
      "insure_company_short_name,sku_charge_type from dwdb.dw_employer_baseinfo_detail")

    /**
      * 理赔表
      */
    val dwPolicyClaimDetail =
      sqlContext.sql("select policy_id,policy_code,case_no,risk_date,case_status,res_pay from dwdb.dw_policy_claim_detail")
      .where("risk_date is not null")

    val claimData = getClaimData(sqlContext,dwPolicyClaimDetail)
      .selectExpr("policy_id","policy_code","risk_date","case_no","cast(end_risk_premium as decimal(14,4)) as end_risk_premium",
        "cast(res_pay as decimal(14,4)) as res_pay")

    claimData.show()

    /**
      * 满期保费数据
      */
    val expireData = getExpirePremiumData(sqlContext,dwPolicyEverydayPremiumDetail,dwEmployerBaseinfoDetail)
      .selectExpr("policy_code_master", "expire_day_id", "expire_premium")

    /**
      * 在保人和已赚保费
      */
    val insuedAndPremium = dwPolicyCurrInsuredDetail.join(dwPolicyEverydayPremiumDetail,'policy_id_insured === 'policy_id_premium and 'day_id_insured==='day_id_premium ,"leftouter")
      .selectExpr(
        "policy_code_insured",
        "day_id_insured",
        "count as insured_count",
        "premium as charge_premium"
      )

    insuedAndPremium.show()
  }

  /**
    * 得到满期保费数据
    * @param sqlContext 上下文
    */
  def getExpirePremiumData(sqlContext:HiveContext,dwPolicyEverydayPremiumDetail:DataFrame,dwEmployerBaseinfoDetail:DataFrame): DataFrame = {
    /**
      * 筛选出基础数据
      */
    val dwEmployerBaseinfoOne =
      dwEmployerBaseinfoDetail.selectExpr("policy_id_master","policy_code_master",
        "regexp_replace(substr(cast(policy_end_date as string),1,10),'-','') as expire_day_id")

    /**
      * 满期保费
      */
    dwEmployerBaseinfoOne.join(dwPolicyEverydayPremiumDetail,dwEmployerBaseinfoOne("policy_id_master")===dwPolicyEverydayPremiumDetail("policy_id_premium"))
      .selectExpr(
        "policy_id_master",
        "policy_code_master",
        "expire_day_id",
        "premium"
      ).registerTempTable("expirePremium")

    val res = sqlContext.sql("select policy_code_master,expire_day_id,sum(case when premium is null then 0 else premium end) as expire_premium " +
      "from expirePremium group by policy_code_master,expire_day_id")

    res
  }

  /**
    * 处理理赔相关的数据
    * @param sqlContext 上下文
    * @param dwPolicyClaimDetail 理赔数据
    */
  def getClaimData(sqlContext:HiveContext,dwPolicyClaimDetail:DataFrame): DataFrame = {
    import sqlContext.implicits._

    val res = dwPolicyClaimDetail.map(x => {
      //policy_id,policy_code,case_no,risk_date,case_status,res_pay
      val policyId = x.getAs[String]("policy_id")
      val policyCode = x.getAs[String]("policy_code")
      val riskDate = getFormatTime(getBeginTime(x.getAs[String]("risk_date").replaceAll("/", "-").concat(" 00:00:00"))).substring(0,10).replaceAll("-","")
      val caseStatus = x.getAs[String]("case_status")
      //已决预估赔付
      val endRiskPremium = if(caseStatus == "结案"){
        x.getAs[java.math.BigDecimal]("res_pay")
      }else{
        java.math.BigDecimal.valueOf(0.0)
      }
      //预估赔付
      val resPay = x.getAs[java.math.BigDecimal]("res_pay")
                                       //案件数,结案赔付，预估赔付
      ((policyId,policyCode,riskDate),(1,endRiskPremium,resPay))
    })
      .reduceByKey((x1,x2)=>{
        val caseNo = x1._1+x2._1
        val endRiskPremium = x1._2.add(x2._2)
        val resPay = x1._3.add(x2._3)
        (caseNo,endRiskPremium,resPay)
      })
      .map(x =>{
        (x._1._1,x._1._2,x._1._3,x._2._1,x._2._2,x._2._3)
      })
      .toDF("policy_id","policy_code","risk_date","case_no","end_risk_premium","res_pay")

    res
  }
}
