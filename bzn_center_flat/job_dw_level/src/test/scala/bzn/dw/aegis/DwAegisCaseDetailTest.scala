package bzn.dw.aegis

import bzn.dw.premium.DwEmploerBaseInfoPersonDetailTest.{getBeginTime, getFormatTime}
import bzn.dw.saleeasy.DwCustomerBaseDetailTest.{CustomerBase, sparkConfInfo}
import bzn.dw.util.SparkUtil
import bzn.job.common.{MysqlUntil, Until}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.hive.HiveContext

/*
* @Author:liuxiang
* @Date：2019/12/24
* @Describe:
*/

object DwAegisCaseDetailTest extends SparkUtil with Until {

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
    CaseDetail(hiveContext)
    sc.stop()
  }

  /**
    *
    * @param hiveContext
    * @return
    */

  def CaseDetail(hiveContext: HiveContext): DataFrame = {
    import hiveContext.implicits._
    //读取保单明细表
    val odsPolicyDetailTemp: DataFrame = hiveContext.sql("select policy_id,holder_name,insured_subject," +
      ",policy_status,policy_start_date,policy_end_date from odsdb.ods_policy_detail")
      .where("policy_status in (1,0,-1)")

    //读取被保人表
    val odsPolicyInsuredDetail = hiveContext.sql("select policy_id as id,insured_cert_no,start_date,end_date from odsdb.ods_policy_insured_detail")

    // 关联被保人表
    val policyAndInsured = odsPolicyDetailTemp.join(odsPolicyInsuredDetail, 'policy_id === 'id, "leftouter")
      .selectExpr("policy_id", "holder_name", "insured_subject", "policy_status", "policy_start_date",
        "policy_end_date", "insured_cert_no", "start_date", "end_date")
    policyAndInsured.registerTempTable("policyAndInsuredTemp")
    val res1 = hiveContext.sql("select policy_id,insured_cert_no,policy_start_date,policy_end_date from policyAndInsuredTemp group by policy_id,insured_cert_no,policy_start_date,policy_end_date")

    //




    //读取理赔表
    val dwPolicyClaimDetailSalve = hiveContext.sql("select case_no,policy_id,risk_date,report_date,product_code,risk_name,risk_cert_no," +
      "case_status,disable_level,case_type,hos_benefits,medical_coverage,pre_com,disable_death_payment,delay_payment,disable_death_payment,res_pay from dwdb.dw_policy_claim_detail")
    //关联理赔表
    val dwPolicyClaimDetailTemp = dwPolicyClaimDetailSalve.join(odsPolicyInsuredDetail, 'policy_id === 'id and 'risk_cert_no === 'insured_cert_no, "leftouter")
      .selectExpr("case_no", "policy_id", "risk_date", "product_code", "risk_name", "risk_cert_no", "start_date", "end_date",
        "case_status", "disable_level", "case_type", "hos_benefits", "medical_coverage", "pre_com", "disable_death_payment",
        "delay_payment", "disable_death_payment", "res_pay")

    println(dwPolicyClaimDetailTemp.count())

    //性别，年龄，年代，星座的计算
    val age: DataFrame = dwPolicyClaimDetailTemp.map(x => {

      val certNo = x.getAs[String]("risk_cert_no")
      val riskDate = x.getAs[String]("risk_date")
      val base = if (certNo == null || certNo.length != 18) {
        null
      } else {
        val area = certNo.substring(0, 6)
        val birthday = certNo.substring(6, 14)
        val age = certNo.substring(7, 8)
        val sex = certNo.substring(16, 17)
        (area, birthday, sex, age)
      }
      //判断性别
      val sex = if (base == null) {
        null
      } else {
        if (base._3 == "1" || base._3 == "3" || base._3 == "5" || base._3 == "7" || base._3 == "9") {
          "男"
        } else {
          "女"
        }
      }
      //年代
      val ageTime = if (base == null) {
        null
      } else {
        base._2.substring(2, 3).concat("0后")
      }
      //格式化出险日期
      val riskDateResTemp = if (riskDate != null) {
        java.sql.Timestamp.valueOf(getFormatTime(getBeginTime(riskDate.replaceAll("/", "-").concat(" 00:00:00"))))
      } else {
        null
      }
      val riskDateRes = if (riskDateResTemp != null) {
        riskDateResTemp.toString.substring(0, 10)
      } else {
        null
      }
      (certNo, sex, ageTime, riskDateRes)

    }).toDF("cert_no", "sex", "age_time", "risk_date_temp")

    val dwPolicyClaimDetail = dwPolicyClaimDetailTemp.join(age, 'risk_cert_no === 'cert_no, "leftouter")
      .selectExpr("case_no", "policy_id", "risk_date", "risk_date_temp", "product_code", "risk_name", "risk_cert_no",
        "case_status", "disable_level", "case_type", "hos_benefits", "medical_coverage", "pre_com", "disable_death_payment", "delay_payment", "disable_death_payment", "res_pay")
      .distinct()
    println(dwPolicyClaimDetail.count())

    //读取工种表
    val dwWorkTypeMatchingDetail = hiveContext.sql("select policy_id as policy_id_salve,insured_name,insured_cert_no,work_type,job_company," +
      "bzn_work_name,bzn_work_risk from dwdb.dw_work_type_matching_detail")

    //读取雇主基础信息表
    val dwEmployerBaseInfoDetail = hiveContext.sql("select policy_id as policy_id_salve,policy_start_date,policy_end_date,holder_name," +
      "insure_subject,work_type, insured_cert_no,start_date,end_date,channel_name,insure_company_name,insure_company_short_name,sale_name," +
      "team_name,biz_operator,sku_coverage,sku_append,sku_ratio,sku_price,sku_charge_type from dwdb.dw_employer_baseinfo_person_detail")

    //匹配表 关联 雇主基础信息表
    val dwPolicyEmpRes = dwPolicyClaimDetail.join(dwEmployerBaseInfoDetail, 'policy_id === 'policy_id_salve and 'risk_cert_no === 'insured_cert_no, "leftouter")
      .selectExpr("case_no", "policy_id", "risk_date", "risk_date_temp", "product_code", "risk_name", "risk_cert_no",
        "case_status", "disable_level", "case_type", "hos_benefits", "medical_coverage", "pre_com", "disable_death_payment",
        "delay_payment", "disable_death_payment", "res_pay", "policy_start_date", "policy_end_date", "holder_name",
        "insure_subject", "work_type", "start_date", "end_date", "channel_name", "insure_company_name", "insure_company_short_name", "sale_name",
        "team_name", "biz_operator", "sku_coverage", "sku_append", "sku_ratio", "sku_price", "sku_charge_type")

    //上层结果关联工种表
    val res = dwPolicyEmpRes.join(dwWorkTypeMatchingDetail, 'policy_id === 'policy_id_salve and 'risk_cert_no === 'insured_cert_no, "leftouter")
      .selectExpr(
        "case_no",
        "policy_id",
        "risk_date",
        "risk_date_temp",
        "product_code",
        "risk_name",
        "risk_cert_no",
        "case_status",
        "disable_level",
        "case_type",
        "hos_benefits",
        "medical_coverage",
        "pre_com",
        "disable_death_payment",
        "delay_payment",
        "disable_death_payment",
        "res_pay",
        "policy_start_date",
        "policy_end_date",
        "holder_name",
        "insure_subject",
        "work_type",
        "start_date",
        "end_date",
        "channel_name",
        "insure_company_name",
        "insure_company_short_name",
        "sale_name",
        "team_name",
        "biz_operator",
        "sku_coverage",
        "sku_append",
        "sku_ratio",
        "sku_price",
        "sku_charge_type",
        "insured_name",
        "insured_cert_no",
        "work_type",
        "job_company",
        "bzn_work_name",
        "bzn_work_risk")
    res

  }


}