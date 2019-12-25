package bzn.dw.aegis

import java.sql.Timestamp

import bzn.dw.premium.DwEmploerBaseInfoPersonDetailTest.{getBeginTime, getFormatTime}
import bzn.dw.saleeasy.DwCustomerBaseDetailTest.{CustomerBase, sparkConfInfo}
import bzn.dw.util.SparkUtil
import bzn.job.common.{MysqlUntil, Until}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
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
    val res = CaseDetail(hiveContext)
    hiveContext.sql("truncate table dwdb.dw_guzhu_claim_case_detail")
    res.write.mode(SaveMode.Append).saveAsTable("dwdb.dw_guzhu_claim_case_detail")
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
    val odsPolicyDetailTemp: DataFrame = hiveContext.sql("select policy_id,holder_name,insured_subject,big_policy,policy_status,policy_start_date,policy_end_date from odsdb.ods_policy_detail")
      .where("policy_status in (1,0,-1)")

    //读取被保人表
    val odsPolicyInsuredDetail = hiveContext.sql("select policy_id as id,insured_cert_no,start_date,end_date,work_type,job_company, gender from odsdb.ods_policy_insured_detail")

    // 关联被保人表
    val policyAndInsured = odsPolicyDetailTemp.join(odsPolicyInsuredDetail, 'policy_id === 'id, "leftouter")
      .selectExpr("policy_id", "big_policy", "holder_name", "insured_subject", "policy_status", "policy_start_date",
        "policy_end_date", "insured_cert_no", "start_date", "end_date", "work_type", "job_company", "gender")
    policyAndInsured.registerTempTable("policyAndInsuredTemp")


    //读取理赔表的数据,判断出险日期在他所在保单的承包日期中
    val res1 = hiveContext.sql("select id,case_no,policy_id,risk_date,report_date,product_code,risk_name,risk_cert_no,case_status,disable_level," +
      "case_type,hos_benefits,medical_coverage,pre_com,disable_death_payment,delay_payment,case_close_date,final_payment from dwdb.dw_policy_claim_detail")

    //println(res1.count())

    val res2 = hiveContext.sql("select policy_id as id_temp,insured_cert_no,start_date,big_policy,end_date,work_type,job_company,gender from policyAndInsuredTemp")


    val res3 = res2.join(res1, 'policy_id === 'id_temp and 'risk_cert_no === 'insured_cert_no)
      .where("start_date is not null")
      .selectExpr("id", "risk_date", "start_date", "end_date", "work_type", "job_company", "gender", "big_policy")
      .map(x => {
        val id = x.getAs[String]("id")
        val riskDate = x.getAs[String]("risk_date")
        val startDate = x.getAs[Timestamp]("start_date")
        val endDate = x.getAs[Timestamp]("end_date")
        val workType = x.getAs[String]("work_type")
        val jobCompany = x.getAs[String]("job_company")
        val gender = x.getAs[Int]("gender")

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
        (id, riskDateRes, startDate, endDate, workType, jobCompany, gender)
      }).toDF("id", "risk_date", "start_date", "end_date", "work_type", "job_company", "gender")

    val res4 = res3.selectExpr("id", "risk_date as risk_date_salve", "start_date", "end_date", "work_type", "job_company", "gender",
      "case when risk_date is null then 1 when risk_date >= substring(cast(start_date as string),1,10) and risk_date <= substring(cast(end_date as string),1,10) then 1 else 0 end as tem")
      .where("tem = 1")

    val res5 = res4.selectExpr("id as id_salve", "start_date", "end_date", "work_type", "job_company", "gender")

    //拿到所有的案件号，不管出险日期在不在保单的状态中
    val dwPolicyClaimDetailTemp = res1.join(res5, 'id === 'id_salve, "leftouter")
      .selectExpr("case_no", "policy_id", "risk_date", "report_date", "product_code", "risk_name", "risk_cert_no", "case_status", "disable_level",
        "case_type", "hos_benefits", "medical_coverage", "pre_com", "case_close_date", "disable_death_payment", "delay_payment", "final_payment",
        "start_date", "end_date", "work_type", "job_company", "gender").distinct()

    //读取bzn工种表
    val odsWorkMatchingTemp: DataFrame = hiveContext.sql("select primitive_work,work_name_check,work_name from odsdb.ods_work_matching_dimension")

    //读取标准工种表 如果bzn_work_name 重复 拿最小的bzn_work_risk,取gs_work_risk的第一个,如果字段为空串,替换成-1
    val resTemp = hiveContext.sql("SELECT bzn_work_name,bzn_work_risk,gs_work_risk from odsdb.ods_work_risk_dimension")
      .where("gs_work_risk is not null")
      .map(x => {
        val bznWorkName = x.getAs[String]("bzn_work_name")
        val bznWorkRisk = x.getAs[String]("bzn_work_risk")
        val gsWrokRisk = x.getAs[String]("gs_work_risk")
        val res = if (gsWrokRisk == "" || gsWrokRisk == null) -1 else if
        (gsWrokRisk != null && gsWrokRisk.split("|")(0) == "S") 0
        else {
          val restemp = gsWrokRisk.split("|")(0).toInt
          restemp
        }

        (bznWorkName, bznWorkRisk, res)

      }).toDF("bzn_work_name", "bzn_work_risk", "gs_work_risk")

    resTemp.registerTempTable("ods_work_risk_dimension_Temp")

    val odsWorkRiskDimension = hiveContext.sql("select bzn_work_name as name, min(bzn_work_risk) as risk," +
      "min(gs_work_risk) as gs_work_risk from ods_work_risk_dimension_Temp group by bzn_work_name")

    val odsWorkMatching = odsWorkMatchingTemp.join(odsWorkRiskDimension, 'work_name_check === 'name, "leftouter")
      .selectExpr("primitive_work", "work_name_check", "work_name", "name", "risk")

    val dwWorkTypeMatchingDetail = dwPolicyClaimDetailTemp.join(odsWorkMatching, 'work_type === 'primitive_work, "leftouter")
      .selectExpr("case_no", "policy_id", "risk_date", "report_date", "product_code", "risk_name", "risk_cert_no",
        "case_status", "disable_level", "case_type", "hos_benefits", "medical_coverage", "pre_com", "case_close_date",
        "delay_payment", "disable_death_payment", "final_payment", "work_type", "job_company", "gender", "start_date",
        "primitive_work", "work_name_check", "work_name", "name", "risk")

    //读取雇主基础信息表
    val dwEmployerBaseInfoDetail = hiveContext.sql("select policy_id as policy_id_salve,product_name,policy_start_date,policy_end_date,holder_name," +
      "insure_subject, insured_cert_no,start_date  as start_date_temp,end_date,channel_name,insure_company_name,insure_company_short_name,sale_name," +
      "team_name,biz_operator,sku_coverage,sku_append,sku_ratio,sku_price,sku_charge_type from dwdb.dw_employer_baseinfo_person_detail")

    //匹配表 关联 雇主基础信息表

    //上层结果关联工种表
    val resTempSalve = dwWorkTypeMatchingDetail.join(dwEmployerBaseInfoDetail, 'policy_id === 'policy_id_salve and 'risk_cert_no === 'insured_cert_no and 'start_date === 'start_date_temp, "leftouter")
      .selectExpr(
        "case_no",
        "policy_id",
        "risk_date",
        "report_date",
        "risk_name",
        "risk_cert_no",
        "gender",
        "cast(SUBSTRING(risk_date,1,4) as int) - cast(SUBSTRING(risk_cert_no,7,4) as int ) as age",
        "concat(SUBSTRING(risk_cert_no,9,1),'0后') as age_time",
        "case when SUBSTRING(risk_cert_no,11,4) between '0120' and '0218' then '水瓶座' when SUBSTRING(risk_cert_no,11,4) between '0219' and '0320' then '双鱼座' when SUBSTRING(risk_cert_no,11,4) between '0321' and '0419' then '白羊座' when SUBSTRING(risk_cert_no,11,4) between '0420' and '0520' then '金牛座' when SUBSTRING(risk_cert_no,11,4) between '0521' and '0621' then '双子座'  when SUBSTRING(risk_cert_no,11,4) between '0622' and '0722' then '巨蟹座' when SUBSTRING(risk_cert_no,11,4) between '0723' and '0822' then '狮子座' when SUBSTRING(risk_cert_no,11,4) between '0823' and '0922' then '处女座' when SUBSTRING(risk_cert_no,11,4) between '0923' and '1023' then '天秤座' when SUBSTRING(risk_cert_no,11,4) between '1024' and '1122' then '天蝎座' when SUBSTRING(risk_cert_no,11,4) between '1123' and '1221' then '射手座' else '摩羯座' end as constellation",
        "case SUBSTRING(risk_cert_no,1,2) when '11' then '北京市' when '12' then '天津市' when '13' then '河北省' when '14' then '山西省' when '15' then '内蒙古自治区' when '21' then '辽宁省' when '22' then '吉林省' when '23' then '黑龙江省' when '31' then '上海市' when '32' then '江苏省' when '33' then '浙江省' when '34' then '安徽省' when '35' then '福建省' when '36' then '江西省' when '37' then '山东省' when '41' then '河南省' when '42' then '湖北省' when '43' then '湖南省' when '44' then '广东省' when '45' then '广西壮族自治区' " +
          "when '46' then '海南省' when '50' then '重庆市' when '51' then '四川省' when '52' then '贵州省' when '53' then '云南省' when '54' then '西藏自治区' when '61' then '陕西省' when '62' then '甘肃省' " +
          "when '63' then '青海省' when '64' then '宁夏回族自治区' when '65' then '新疆维吾尔自治区' when '71' then '台湾省' when '81' then '香港特别行政区' when '82' then '澳门特别行政区' " +
          "else '未知' end  as province ",
        "null as city",
        "null as county",
        "work_name",
        "risk",
        "risk as insure_work_risk",
        "job_company",
        "holder_name",
        "insure_subject",
        "channel_name",
        "sale_name",
        "biz_operator",
        "insure_company_name",
        "insure_company_short_name",
        "product_name",
        "null as business_line",
        "null as big_policy",
        "sku_coverage",
        "sku_append",
        "sku_ratio",
        "sku_price",
        "sku_charge_type",
        "null as extend_time",
        "null as extend_hospital",
        "case_status",
        "pre_com",
        "disable_level",
        "case_type",
        "case_close_date",
        "hos_benefits",
        "medical_coverage",
        "delay_payment",
        "disable_death_payment",
        "final_payment",
        "case when final_payment is null then pre_com else pre_com end as res_pay",
        "null as hospital")

    val res = resTempSalve.
      selectExpr("case_no",
        "policy_id",
        "risk_date",
        "report_date",
        "risk_name",
        "risk_cert_no",
        "gender",
        "age",
        "age_time",
        "constellation",
        "province",
        "null as city",
        "null as county",
        "work_name",
        "risk",
        "risk as insure_work_risk",
        "job_company",
        "holder_name",
        "insure_subject",
        "channel_name",
        "sale_name",
        "biz_operator",
        "insure_company_name",
        "insure_company_short_name",
        "product_name",
        "null as business_line",
        "big_policy",
        "sku_coverage",
        "sku_append",
        "sku_ratio",
        "sku_price",
        "sku_charge_type",
        "null as extend_time",
        "null as extend_hospital",
        "case_status",
        "pre_com",
        "disable_level",
        "case_type",
        "case_close_date",
        "hos_benefits",
        "medical_coverage",
        "delay_payment",
        "disable_death_payment",
        "final_payment",
        "res_pay",
        "null as hospital", "case when res_pay <15000 then '一类' when res_pay>15000 and res_pay<50000 then '二类' when res_pay >= 50000 and res_pay <300000 then '三类' " +
          "when res_pay >=300000 and res_pay <800000 then '四类' when res_pay >=800000 then '五类' end as case_level").distinct()
    res

  }


}