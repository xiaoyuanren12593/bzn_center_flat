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

object DwAegisCaseDetailTest extends SparkUtil with Until with MysqlUntil {

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
    val res1 = res.select("risk_cert_no")
    /* hiveContext.sql("truncate table dwdb.dw_guzhu_claim_case_detail")
      res.write.mode(SaveMode.Append).saveAsTable("dwdb.dw_guzhu_claim_case_detail")*/

    saveASMysqlTable(res,"ods_guzhu_claim_case_detail",
      SaveMode.Overwrite,"mysql.username","mysql.password","mysql.driver","mysql.url")


    sc.stop()
  }

  /**
   *
   * @param hiveContext
   * @return
   */

  def CaseDetail(hiveContext: HiveContext): DataFrame = {
    import hiveContext.implicits._
     hiveContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
     hiveContext.udf.register("clean", (str: String) => clean(str))
    //读取保单明细表
    val odsPolicyDetailTempSalve: DataFrame = hiveContext.sql("select policy_id,holder_name,insured_subject,big_policy,policy_status,policy_start_date,policy_end_date,product_code from odsdb.ods_policy_detail")
      .where("policy_status in (1,0,-1)")

    //读取产品表
    val odsProductDetail = hiveContext.sql("select product_code as product_code_salve,business_line from odsdb.ods_product_detail")

    val odsPolicyDetailTemp = odsPolicyDetailTempSalve.join(odsProductDetail, 'product_code === 'product_code_salve, "leftouter")
      .selectExpr("policy_id", "big_policy", "holder_name", "insured_subject", "policy_status", "policy_start_date",
        "policy_end_date", "business_line")

    //读取被保人表
    val odsPolicyInsuredDetail = hiveContext.sql("select policy_id as id,insured_cert_no,start_date,end_date,work_type,job_company, gender from odsdb.ods_policy_insured_detail")

    // 关联被保人表
    val policyAndInsured = odsPolicyDetailTemp.join(odsPolicyInsuredDetail, 'policy_id === 'id, "leftouter")
      .selectExpr("policy_id", "big_policy", "holder_name", "insured_subject", "policy_status", "policy_start_date",
        "policy_end_date", "insured_cert_no", "start_date", "end_date", "work_type", "job_company", "gender", "business_line")
    policyAndInsured.registerTempTable("policyAndInsuredTemp")

    //读取理赔表的数据,判断出险日期在他所在保单的承包日期中
    val res1 = hiveContext.sql("select id,case_no,policy_id,policy_code,risk_date,report_date,product_code,risk_name,risk_cert_no,case_status,disable_level," +
      "case_type,hos_benefits,medical_coverage,pre_com,disable_death_payment,delay_payment,case_close_date,final_payment from dwdb.dw_policy_claim_detail")

    val res2 = hiveContext.sql("select policy_id as id_temp,insured_cert_no,start_date,big_policy,end_date,work_type,job_company,gender,business_line from policyAndInsuredTemp")

    val res3 = res2.join(res1, 'policy_id === 'id_temp and 'risk_cert_no === 'insured_cert_no)
      .where("start_date is not null")
      .selectExpr("id", "risk_date", "start_date", "end_date", "work_type", "job_company", "gender", "big_policy", "business_line")
      .map(x => {
        val id = x.getAs[String]("id")
        val riskDate = x.getAs[String]("risk_date")
        val startDate = x.getAs[Timestamp]("start_date")
        val endDate = x.getAs[Timestamp]("end_date")
        val workType = x.getAs[String]("work_type")
        val jobCompany = x.getAs[String]("job_company")
        val gender = x.getAs[Int]("gender")
        val bigPolicy = x.getAs[Int]("big_policy")
        val businessLine = x.getAs[String]("business_line")
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
        (id, riskDateRes, startDate, endDate, workType, jobCompany, gender, bigPolicy, businessLine)
      }).toDF("id", "risk_date", "start_date", "end_date", "work_type", "job_company", "gender", "big_policy", "business_line")

    val res4 = res3.selectExpr("id", "risk_date", "start_date", "end_date", "work_type", "job_company", "gender", "big_policy", "business_line",
      "case when risk_date is null then 1 when risk_date >= substring(cast(start_date as string),1,10) and risk_date <= substring(cast(end_date as string),1,10) then 1 else 0 end as tem")
      .where("tem = 1")
 
    val res5 = res4.selectExpr("id as id_salve", "start_date", "end_date", "work_type", "job_company", "gender", "big_policy", "business_line")

    //拿到所有的案件号，不管出险日期在不在保单的状态中
    val dwPolicyClaimDetailTemp = res1.join(res5, 'id === 'id_salve, "leftouter")
      .selectExpr("policy_code", "case_no", "policy_id", "risk_date", "report_date", "product_code", "risk_name", "risk_cert_no", "case_status", "disable_level",
        "case_type", "hos_benefits", "medical_coverage", "pre_com", "case_close_date", "disable_death_payment", "delay_payment", "final_payment",
        "start_date", "end_date", "work_type", "job_company", "gender", "big_policy", "business_line").distinct()

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

    val dwWorkTypeMatchingDetailTemp = dwPolicyClaimDetailTemp.join(odsWorkMatching, 'work_type === 'primitive_work, "leftouter")
      .selectExpr("policy_code", "case_no", "policy_id", "risk_date", "report_date", "product_code", "risk_name", "risk_cert_no",
        "case_status", "disable_level", "case_type", "hos_benefits", "medical_coverage", "pre_com", "case_close_date",
        "delay_payment", "disable_death_payment", "final_payment", "work_type", "job_company", "gender", "start_date",
        "primitive_work", "work_name_check", "work_name", "name", "risk", "big_policy", "business_line")

    //读取方案类别表
    val odsWorkGradeDimension: DataFrame = hiveContext.sql("select policy_code as policy_code_temp,profession_type from odsdb.ods_work_grade_dimension")
      .map(x => {
        val policyCodTemp = x.getAs[String]("policy_code_temp")
        val professionType = x.getAs[String]("profession_type")
        val result = if (professionType != null && professionType.length > 0) {
          val res = professionType.replaceAll("类", "")
          if (res == "5") {
            (1, 5)
          } else {
            val sp1 = res.split("-")(0).toInt
            val sp2 = res.split("-")(1).toInt
            (sp1, sp2)
          }
        } else {
          (-1, -1)
        }
        (policyCodTemp, professionType, result._1, result._2)
      })
      .toDF("policy_code_temp", "profession_type", "profession_type_slow", "profession_type_high")
    val dwWorkTypeMatchingDetail = dwWorkTypeMatchingDetailTemp.join(odsWorkGradeDimension, 'policy_code === 'policy_code_temp, "leftouter")
      .selectExpr("case_no", "policy_id", "risk_date", "report_date", "product_code", "risk_name", "risk_cert_no",
        "case_status", "disable_level", "case_type", "hos_benefits", "medical_coverage", "pre_com", "case_close_date",
        "delay_payment", "disable_death_payment", "final_payment", "work_type", "job_company", "gender", "start_date",
        "primitive_work", "work_name_check", "work_name", "name", "risk", "big_policy", "profession_type", "business_line")

    //读取雇主基础信息表
    val dwEmployerBaseInfoDetail = hiveContext.sql("select policy_id as policy_id_salve,product_name,holder_name," +
      "insured_subject,channel_name,insure_company_name,insure_company_short_name,sale_name," +
      "team_name,biz_operator,sku_coverage,sku_append,sku_ratio,sku_price,sku_charge_type from dwdb.dw_employer_baseinfo_detail")

    //上层结果关联工种表
    val resTempSalve = dwWorkTypeMatchingDetail.join(dwEmployerBaseInfoDetail, 'policy_id === 'policy_id_salve, "leftouter")
      .selectExpr(
        "case_no",
        "policy_id",
        "case when risk_date is null then '' else risk_date end as risk_date",
        "case when report_date is null then '' else report_date end as report_date",
        "case when risk_name is null then '' else risk_name end as risk_name",
        "case when risk_cert_no is null then '' else risk_cert_no end as risk_cert_no",
        "case when SUBSTRING(risk_cert_no,17,1) in('1','3','5','7','9') then '男'  when SUBSTRING(risk_cert_no,17,1) in('0','2','4','6','8') then '女' else '未知' end as gender",
        "case when cast(SUBSTRING(risk_date,1,4) as int) - cast(SUBSTRING(risk_cert_no,7,4) as int ) is null " +
          "then '' else cast(SUBSTRING(risk_date,1,4) as int) - cast(SUBSTRING(risk_cert_no,7,4) as int ) end as age",
        "case when concat(SUBSTRING(risk_cert_no,9,1),'0后') is null then '' else concat(SUBSTRING(risk_cert_no,9,1),'0后') end as age_time",
        "case when SUBSTRING(risk_cert_no,11,4) between '0120' and '0218' then '水瓶座' when SUBSTRING(risk_cert_no,11,4) between '0219' and '0320' then '双鱼座' when SUBSTRING(risk_cert_no,11,4) between '0321' and '0419' then '白羊座' when SUBSTRING(risk_cert_no,11,4) between '0420' and '0520' then '金牛座' when SUBSTRING(risk_cert_no,11,4) between '0521' and '0621' then '双子座'  when SUBSTRING(risk_cert_no,11,4) between '0622' and '0722' then '巨蟹座' when SUBSTRING(risk_cert_no,11,4) between '0723' and '0822' then '狮子座' when SUBSTRING(risk_cert_no,11,4) between '0823' and '0922' then '处女座' when SUBSTRING(risk_cert_no,11,4) between '0923' and '1023' then '天秤座' when SUBSTRING(risk_cert_no,11,4) between '1024' and '1122' then '天蝎座' when SUBSTRING(risk_cert_no,11,4) between '1123' and '1221' then '射手座' when SUBSTRING(risk_cert_no,11,4) is null then '' else '摩羯座' end as constellation",
        "case SUBSTRING(risk_cert_no,1,2) when '11' then '北京市' when '12' then '天津市' when '13' then '河北省' when '14' then '山西省' when '15' then '内蒙古自治区' when '21' then '辽宁省' when '22' then '吉林省' when '23' then '黑龙江省' when '31' then '上海市' when '32' then '江苏省' when '33' then '浙江省' when '34' then '安徽省' when '35' then '福建省' when '36' then '江西省' when '37' then '山东省' when '41' then '河南省' when '42' then '湖北省' when '43' then '湖南省' when '44' then '广东省' when '45' then '广西壮族自治区' " +
          "when '46' then '海南省' when '50' then '重庆市' when '51' then '四川省' when '52' then '贵州省' when '53' then '云南省' when '54' then '西藏自治区' when '61' then '陕西省' when '62' then '甘肃省' " +
          "when '63' then '青海省' when '64' then '宁夏回族自治区' when '65' then '新疆维吾尔自治区' when '71' then '台湾省' when '81' then '香港特别行政区' when '82' then '澳门特别行政区'  else '未知' end  as province ",
        "clean('') as city",
        "clean('') as county",
        "case when work_type is null then '' else work_type end as work_type",
        "case when risk is null then '' else risk end as risk",
        "case when profession_type is null then '' else profession_type end as insure_work_risk",
        "case when job_company is null then '' else job_company end as job_company",
        "case when holder_name is null then '' else holder_name end as holder_name",
        "case when insured_subject is null then '' else insured_subject end as insured_subject",
        "case when channel_name is null then '' else channel_name end as channel_name",
        "case when sale_name is null then '' else sale_name end as sale_name",
        "case when biz_operator is null then '' else biz_operator end as biz_operator",
        "case when insure_company_name is null then '' else insure_company_name end as insure_company_name",
        "case when insure_company_short_name is null then '' else insure_company_short_name end as insure_company_short_name",
        "case when product_name is null then '' else product_name end as product_name",
        "case when business_line is null then '' else business_line end as business_line",
        "case when big_policy is null then '' else big_policy end as big_policy",
        "case when sku_coverage is null then 0 else sku_coverage end as sku_coverage",
        "case when sku_append is null then '' else sku_append end as sku_append",
        "case when sku_ratio is null then '' else sku_ratio end as sku_ratio",
        "case when sku_price is null then 0 else sku_price end as sku_price",
        "case when sku_charge_type is null then '' else sku_charge_type end as sku_charge_type",
        "clean('') as extend_time",
        "clean('') as extend_hospital",
        "case when case_status is null then '' else case_status end as case_status",
        "case when pre_com is null then '' else pre_com end as pre_com",
        "case when disable_level is null then '' else disable_level end as disable_level",
        "case when case_type is null then '' else case_type end as case_type",
        "case when case_close_date is null then '' else case_close_date end as case_close_date",
        "case when hos_benefits is null then 0 else hos_benefits end as hos_benefits",
        "case when medical_coverage is null then 0 else medical_coverage end as medical_coverage",
        "case when delay_payment is null then 0 else delay_payment end as delay_payment",
        "case when disable_death_payment is null then 0 else disable_death_payment end as disable_death_payment",
        "case when final_payment is null then 0 else final_payment end as final_payment",
        "case when final_payment is null then pre_com else final_payment end as res_pay",
        "clean('') as hospital").distinct()

    val res = resTempSalve.
      selectExpr(
        "getUUID() as id",
        "case_no",
        "policy_id",
        "risk_date",
        "report_date",
        "risk_name",
        "risk_cert_no",
        "case when gender is null then '' else gender end as sex",
        "age",
        "age_time",
        "constellation",
        "province",
        "city",
        "county",
        "work_type as work_name",
        "risk",
        "insure_work_risk",
        "job_company",
        "holder_name",
        "insured_subject as insure_subject",
        "channel_name",
        "sale_name",
        "biz_operator",
        "insure_company_name",
        "insure_company_short_name",
        "product_name",
        "business_line",
        "big_policy",
        "sku_coverage",
        "sku_append",
        "sku_ratio",
        "sku_price",
        "sku_charge_type",
        "extend_time",
        "extend_hospital",
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
        "hospital",
        "case when res_pay <15000 then '一类' when res_pay>=15000 and res_pay<50000 then '二类' when res_pay >= 50000 and res_pay <300000 then '三类' " +
          "when res_pay >=300000 and res_pay <800000 then '四类' when res_pay >=800000 then '五类' end as case_level")
    //res.printSchema()
    res

  }


}