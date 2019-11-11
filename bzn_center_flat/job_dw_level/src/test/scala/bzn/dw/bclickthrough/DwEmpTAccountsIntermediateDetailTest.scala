package bzn.dw.bclickthrough


import java.text.SimpleDateFormat
import java.util.Date

import bzn.dw.bclickthrough.DwEmpTAccountsIntermediateDetail.{clean, sparkConfInfo}
import bzn.dw.util.SparkUtil
import bzn.job.common.{MysqlUntil, Until}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext


/*
* @Author:liuxiang
* @Date：2019/10/29
* @Describe:
*/ object DwEmpTAccountsIntermediateDetailTest extends SparkUtil with Until {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "local[*]")

    val sc = sparkConf._2
    val hqlContext = sparkConf._4
    val AddPolicyRes = TAccountsEmployerAddPolicy(hqlContext)
    hqlContext.sql("truncate table dwdb.dw_t_accounts_employer_test_detail")
    AddPolicyRes.repartition(10).write.mode(SaveMode.Append).saveAsTable("dwdb.dw_t_accounts_employer_test_detail")
    sc.stop()

  }


  /**
    * 添加保单数据
    *
    * @param hqlContext
    */
  def TAccountsEmployerAddPolicy(hqlContext: HiveContext): DataFrame = {
    import hqlContext.implicits._
    hqlContext.udf.register("clean", (str: String) => clean(str))
    hqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    hqlContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

      //设置日期格式
      val date = df.format(new Date()) // new Date()为获取当前系统时间
      date + ""
    })
    /**
      * 读取保单明细表
      */

    val odsPolicyDetail = hqlContext.sql("select distinct policy_code,case source_system when '1.0' then '1' when '2.0' then '2' end as data_source," +
      "policy_status,policy_effect_date,policy_start_date,policy_end_date,product_code,insure_company_name,f" +
      "irst_premium,holder_name,insured_subject,invoice_type,preserve_policy_no,policy_create_time from odsdb.ods_policy_detail")

    /**
      * 读取客户归属表
      */

    val odsEntGuzhuDetail = hqlContext.sql("select salesman,ent_name,biz_operator,channel_name,business_source from odsdb.ods_ent_guzhu_salesman_detail")

    /**
      * 保单明细表关联客户归属信息表
      */
    val policyAndGuzhuRes = odsPolicyDetail.join(odsEntGuzhuDetail, 'holder_name === 'ent_name, "leftouter")
      .selectExpr("policy_code", "product_code", "data_source", "policy_status", "policy_effect_date", "policy_start_date", "policy_end_date", "insure_company_name", "first_premium",
        "holder_name", "insured_subject", "invoice_type", "salesman", "ent_name", "channel_name", "biz_operator", "business_source", "preserve_policy_no", "policy_create_time")

    /**
      * 读取销售团队表
      */

    val odsEntSalesTeam = hqlContext.sql("select team_name,sale_name from odsdb.ods_ent_sales_team_dimension")

    /**
      * 将结果关联销售团队表
      */

    val policyAndGuzhuSalve = policyAndGuzhuRes.join(odsEntSalesTeam, 'salesman === 'sale_name, "leftouter")
      .selectExpr("policy_code", "product_code", "data_source", "policy_status", "policy_effect_date", "policy_start_date", "policy_end_date",
        "insure_company_name", "first_premium", "holder_name", "insured_subject", "invoice_type", "salesman", "team_name", "ent_name", "channel_name", "biz_operator", "business_source", "preserve_policy_no", "policy_create_time")

    /**
      * 读取方案类别表
      */

    val odsPolicyProductPlanDetail = hqlContext.sql("select policy_code as policy_code_salve,sku_price,sku_ratio,sku_append,sku_coverage,economic_rate,tech_service_rate,sku_charge_type,commission_discount_rate from odsdb.ods_policy_product_plan_detail")


    /**
      * 将上述结果关联方案类别表
      */
    val policyAndProductPlan = policyAndGuzhuSalve.join(odsPolicyProductPlanDetail, 'policy_code === 'policy_code_salve, "leftouter")
      .selectExpr("policy_code", "product_code", "data_source", "policy_status", "policy_effect_date", "policy_start_date", "policy_end_date",
        "insure_company_name", "first_premium", "holder_name", "insured_subject", "invoice_type", "salesman", "team_name", "ent_name", "channel_name", "biz_operator", "business_source",
        "sku_price", "sku_ratio", "sku_append", "sku_coverage", "economic_rate", "tech_service_rate", "sku_charge_type", "preserve_policy_no", "commission_discount_rate", "policy_create_time")

    /**
      * 读取产品表
      */
    val odsProductDetail = hqlContext.sql("select product_desc,product_code as insure_code,product_name,two_level_pdt_cate from odsdb.ods_product_detail")


    /**
      * 关联产品表
      */
    val resTempRes = policyAndProductPlan.join(odsProductDetail, 'product_code === 'insure_code, "leftouter")
      .selectExpr("policy_code", "product_code", "product_desc", "product_name", "two_level_pdt_cate", "data_source", "policy_status", "policy_effect_date", "policy_start_date", "policy_end_date",
        "insure_company_name", "first_premium", "holder_name", "insured_subject", "invoice_type", "salesman", "team_name", "ent_name", "channel_name", "biz_operator", "business_source", "sku_price", "sku_ratio", "sku_append", "sku_coverage",
        "economic_rate",
        "tech_service_rate", "sku_charge_type", "preserve_policy_no", "commission_discount_rate", "policy_create_time")
      .where("policy_code !='' and policy_status in(0,1) and policy_start_date >=cast('2019-01-01' as timestamp) and two_level_pdt_cate in ('外包雇主', '骑士保', '大货车', '零工保')")


    val res = resTempRes.selectExpr(
      "getUUID() as id",
      "'' as batch_no",
      "policy_code as policy_no",
      "'' as preserve_id",
      "'' as add_batch_code",
      "'' as del_batch_code",
      "'' as preserve_status",
      "data_source",
      "product_desc as project_name",
      "product_code",
      "product_name",
      "if(trim(channel_name)='直客',trim(ent_name),trim(channel_name)) as channel_name",
      "salesman as business_owner",
      "team_name as business_region",
      "business_source",
      "cast(if(preserve_policy_no is null,1,2) as string) as business_type",
      "if(policy_start_date >=policy_create_time,policy_start_date,policy_create_time) as performance_accounting_day",
      "biz_operator as operational_name",
      "holder_name",
      "insured_subject as insurer_name",
      "sku_price as plan_price",
      "sku_coverage as plan_coverage",
      "sku_append as plan_append",
      "cast(if(sku_ratio is null,0,if(sku_ratio = 1, 0.05,if(sku_ratio = 2, 0.1,sku_ratio))) as decimal(14,4)) as plan_disability_rate",
      "if(two_level_pdt_cate = '零工保','3',sku_charge_type) as plan_pay_type",
      "insure_company_name as underwriting_company",
      "policy_effect_date",
      "cast('' as timestamp) as policy_start_time",
      "policy_start_date as policy_effective_time",
      "policy_end_date as policy_expire_time",
      "cast(policy_status as string) as policy_status",
      "first_premium as premium_total",
      "'' as premium_pay_status",
      "'' as has_behalf",
      "'' as behalf_status",
      "'天津中策' as economy_company",
      "cast(invoice_type as string) as premium_invoice_type",
      "economic_rate as economy_rates",
      "cast(first_premium * economic_rate as decimal(14,4)) as economy_fee",
      "tech_service_rate as technical_service_rates",
      "cast(first_premium * tech_service_rate as decimal(14,4)) as technical_service_fee",
      "cast('' as decimal(14,4)) as consulting_service_rates",
      "cast('' as decimal(14,4)) as consulting_service_fee",
      "cast('' as timestamp) as service_fee_check_time",
      "'' as cur_policy_status",
      "'' as service_fee_check_status",
      "'' as has_brokerage",
      "case when commission_discount_rate is  null then 0 else commission_discount_rate end as brokerage_ratio",
      "cast(case when (first_premium * commission_discount_rate) is not null then (first_premium * commission_discount_rate) else 0 end as decimal(14,4)) as brokerage_fee",
      "'' as brokerage_pay_status",
      "'' as remake",
      "cast(getNow() as timestamp) as create_time",
      "cast(getNow() as timestamp) as update_time",
      "cast('' as int) as operator")


    /**
      * 将结果注册成临时表
      */
    res.registerTempTable("t_accounts_employer_intermediate_temp")

    /**
      * 读取保单和批单的数据
      */
    val dwTaccountEmployerIntermeditae = hqlContext.sql("select distinct id,policy_no,batch_no,preserve_id,add_batch_code,del_batch_code,preserve_status,data_source,project_name,product_code,product_name,channel_name,business_owner,business_region,business_source," +
      "business_type,performance_accounting_day,operational_name,holder_name,insurer_name,plan_price,plan_coverage,plan_append,plan_disability_rate,plan_pay_type,underwriting_company,policy_effect_date,policy_effective_time,policy_start_time," +
      "policy_expire_time,cur_policy_status,policy_status,premium_total,premium_pay_status,has_behalf,behalf_status,premium_invoice_type,economy_company,economy_rates,economy_fee,technical_service_rates,technical_service_fee,consulting_service_rates,consulting_service_fee,service_fee_check_time," +
      "service_fee_check_status,has_brokerage,brokerage_ratio,brokerage_fee,brokerage_pay_status,remake,create_time,update_time,operator from t_accounts_employer_intermediate_temp")

    /**
      * 读取业务表的数据
      */

    val dwTAccountsEmployerDetailTemp = hqlContext.sql("select * from dwdb.dw_t_accounts_employer_detail").cache()

    val dwTAccountsEmployerDetail = dwTAccountsEmployerDetailTemp.selectExpr("policy_no as policy_no_salve").cache()


    /**
      * 关联两个表 过滤出保单数据的增量数据
      */
    val resTempResfi = dwTaccountEmployerIntermeditae.join(dwTAccountsEmployerDetail, 'policy_no === 'policy_no_salve, "leftouter")
      .selectExpr("id","policy_no", "batch_no", "policy_no_salve", "preserve_id", "add_batch_code", "del_batch_code", "preserve_status", "data_source", "project_name", "product_code", "product_name", "channel_name",
        "business_owner", "business_region", "business_source", "business_type", "performance_accounting_day", "operational_name", "holder_name", "insurer_name",
        "plan_price", "plan_coverage", "plan_append", "plan_disability_rate", "plan_pay_type", "underwriting_company",
        "policy_effect_date", "policy_start_time", "policy_effective_time", "policy_expire_time","cur_policy_status", "policy_status", "premium_total", "premium_pay_status","has_behalf","behalf_status", "premium_invoice_type", "economy_company",
        "economy_rates", "economy_fee", "technical_service_rates", "technical_service_fee", "consulting_service_rates", "consulting_service_fee", "service_fee_check_time",
        "service_fee_check_status", "has_brokerage", "brokerage_ratio", "brokerage_fee", "brokerage_pay_status", "remake", "create_time", "update_time", "operator")
      .where("policy_no_salve is null")

    val resTemp = resTempResfi.selectExpr("id",  "batch_no","policy_no","preserve_id", "add_batch_code", "del_batch_code", "preserve_status", "data_source", "project_name", "product_code", "product_name", "channel_name",
      "business_owner", "business_region", "business_source", "business_type", "performance_accounting_day", "operational_name", "holder_name", "insurer_name",
      "plan_price", "plan_coverage", "plan_append", "plan_disability_rate", "plan_pay_type", "underwriting_company",
      "policy_effect_date", "policy_start_time", "policy_effective_time", "policy_expire_time", "cur_policy_status", "policy_status", "premium_total", "premium_pay_status", "has_behalf","behalf_status", "premium_invoice_type", "economy_company",
      "economy_rates", "economy_fee", "technical_service_rates", "technical_service_fee", "consulting_service_rates", "consulting_service_fee", "service_fee_check_time",
      "service_fee_check_status", "has_brokerage", "brokerage_ratio", "brokerage_fee",  "brokerage_pay_status", "remake", "create_time", "update_time", "operator")


    val frame = dwTAccountsEmployerDetailTemp.unionAll(resTemp)

    //增量数据
    val res1 = frame.selectExpr(
      "id",
      "clean(batch_no) as batch_no",
      "clean(policy_no) as policy_no",
      "clean(preserve_id) as preserve_id",
      "clean(add_batch_code) as add_batch_code",
      "clean(del_batch_code) as del_batch_code",
      "clean(preserve_status) as preserve_status",
      "clean(data_source) as data_source",
      "clean(project_name) as project_name",
      "clean(product_code) as product_code",
      "clean(product_name) as product_name",
      "clean(channel_name) as channel_name",
      "clean(business_owner)  as business_owner",
      "clean(business_region) as business_region",
      "clean(business_source) as business_source",
      "clean(business_type) as business_type",
      "performance_accounting_day",
      "clean(operational_name) as operational_name",
      "clean(holder_name) as holder_name",
      "clean(insurer_name) as insurer_name",
      "plan_price",
      "plan_coverage",
      "clean(plan_append) as plan_append",
      "plan_disability_rate",
      "clean(plan_pay_type) as plan_pay_type",
      "clean(underwriting_company) as underwriting_company",
      "policy_effect_date",
      "policy_start_time",
      "policy_effective_time",
      "policy_expire_time",
      "clean(cur_policy_status) as cur_policy_status",
      "policy_status",
      "premium_total",
      "clean(premium_pay_status) as premium_pay_status",
      "clean(has_behalf) as has_behalf",
      "clean(behalf_status) as behalf_status",
      "clean(premium_invoice_type) as premium_invoice_type",
      "clean(economy_company) as economy_company",
      "economy_rates",
      "economy_fee",
      "technical_service_rates",
      "technical_service_fee",
      "consulting_service_rates",
      "consulting_service_fee",
      "service_fee_check_time",
      "clean(service_fee_check_status) as service_fee_check_status",
      "clean(has_brokerage) as has_brokerage",
      "brokerage_ratio",
      "brokerage_fee",
      "clean(brokerage_pay_status)  as brokerage_pay_status",
      "clean(remake) as remake", "create_time", "update_time",
      "operator")
    res1


  }


}