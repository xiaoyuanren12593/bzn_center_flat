package bzn.dw.bclickthrough

import bzn.dw.util.SparkUtil
import bzn.job.common.Until
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext

/*
* @Author:liuxiang
* @Date：2019/11/4
* @Describe:
*/ object DwTAccountsEmployer extends SparkUtil with Until {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "")

    val sc = sparkConf._2
    val hqlContext = sparkConf._4
    val AddPolicyRes = TAccountsEmployerAddPolicy(hqlContext)


    AddPolicyRes.write.mode(SaveMode.Append).saveAsTable("dwdb.dw_t_accounts_employer_detail")
    val res1 = TAccountsEmployerAddPreserve(hqlContext)
    res1.write.mode(SaveMode.Append).saveAsTable("dwdb.dw_t_accounts_employer_detail")

    sc.stop()
  }


  /**
    * 业务表添加保单数据
    *
    * @param hqlContext
    */
  def TAccountsEmployerAddPolicy(hqlContext: HiveContext): DataFrame = {
    hqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    hqlContext.udf.register("clean", (str: String) => clean(str))
    import hqlContext.implicits._

    /**
      * 读取保单和批单的数据
      */
    val dwTaccountEmployerIntermeditae = hqlContext.sql("select distinct policy_no,batch_no,preserve_id,add_batch_code,del_batch_code,preserve_status,data_source,project_name,product_code,product_name,channel_name,business_owner,business_region,business_source," +
      "business_type,performance_accounting_day,operational_name,holder_name,insurer_name,plan_price,plan_coverage,plan_append,plan_disability_rate,plan_pay_type,underwriting_company,policy_effect_date,policy_effective_time,policy_start_time," +
      "policy_expire_time,policy_status,premium_total,premium_pay_status,premium_invoice_type,economy_company,economy_rates,economy_fee,technical_service_rates,technical_service_fee,consulting_service_rates,consulting_service_fee,service_fee_check_time," +
      "service_fee_check_status,has_brokerage,brokerage_ratio,brokerage_fee,brokerage_pay_status,remake,create_time,update_time,operator from dwdb.dw_t_accounts_employer_intermediate")

    /**
      * 读取业务表的数据
      */

    val dwTAccountsEmployerDetail = hqlContext.sql("select policy_no as policy_no_salve from dwdb.dw_t_accounts_employer_detail")


    /**
      * 关联两个表 过滤出保单数据的增量数据
      */
    val resTemp = dwTaccountEmployerIntermeditae.join(dwTAccountsEmployerDetail, 'policy_no === 'policy_no_salve, "leftouter")
      .selectExpr("policy_no", "batch_no","policy_no_salve", "preserve_id", "add_batch_code", "del_batch_code","preserve_status", "data_source", "project_name", "product_code", "product_name", "channel_name",
        "business_owner", "business_region", "business_source", "business_type", "performance_accounting_day", "operational_name", "holder_name", "insurer_name",
        "plan_price", "plan_coverage", "plan_append", "plan_disability_rate", "plan_pay_type", "underwriting_company",
        "policy_effect_date","policy_start_time", "policy_effective_time", "policy_expire_time", "policy_status", "premium_total", "premium_pay_status", "premium_invoice_type", "economy_company",
        "economy_rates", "economy_fee", "technical_service_rates", "technical_service_fee", "consulting_service_rates", "consulting_service_fee", "service_fee_check_time",
        "service_fee_check_status", "has_brokerage", "brokerage_ratio", "brokerage_fee", "brokerage_fee", "brokerage_pay_status", "remake", "create_time", "update_time", "operator")
      .where("add_batch_code is null and del_batch_code is null and policy_no_salve is null")


    //增量数据
    val res = resTemp.selectExpr(
      "getUUID() as id",
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
      "policy_effect_date","policy_start_time", "policy_effective_time", "policy_expire_time",
      "clean('') as cur_policy_status",
      "policy_status",
      "premium_total",
      "clean(premium_pay_status) as premium_pay_status",
      "clean('') as has_behalf",
      "clean('') as behalf_status",
      "clean(premium_invoice_type) as premium_invoice_type",
      "clean(economy_company) as economy_company",
      "economy_rates",
      "economy_fee",
      "technical_service_rates",
      "technical_service_fee",
      "consulting_service_rates",
      "consulting_service_fee",
      "service_fee_check_time",
      "service_fee_check_status",
      "clean(has_brokerage) as has_brokerage",
      "brokerage_ratio",
      "brokerage_fee",
      "clean(brokerage_pay_status)  as brokerage_pay_status",
      "clean(remake) as remake", "create_time", "update_time", "operator")
    res
  }


  /**
    * 业务表增加批单数据
    *
    * @param hqlContext
    */
  def TAccountsEmployerAddPreserve(hqlContext: HiveContext): DataFrame = {
    hqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    hqlContext.udf.register("clean", (str: String) => clean(str))
    import hqlContext.implicits._

    /**
      * 读取保单和批单的数据
      */
    val dwTaccountEmployerIntermeditae = hqlContext.sql("select distinct policy_no,batch_no,preserve_id,add_batch_code,del_batch_code,preserve_status,data_source,project_name,product_code,product_name,channel_name,business_owner,business_region,business_source," +
      "business_type,performance_accounting_day,operational_name,holder_name,insurer_name,plan_price,plan_coverage,plan_append,plan_disability_rate,plan_pay_type,underwriting_company,policy_effect_date,policy_effective_time,policy_start_time," +
      "policy_expire_time,policy_status,premium_total,premium_pay_status,premium_invoice_type,economy_company,economy_rates,economy_fee,technical_service_rates,technical_service_fee,consulting_service_rates,consulting_service_fee,service_fee_check_time," +
      "service_fee_check_status,has_brokerage,brokerage_ratio,brokerage_fee,brokerage_pay_status,remake,create_time,update_time,operator from dwdb.dw_t_accounts_employer_intermediate")

    /**
      * 读取业务表的数据
      */

    val dwTAccountsEmployerDetail = hqlContext.sql("select policy_no as policy_no_salve,preserve_id as preserve_id_salve from dwdb.dw_t_accounts_employer_detail")

    /**
      * 关联两个表 拿到批单数据的增量数据
      */
    val resTemp = dwTaccountEmployerIntermeditae.join(dwTAccountsEmployerDetail, 'policy_no === 'policy_no_salve and 'preserve_id === 'preserve_id_salve, "leftouter")
      .selectExpr("policy_no", "batch_no","policy_no_salve", "preserve_id", "add_batch_code", "del_batch_code","preserve_status", "data_source", "project_name", "product_code", "product_name", "channel_name",
        "business_owner", "business_region", "business_source", "business_type", "performance_accounting_day", "operational_name", "holder_name", "insurer_name",
        "plan_price", "plan_coverage", "plan_append", "plan_disability_rate", "plan_pay_type", "underwriting_company",
        "policy_effect_date","policy_start_time", "policy_effective_time", "policy_expire_time", "policy_status", "premium_total", "premium_pay_status", "premium_invoice_type", "economy_company",
        "economy_rates", "economy_fee", "technical_service_rates", "technical_service_fee", "consulting_service_rates", "consulting_service_fee", "service_fee_check_time",
        "service_fee_check_status", "has_brokerage", "brokerage_ratio", "brokerage_fee", "brokerage_fee", "brokerage_pay_status", "remake", "create_time", "update_time", "operator")
      .where("preserve_id is not null and policy_no_salve is null")


    val res= resTemp.selectExpr(
      "getUUID() as id",
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
      "policy_effect_date","policy_start_time", "policy_effective_time", "policy_expire_time",
      "clean('') as cur_policy_status",
      "policy_status",
      "premium_total",
      "clean(premium_pay_status) as premium_pay_status",
      "clean('') as has_behalf",
      "clean('') as behalf_status",
      "clean(premium_invoice_type) as premium_invoice_type",
      "clean(economy_company) as economy_company",
      "economy_rates",
      "economy_fee",
      "technical_service_rates",
      "technical_service_fee",
      "consulting_service_rates",
      "consulting_service_fee",
      "service_fee_check_time",
      "service_fee_check_status",
      "clean(has_brokerage) as has_brokerage",
      "brokerage_ratio",
      "brokerage_fee",
      "clean(brokerage_pay_status)  as brokerage_pay_status",
      "clean(remake) as remake", "create_time", "update_time", "operator")
    res


  }




}
