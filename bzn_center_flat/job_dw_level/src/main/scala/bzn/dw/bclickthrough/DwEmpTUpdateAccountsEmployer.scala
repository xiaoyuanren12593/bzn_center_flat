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
*/ object DwEmpTUpdateAccountsEmployer extends SparkUtil with Until {

  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "")
    val sc = sparkConf._2
    val hqlContext = sparkConf._4
    val res = UpdateMessage(hqlContext)
     hqlContext.sql("truncate table dwdb.dw_t_update_accounts_employer_detail")
    res.write.mode(SaveMode.Append).saveAsTable("dwdb.dw_t_update_accounts_employer_detail")

    sc.stop()
  }


  /**
    * 更新 渠道信息,运营信息,销售信息,业务来源信息,保单状态信息
    *
    * @param hqlContext
    */
  def UpdateMessage(hqlContext: HiveContext): DataFrame = {
    hqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    hqlContext.udf.register("clean", (str: String) => clean(str))
    import hqlContext.implicits._

    /**
      * 读取最新的业务表
      */

    val dwTAccountsEmployerDetail = hqlContext.sql("select * from dwdb.dw_t_accounts_employer_test_presever_detail")


    /**
      * 读取销售渠道表
      */

    val odsGuzhuSalemanDetail = hqlContext.sql("select ent_name as ent_name_salve,channel_name as channel_name_salve,biz_operator as biz_operator_salve," +
      "salesman as salesman_salve,business_source as business_source_salve from odsdb.ods_ent_guzhu_salesman_detail")


    //关联两个表,更新渠道信息
    val updateTemp = dwTAccountsEmployerDetail.join(odsGuzhuSalemanDetail, 'holder_name === 'ent_name_salve, "leftouter")
      .selectExpr(
        "id",
        "batch_no",
        "policy_no",
        "preserve_id",
        "add_batch_code",
        "del_batch_code",
        "preserve_status",
        "data_source",
        "project_name",
        "product_code",
        "product_name",
        "case when channel_name = '' or channel_name is null then if(channel_name_salve = '直客',ent_name_salve,channel_name_salve)  else channel_name end  as channel_name",
        "case when business_owner = '' or business_owner is null then salesman_salve else business_owner end as business_owner",
        "business_region",
        "case when business_source = '' or business_source is null then business_source_salve else business_source end as business_source",
        "business_type",
        "performance_accounting_day",
        "case when operational_name='' or operational_name is null then biz_operator_salve else operational_name end as operational_name",
        "holder_name",
        "insurer_name",
        "plan_price",
        "plan_coverage",
        "plan_append",
        "plan_disability_rate",
        "plan_pay_type",
        "underwriting_company",
        "policy_effect_date",
        "policy_start_time",
        "policy_effective_time",
        "policy_expire_time",
        "cur_policy_status",
        "policy_status",
        "premium_total",
        "premium_pay_status",
        "has_behalf",
        "behalf_status",
        "premium_invoice_type",
        "economy_company",
        "economy_rates",
        "economy_fee",
        "technical_service_rates",
        "technical_service_fee",
        "consulting_service_rates",
        "consulting_service_fee",
        "service_fee_check_time",
        "service_fee_check_status",
        "has_brokerage",
        "brokerage_ratio",
        "brokerage_fee",
        "brokerage_pay_status",
        "remake",
        "create_time",
        "update_time",
        "operator")
    //读取保单信息
    val odsPolicyDetail = hqlContext.sql("select  DISTINCT policy_code,cast(policy_status as string) as policy_status_salve from odsdb.ods_policy_detail")

    val update = updateTemp.join(odsPolicyDetail, 'policy_no === 'policy_code, "leftouter")
      .selectExpr(
        "id",
        "batch_no",
        "policy_no",
        "preserve_id",
        "add_batch_code",
        "del_batch_code",
        "preserve_status",
        "data_source",
        "project_name",
        "product_code",
        "product_name",
        "channel_name",
        "business_owner",
        "business_region",
        "business_source",
        "business_type",
        "performance_accounting_day",
        "operational_name",
        "holder_name",
        "insurer_name",
        "plan_price",
        "plan_coverage",
        "plan_append",
        "plan_disability_rate",
        "plan_pay_type",
        "underwriting_company",
        "policy_effect_date",
        "policy_start_time",
        "policy_effective_time",
        "policy_expire_time",
        "cur_policy_status",
        "case when policy_no is not null then policy_status_salve else policy_status end as policy_status",
        "premium_total",
        "premium_pay_status",
        "has_behalf",
        "behalf_status",
        "premium_invoice_type",
        "economy_company",
        "economy_rates",
        "economy_fee",
        "technical_service_rates",
        "technical_service_fee",
        "consulting_service_rates",
        "consulting_service_fee",
        "service_fee_check_time",
        "service_fee_check_status",
        "has_brokerage",
        "brokerage_ratio",
        "brokerage_fee",
        "brokerage_pay_status",
        "remake",
        "create_time",
        "update_time",
        "operator")
    update

  }
}