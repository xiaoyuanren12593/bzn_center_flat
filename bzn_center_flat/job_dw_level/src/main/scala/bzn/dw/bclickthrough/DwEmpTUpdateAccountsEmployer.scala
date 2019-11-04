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
*/ object DwEmpTUpdateAccountsEmployer extends SparkUtil with Until{

  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "")
    val sc = sparkConf._2
    val hqlContext = sparkConf._4
    val res = UpdateMessage(hqlContext)

     hqlContext.sql("truncate table dwdb.dw_t_update_accounts_employer_detail")

     res.repartition(10).write.mode(SaveMode.Append).saveAsTable("dwdb.dw_t_update_accounts_employer_detail")

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

    val dwTAccountsEmployerDetail = hqlContext.sql("select id,batch_no,policy_no,preserve_id,add_batch_code,del_batch_code,preserve_status,data_source,project_name,product_code,product_name,channel_name as channel_name," +
      "business_owner,business_region,business_source,business_type,performance_accounting_day,operational_name,holder_name,insurer_name,plan_price,plan_coverage," +
      "plan_append,plan_disability_rate,plan_pay_type,underwriting_company,policy_effect_date,policy_start_time,policy_effective_time,policy_expire_time,policy_status,premium_total," +
      "premium_pay_status,premium_invoice_type,economy_company,economy_rates,economy_fee,technical_service_rates,technical_service_fee,consulting_service_rates," +
      "consulting_service_fee,service_fee_check_time,service_fee_check_status,has_brokerage,brokerage_ratio,brokerage_fee,brokerage_pay_status,remake,create_time," +
      "update_time,operator from dwdb.dw_t_accounts_employer_detail")


    /**
      * 读取销售渠道表
      */

    val odsGuzhuSalemanDetail = hqlContext.sql("select ent_name as ent_name_salve,channel_name as channel_name_salve,biz_operator as biz_operator_salve," +
      "salesman as salesman_salve,business_source as business_source_salve from odsdb.ods_ent_guzhu_salesman_detail")

    /**
      * 读取业务表,拿到保单号和要更新的渠道
      */
    val dwTAccountsEmployerDetailTemp = hqlContext.sql("select policy_no as policy_no_temp,holder_name as holder_name_temp," +
      "channel_name as channel_name_temp,operational_name as operational_name_temp,business_owner as business_owner_temp,business_source as business_source_temp from dwdb.dw_t_accounts_employer_detail")

    //更新业务表里面的渠道信息字段

    val updateChannelNameTemp = dwTAccountsEmployerDetailTemp.join(odsGuzhuSalemanDetail, 'holder_name_temp === 'ent_name_salve, "leftouter")
      .selectExpr("policy_no_temp", "holder_name_temp","if(channel_name_salve = '直客',ent_name_salve,channel_name_salve) as channel_name_salve","channel_name_temp")
      .where("holder_name_temp !='' and (channel_name_temp = '' or channel_name_temp is null)")

    //更新渠道信息

    val updateChannelName = dwTAccountsEmployerDetail.join(updateChannelNameTemp, 'policy_no === 'policy_no_temp, "leftouter")
      .selectExpr("id", "batch_no", "policy_no", "preserve_id", "add_batch_code", "del_batch_code","preserve_status", "data_source", "project_name", "product_code", "product_name",
        "if(channel_name_salve is null,channel_name_temp,channel_name_salve) as channel_name",
        "business_owner", "business_region", "business_source", "business_type", "performance_accounting_day", "operational_name", "holder_name", "insurer_name",
        "plan_price", "plan_coverage", "plan_append", "plan_disability_rate", "plan_pay_type", "underwriting_company",
        "policy_effect_date","policy_start_time", "policy_effective_time", "policy_expire_time", "policy_status", "premium_total", "premium_pay_status", "premium_invoice_type", "economy_company",
        "economy_rates", "economy_fee", "technical_service_rates", "technical_service_fee", "consulting_service_rates", "consulting_service_fee", "service_fee_check_time",
        "service_fee_check_status", "has_brokerage", "brokerage_ratio", "brokerage_fee", "brokerage_fee", "brokerage_pay_status", "remake", "create_time", "update_time", "operator")


    //更新业务表里面的运营字段
    val updateBizOperatorTemp = dwTAccountsEmployerDetailTemp.join(odsGuzhuSalemanDetail, 'holder_name_temp === 'ent_name_salve, "leftouter")
      .selectExpr("policy_no_temp", "operational_name_temp", "holder_name_temp", "biz_operator_salve")
      .where("holder_name_temp !='' and (operational_name_temp='' or operational_name_temp is null)")

    //更新业务表得运营信息
    val updateBizOperator = updateChannelName.join(updateBizOperatorTemp, 'policy_no === 'policy_no_temp, "leftouter")
      .selectExpr("id", "batch_no", "policy_no", "preserve_id", "add_batch_code", "del_batch_code","preserve_status", "data_source", "project_name", "product_code", "product_name",
        "channel_name",
        "business_owner", "business_region", "business_source", "business_type", "performance_accounting_day", "if(biz_operator_salve is null,operational_name_temp,biz_operator_salve) as operational_name", "holder_name", "insurer_name",
        "plan_price", "plan_coverage", "plan_append", "plan_disability_rate", "plan_pay_type", "underwriting_company",
        "policy_effect_date","policy_start_time", "policy_effective_time", "policy_expire_time", "policy_status", "premium_total", "premium_pay_status", "premium_invoice_type", "economy_company",
        "economy_rates", "economy_fee", "technical_service_rates", "technical_service_fee", "consulting_service_rates", "consulting_service_fee", "service_fee_check_time",
        "service_fee_check_status", "has_brokerage", "brokerage_ratio", "brokerage_fee", "brokerage_fee", "brokerage_pay_status", "remake", "create_time", "update_time", "operator")


    //更新业务表里面的销售信息
    val updateSalemanTemp = dwTAccountsEmployerDetailTemp.join(odsGuzhuSalemanDetail, 'holder_name_temp === 'ent_name_salve, "leftouter")
      .selectExpr("policy_no_temp", "business_owner_temp", "holder_name_temp", "salesman_salve")
      .where("holder_name_temp !='' and (business_owner_temp = '' or business_owner_temp is null)")


    //更新销售信息

    val updateSaleman = updateBizOperator.join(updateSalemanTemp, 'policy_no === 'policy_no_temp, "leftouter")
      .selectExpr("id", "batch_no", "policy_no", "preserve_id", "add_batch_code", "del_batch_code","preserve_status", "data_source", "project_name", "product_code", "product_name",
        "channel_name",
        "if(salesman_salve is null,business_owner_temp,salesman_salve) as business_owner", "business_region", "business_source", "business_type", "performance_accounting_day", "operational_name", "holder_name", "insurer_name",
        "plan_price", "plan_coverage", "plan_append", "plan_disability_rate", "plan_pay_type", "underwriting_company",
        "policy_effect_date","policy_start_time", "policy_effective_time", "policy_expire_time", "policy_status", "premium_total", "premium_pay_status", "premium_invoice_type", "economy_company",
        "economy_rates", "economy_fee", "technical_service_rates", "technical_service_fee", "consulting_service_rates", "consulting_service_fee", "service_fee_check_time",
        "service_fee_check_status", "has_brokerage", "brokerage_ratio", "brokerage_fee", "brokerage_fee", "brokerage_pay_status", "remake", "create_time", "update_time", "operator")


    //更细业务表的业务来源信息
    val updateSourceTemp = dwTAccountsEmployerDetailTemp.join(odsGuzhuSalemanDetail, 'holder_name_temp === 'ent_name_salve, "leftouter")
      .selectExpr("policy_no_temp", "business_source_temp", "holder_name_temp", "business_source_salve")
      .where("holder_name_temp !='' and (business_source_temp = '' or business_source_temp is null)")

    //更新业务来源信息
    val updateSource = updateSaleman.join(updateSourceTemp, 'policy_no === 'policy_no_temp, "leftouter")
      .selectExpr("id", "batch_no", "policy_no", "preserve_id", "add_batch_code", "del_batch_code","preserve_status", "data_source", "project_name", "product_code", "product_name",
        "channel_name",
        "business_owner", "business_region", "if(business_source_salve is null,business_source_temp,business_source_salve) as business_source", "business_type", "performance_accounting_day", "operational_name", "holder_name", "insurer_name",
        "plan_price", "plan_coverage", "plan_append", "plan_disability_rate", "plan_pay_type", "underwriting_company",
        "policy_effect_date","policy_start_time", "policy_effective_time", "policy_expire_time", "policy_status", "premium_total", "premium_pay_status", "premium_invoice_type", "economy_company",
        "economy_rates", "economy_fee", "technical_service_rates", "technical_service_fee", "consulting_service_rates", "consulting_service_fee", "service_fee_check_time",
        "service_fee_check_status", "has_brokerage", "brokerage_ratio", "brokerage_fee", "brokerage_fee", "brokerage_pay_status", "remake", "create_time", "update_time", "operator")


    //更新保单状态信息

    //读取保单信息
    val odsPolicyDetail = hqlContext.sql("select policy_code,cast(policy_status as string) as policy_status_salve from odsdb.ods_policy_detail")

    //更新业务表的保单状态信息
    val updatePolicyStatusTemp = dwTAccountsEmployerDetailTemp.join(odsPolicyDetail, 'policy_no_temp === 'policy_code, "leftouter")
      .selectExpr("policy_no_temp", "policy_status_salve")
      .where("policy_no_temp is not null")


    val updatePolicyStatus = updateSource.join(updatePolicyStatusTemp, 'policy_no === 'policy_no_temp, "leftouter")
      .selectExpr(
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
 updatePolicyStatus


  }


}