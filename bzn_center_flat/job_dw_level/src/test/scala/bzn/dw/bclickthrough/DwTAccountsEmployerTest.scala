package bzn.dw.bclickthrough

import bzn.dw.bclickthrough.DwEmpTAccountsIntermediateDetailTest.sparkConfInfo
import bzn.dw.util.SparkUtil
import bzn.job.common.Until
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.hive.HiveContext

/*
* @Author:liuxiang
* @Date：2019/10/31
* @Describe:
*/ object DwTAccountsEmployerTest extends  SparkUtil with Until{

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "local[*]")

    val sc = sparkConf._2
    val hqlContext = sparkConf._4



  }


  /**
    * 业务表添加保单数据
    * @param hqlContext
    */
  def TAccountsEmployerAddPolicy(hqlContext: HiveContext): DataFrame ={
    import hqlContext.implicits._

    /**
      *读取保单和批单的数据
      */
    val dwTaccountEmployerIntermeditae = hqlContext.sql("select distinct policy_no,preserve_id,add_batch_code,del_batch_code,data_source,project_name,product_code,product_name,channel_name,business_owner,business_region,business_source," +
      "business_type,performance_accounting_day,operational_name,holder_name,insurer_name,plan_price,plan_coverage,plan_append,plan_disability_rate,plan_pay_type,underwriting_company,policy_effect_date,policy_effective_time," +
      "policy_expire_time,policy_status,premium_total,premium_pay_status,premium_invoice_type,economy_company,economy_rates,economy_fee,technical_service_rates,technical_service_fee,consulting_service_rates,consulting_service_fee,service_fee_check_time," +
      "service_fee_check_status,has_brokerage,brokerage_ratio,brokerage_fee,brokerage_pay_status,remake,create_time,update_time,operator from dwdb.dw_t_accounts_employer_intermediate")


    /**
      * 读取业务表的数据
      */

    val dwTAccountsEmployerDetail = hqlContext.sql("select policy_no as policy_no_salve from dwdb.dw_t_accounts_employer_detail")


    /**
      * 关联两个表 拿到保单数据的增量数据
      */
    val res = dwTaccountEmployerIntermeditae.join(dwTAccountsEmployerDetail, 'policy_no === 'policy_no_salve, "leftouter")
      .selectExpr("policy_no", "preserve_id", "add_batch_code", "del_batch_code", "data_source", "project_name", "product_code", "product_name", "channel_name",
        "business_owner", "business_region", "business_source", "business_type", "performance_accounting_day", "operational_name", "holder_name", "insurer_name",
        "plan_price", "plan_coverage", "plan_append", "plan_disability_rate", "plan_pay_type", "underwriting_company",
        "policy_effect_date", "policy_effective_time", "policy_expire_time", "policy_status", "premium_total", "premium_pay_status", "premium_invoice_type", "economy_company",
        "economy_rates", "economy_fee", "technical_service_rates", "technical_service_fee", "consulting_service_rates", "consulting_service_fee", "service_fee_check_time",
        "service_fee_check_status", "has_brokerage", "brokerage_ratio", "brokerage_fee", "brokerage_fee", "brokerage_pay_status", "remake", "create_time", "update_time", "operator")
      .where("add_batch_code is null and del_batch_code is null and policy_no_salve is null")
    res

  }



  /**
    * 业务表增加批单数据
    * @param hqlContext
    */
  def TAccountsEmployerAddPreserve(hqlContext: HiveContext): Unit ={
    import hqlContext.implicits._

    /**
      *读取保单和批单的数据
      */
    val dwTaccountEmployerIntermeditae = hqlContext.sql("select distinct policy_no,preserve_id,add_batch_code,del_batch_code,data_source,project_name,product_code,product_name,channel_name,business_owner,business_region,business_source," +
      "business_type,performance_accounting_day,operational_name,holder_name,insurer_name,plan_price,plan_coverage,plan_append,plan_disability_rate,plan_pay_type,underwriting_company,policy_effect_date,policy_effective_time," +
      "policy_expire_time,policy_status,premium_total,premium_pay_status,premium_invoice_type,economy_company,economy_rates,economy_fee,technical_service_rates,technical_service_fee,consulting_service_rates,consulting_service_fee,service_fee_check_time," +
      "service_fee_check_status,has_brokerage,brokerage_ratio,brokerage_fee,brokerage_pay_status,remake,create_time,update_time,operator from dwdb.dw_t_accounts_employer_intermediate")



    /**
      * 读取业务表的数据
      */

    val dwTAccountsEmployerDetail = hqlContext.sql("select policy_no as policy_no_salve,preserve_id as preserve_id_salve from dwdb.dw_t_accounts_employer_detail")

    /**
      * 关联两个表 拿到批单数据的增量数据
      */
    val res = dwTaccountEmployerIntermeditae.join(dwTAccountsEmployerDetail, 'policy_no === 'policy_no_salve and 'preserve_id==='preserve_id_salve , "leftouter")
      .selectExpr("policy_no", "preserve_id", "add_batch_code", "del_batch_code", "data_source", "project_name", "product_code", "product_name", "channel_name",
        "business_owner", "business_region", "business_source", "business_type", "performance_accounting_day", "operational_name", "holder_name", "insurer_name",
        "plan_price", "plan_coverage", "plan_append", "plan_disability_rate", "plan_pay_type", "underwriting_company",
        "policy_effect_date", "policy_effective_time", "policy_expire_time", "policy_status", "premium_total", "premium_pay_status", "premium_invoice_type", "economy_company",
        "economy_rates", "economy_fee", "technical_service_rates", "technical_service_fee", "consulting_service_rates", "consulting_service_fee", "service_fee_check_time",
        "service_fee_check_status", "has_brokerage", "brokerage_ratio", "brokerage_fee", "brokerage_fee", "brokerage_pay_status", "remake", "create_time", "update_time", "operator")
      .where("preserve_id is null and policy_no_salve is null")
    res

  }


  /**
    * 更新 渠道信息,运营信息,销售信息,业务来源信息,保单状态信息
    * @param hqlContext
    */
    def UpdateMessage(hqlContext: HiveContext): Unit ={
      import hqlContext.implicits._

      /**
        * 读取最新的业务表
        */


      val dwTAccountsEmployerDetail = hqlContext.sql("select id,batch_no,policy_no,preserve_id,add_batch_code,del_batch_code,data_source,project_name,product_code,product_name,channel_name as channel_name," +
        "business_owner,business_region,business_source,business_type,performance_accounting_day,operational_name,holder_name,insurer_name,plan_price,plan_coverage," +
        "plan_append,plan_disability_rate,plan_pay_type,underwriting_company,policy_effect_date,policy_effective_time,policy_expire_time,policy_status,premium_total," +
        "premium_pay_status,premium_invoice_type,economy_company,economy_rates,economy_fee,technical_service_rates,technical_service_fee,consulting_service_rates," +
        "consulting_service_fee,service_fee_check_time,service_fee_check_status,has_brokerage,brokerage_ratio,brokerage_fee,brokerage_pay_status,remake,create_time," +
        "update_time,operator from dwdb.dw_t_accounts_employer_detail")


      /**
        * 读取销售渠道表
        */

       val odsGuzhuSalemanDetail = hqlContext.sql("select ent_name as ent_name_salve,channel_name as channel_name_salve,biz_operator as biz_operator_salve,salesman as salesman_salve,business_source as business_source_salve from odsdb.ods_ent_guzhu_salesman_detail")


      //更新渠道信息

      val updateChannelName = dwTAccountsEmployerDetail.join(odsGuzhuSalemanDetail, 'holder_name === 'ent_name_salve, "leftouter")
        .selectExpr("id", "batch_no", "policy_no", "preserve_id", "add_batch_code", "del_batch_code", "data_source", "project_name", "product_code", "product_name",
          "if(channel_name_salve = '直客',ent_name_salve,channel_name_salve) as channel_name",
          "business_owner", "business_region", "business_source", "business_type", "performance_accounting_day", "operational_name", "holder_name", "insurer_name",
          "plan_price", "plan_coverage", "plan_append", "plan_disability_rate", "plan_pay_type", "underwriting_company",
          "policy_effect_date", "policy_effective_time", "policy_expire_time", "policy_status", "premium_total", "premium_pay_status", "premium_invoice_type", "economy_company",
          "economy_rates", "economy_fee", "technical_service_rates", "technical_service_fee", "consulting_service_rates", "consulting_service_fee", "service_fee_check_time",
          "service_fee_check_status", "has_brokerage", "brokerage_ratio", "brokerage_fee", "brokerage_fee", "brokerage_pay_status", "remake", "create_time", "update_time", "operator")
          .where("holder_name != '' and (channel_name = '' or channel_name is null)")


      //更新运营信息

      val updateBizOperator = updateChannelName.join(odsGuzhuSalemanDetail, 'holder_name === 'ent_name_salve, "leftouter")
        .selectExpr("id", "batch_no", "policy_no", "preserve_id", "add_batch_code", "del_batch_code", "data_source", "project_name", "product_code", "product_name",
          "channel_name",
          "business_owner", "business_region", "business_source", "business_type", "performance_accounting_day", "biz_operator_salve as operational_name", "holder_name", "insurer_name",
          "plan_price", "plan_coverage", "plan_append", "plan_disability_rate", "plan_pay_type", "underwriting_company",
          "policy_effect_date", "policy_effective_time", "policy_expire_time", "policy_status", "premium_total", "premium_pay_status", "premium_invoice_type", "economy_company",
          "economy_rates", "economy_fee", "technical_service_rates", "technical_service_fee", "consulting_service_rates", "consulting_service_fee", "service_fee_check_time",
          "service_fee_check_status", "has_brokerage", "brokerage_ratio", "brokerage_fee", "brokerage_fee", "brokerage_pay_status", "remake", "create_time", "update_time", "operator")
        .where("holder_name != '' and (operational_name = '' or operational_name is null)")


      //更新销售信息
      val updateSaleman = updateBizOperator.join(odsGuzhuSalemanDetail, 'updateSaleman)
        .selectExpr("id", "batch_no", "policy_no", "preserve_id", "add_batch_code", "del_batch_code", "data_source", "project_name", "product_code", "product_name",
          "channel_name",
          " salesman_salve as business_owner", "business_region", "business_source", "business_type", "performance_accounting_day", "operational_name", "holder_name", "insurer_name",
          "plan_price", "plan_coverage", "plan_append", "plan_disability_rate", "plan_pay_type", "underwriting_company",
          "policy_effect_date", "policy_effective_time", "policy_expire_time", "policy_status", "premium_total", "premium_pay_status", "premium_invoice_type", "economy_company",
          "economy_rates", "economy_fee", "technical_service_rates", "technical_service_fee", "consulting_service_rates", "consulting_service_fee", "service_fee_check_time",
          "service_fee_check_status", "has_brokerage", "brokerage_ratio", "brokerage_fee", "brokerage_fee", "brokerage_pay_status", "remake", "create_time", "update_time", "operator")
        .where("holder_name != '' and business_owner = '' or business_owner is null")

      //更新业务来源信息
      val updateSource = updateSaleman.join(updateSaleman, 'holder_name === 'ent_name_salve, "leftouter")
        .selectExpr("id", "batch_no", "policy_no", "preserve_id", "add_batch_code", "del_batch_code", "data_source", "project_name", "product_code", "product_name",
          "channel_name",
          "business_owner", "business_region", "business_source_salve as business_source", "business_type", "performance_accounting_day", "operational_name", "holder_name", "insurer_name",
          "plan_price", "plan_coverage", "plan_append", "plan_disability_rate", "plan_pay_type", "underwriting_company",
          "policy_effect_date", "policy_effective_time", "policy_expire_time", "policy_status", "premium_total", "premium_pay_status", "premium_invoice_type", "economy_company",
          "economy_rates", "economy_fee", "technical_service_rates", "technical_service_fee", "consulting_service_rates", "consulting_service_fee", "service_fee_check_time",
          "service_fee_check_status", "has_brokerage", "brokerage_ratio", "brokerage_fee", "brokerage_fee", "brokerage_pay_status", "remake", "create_time", "update_time", "operator")
        .where("holder_name != '' and business_source = '' or business_source is null")



     //更新保单状态信息


      //读取保单信息
      val odsPolicyDetail = hqlContext.sql("select policy_code,cast(policy_status as string) as policy_status_salve from odsdb.ods_policy_detail")


      val updatePolicyStatus = updateSource.join(odsPolicyDetail, 'policy_no === 'policy_code, "leftouter")
        .selectExpr("id", "batch_no", "policy_no", "preserve_id", "add_batch_code", "del_batch_code", "data_source", "project_name", "product_code", "product_name",
          "channel_name",
          "business_owner", "business_region", "business_source", "business_type", "performance_accounting_day", "operational_name", "holder_name", "insurer_name",
          "plan_price", "plan_coverage", "plan_append", "plan_disability_rate", "plan_pay_type", "underwriting_company",
          "policy_effect_date", "policy_effective_time", "policy_expire_time", "policy_status_salve as policy_status", "premium_total", "premium_pay_status", "premium_invoice_type", "economy_company",
          "economy_rates", "economy_fee", "technical_service_rates", "technical_service_fee", "consulting_service_rates", "consulting_service_fee", "service_fee_check_time",
          "service_fee_check_status", "has_brokerage", "brokerage_ratio", "brokerage_fee", "brokerage_fee", "brokerage_pay_status", "remake", "create_time", "update_time", "operator")
        .where("policy_no is not null")
      updatePolicyStatus




    }




















}
