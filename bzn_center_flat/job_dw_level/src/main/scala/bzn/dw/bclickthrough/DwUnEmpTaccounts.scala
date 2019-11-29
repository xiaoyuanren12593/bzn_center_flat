package bzn.dw.bclickthrough

import bzn.dw.bclickthrough.DwEmpTAccountsIntermediateDetail.saveASMysqlTable
import bzn.dw.util.SparkUtil
import bzn.job.common.{MysqlUntil, Until}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext

/*
* @Author:liuxiang
* @Date：2019/11/28
* @Describe:
*/ object DwUnEmpTaccounts extends SparkUtil with Until  with  MysqlUntil{
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "")

    val sc = sparkConf._2
    val sqlContext = sparkConf._3
    val hiveContext = sparkConf._4

    val res = getAllBusinessPolicyDetail(hiveContext)
    val frame1 = getSportsScenMemberHealthPolicyDetail(hiveContext, sqlContext, res)
    val frame2 = getSportsScenMemberPreserveDetail(hiveContext, sqlContext, res)
    val frame3 = getHealthMemberPreserveDetail(hiveContext, sqlContext, res)


    val finRes = frame1.unionAll(frame2).unionAll(frame3)

    saveASMysqlTable(finRes, "t_accounts_un_employer_test", SaveMode.Append, "mysql.username.103",
      "mysql.password.103", "mysql.driver", "mysql_url.103.odsdb")

    //    res.write.mode(SaveMode.Overwrite).saveAsTable("dwdb.dw_policy_premium_detail")
    sc.stop()
  }

  /**
    * 获取所有业务条线的保单数据
    *
    * @param sqlContext 上下文
    */
  def getAllBusinessPolicyDetail(sqlContext: HiveContext): DataFrame = {

    /**
      * 读取保单明细表
      */
    val odsPolicyDetail = sqlContext.sql("select policy_code,case when source_system = '2.0' then '2' when source_system = '1.0' then '1' end as source_system,policy_status,policy_effect_date," +
      "policy_start_date,policy_end_date,insure_company_name,channel_id,channel_name,sales_name,first_premium,holder_name,invoice_type,product_code,policy_create_time,insured_subject from odsdb.ods_policy_detail")
      .where("policy_status in (0,1,-1) and policy_code is not null")

    /**
      * 读取产品表
      */
    val odsProductDetail = sqlContext.sql("select product_code as product_code_slave,product_name,product_desc,business_line from odsdb.ods_product_detail")

    /**
      * 读取方案表
      */
    val odsPolicyProductPlanDetail = sqlContext.sql("select policy_code as policy_code_slave,sku_coverage,sku_charge_type,sku_ratio,sku_price,sku_append,tech_service_rate,economic_rate,commission_discount_rate from odsdb.ods_policy_product_plan_detail")

    /**
      * 产品表  '体育','场景','员福','健康'
      */
    val odsSportProductDetail = odsProductDetail.where("business_line in ('体育','场景','员福','健康')")

    /**
      * 读取体育销售表
      */
    val odsSportsCustomersDimension = sqlContext.sql("select name,sales_name as sales_name_slave from  odsdb.ods_sports_customers_dimension")

    /**
      * 读取销售团队表
      */
    val odsEntSalesTeamDimension = sqlContext.sql("select sale_name,team_name from odsdb.ods_salesman_detail")

    /**
      * 保单表和产品方案表进行关联
      */

    val policyAndPlanRes = odsPolicyDetail.join(odsPolicyProductPlanDetail, odsPolicyDetail("policy_code") === odsPolicyProductPlanDetail("policy_code_slave"), "leftouter")
      .selectExpr(
        "policy_code",
        "source_system",
        "policy_status",
        "policy_effect_date",
        "policy_start_date",
        "policy_end_date",
        "policy_create_time",
        "insure_company_name",
        "channel_id",
        "channel_name",
        "sales_name",
        "first_premium",
        "holder_name",
        "insured_subject",
        "invoice_type",
        "product_code",
        "sku_coverage",
        "sku_charge_type",
        "sku_ratio",
        "sku_price",
        "sku_append",
        "tech_service_rate",
        "economic_rate",
        "commission_discount_rate"
      )

    /**
      * 结果与产品表进行关联
      */
    val policyAndPlanAndProductRes = policyAndPlanRes.join(odsSportProductDetail, policyAndPlanRes("product_code") === odsSportProductDetail("product_code_slave"))
      .selectExpr(
        "policy_code",
        "source_system",
        "policy_status",
        "policy_effect_date",
        "policy_start_date",
        "policy_end_date",
        "policy_create_time",
        "insure_company_name",
        "channel_id",
        "channel_name",
        "sales_name",
        "first_premium",
        "holder_name",
        "insured_subject",
        "invoice_type",
        "product_code",
        "product_name",
        "product_desc",
        "business_line",
        "sku_coverage",
        "sku_charge_type",
        "sku_ratio",
        "sku_price",
        "sku_append",
        "tech_service_rate",
        "economic_rate",
        "commission_discount_rate"
      )

    // 体育销售的处理
    val policyAndPlanAndProductSportsSaleRes = policyAndPlanAndProductRes.join(odsSportsCustomersDimension, policyAndPlanAndProductRes("channel_name") === odsSportsCustomersDimension("name"), "leftouter")
      .selectExpr(
        "policy_code",
        "source_system",
        "policy_status",
        "policy_effect_date",
        "policy_start_date",
        "policy_end_date",
        "policy_create_time",
        "insure_company_name",
        "channel_id",
        "channel_name",
        "case when sales_name is null and business_line = '体育' then sales_name_slave when sales_name is null and business_line = '健康' then '王艳' else sales_name  end as sales_name",
        "first_premium",
        "holder_name",
        "insured_subject",
        "invoice_type",
        "product_code",
        "product_name",
        "product_desc",
        "business_line",
        "sku_coverage",
        "sku_charge_type",
        "sku_ratio",
        "sku_price",
        "sku_append",
        "tech_service_rate",
        "economic_rate",
        "commission_discount_rate"
      )

    /**
      * 上结果与团队表进行关联
      */
    val policyAndPlanAndTeamRes = policyAndPlanAndProductSportsSaleRes.join(odsEntSalesTeamDimension, policyAndPlanAndProductSportsSaleRes("sales_name") === odsEntSalesTeamDimension("sale_name"), "leftouter")
      .selectExpr(
        "policy_code",
        "source_system",
        "policy_status",
        "policy_effect_date",
        "policy_start_date",
        "policy_end_date",
        "policy_create_time",
        "insure_company_name",
        "channel_id",
        "channel_name",
        "sales_name",
        "team_name",
        "first_premium",
        "holder_name",
        "insured_subject",
        "invoice_type",
        "product_code",
        "product_name",
        "product_desc",
        "business_line",
        "sku_coverage",
        "sku_charge_type",
        "sku_ratio",
        "sku_price",
        "sku_append",
        "tech_service_rate",
        "economic_rate",
        "commission_discount_rate"
      )

    /**
      * 获取体育保单数据信息
      */
    /* val resPolicy = getSportsScenMemberHealthPolicyDetail(hqlContext,sqlContext,policyAndPlanAndTeamRes)
     resPolicy.printSchema()
 */
    policyAndPlanAndTeamRes
  }


  /**
    * 体育,健康,员福,场景 的保单信息
    *
    * @param hqlContext
    * @param policyAndPlanAndTeamRes
    * @return
    */
  def getSportsScenMemberHealthPolicyDetail(hqlContext: HiveContext, sqlContext: SQLContext, policyAndPlanAndTeamRes: DataFrame): DataFrame = {
    import hqlContext.implicits._
    hqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    hqlContext.udf.register("clean", (str: String) => clean(str))

    val policyAndPlanAndTeamAndProductRes = policyAndPlanAndTeamRes
    //  .where("policy_start_date>='2019-01-01 00:00:00'")
      .selectExpr(
        "getUUID() as id",
        "clean('') as batch_no",
        "policy_code as policy_no",
        "clean('') as preserve_id",
        "clean('') as preserve_status",
        "clean('') as add_batch_code",
        "clean('') as del_batch_code",
        "source_system as data_source",
        "business_line as project_name",
        "product_code",
        "product_name",
        "product_desc as product_detail",
        "channel_name",
        "sales_name as business_owner",
        "team_name as business_region",
        "clean('') as business_source",
        "'1' as business_type",
        "if(policy_start_date >= policy_create_time,policy_start_date,policy_create_time) as performance_accounting_day",
        "holder_name",
        "insured_subject as insurer_name",
        "insure_company_name as underwriting_company",
        "policy_effect_date",
        "policy_start_date as policy_effective_time",
        "policy_end_date as policy_expire_time",
        "cast(policy_status as string) as policy_status",
        "sku_coverage as plan_coverage",
        "first_premium as premium_total",
        "'1' as premium_pay_status", //保费实收状态
        "clean('') as  behalf_number",
        "case when invoice_type is null then '0' else cast(invoice_type as string) end as premium_invoice_type",
        "'天津中策' as economy_company",
        "economic_rate as economy_rates",
        "cast((first_premium * economic_rate) as decimal(14,4)) as economy_fee",
        "tech_service_rate as technical_service_rates",
        "cast((first_premium * tech_service_rate) as decimal(14,4)) as technical_service_fee",
        "clean('') as consulting_service_rates",
        "clean('') as consulting_service_fee",
        "clean('') as service_fee_check_time",
        "clean('') as service_fee_check_status",
        "clean('') as has_brokerage",
        "commission_discount_rate as brokerage_ratio",
        "cast((first_premium * commission_discount_rate) as decimal(14,4)) as brokerage_fee",
        "clean('') as brokerage_pay_status",
        "clean('') as remake",
        "now() as create_time",
        "now() as update_time",
        "cast(clean('') as int) as operator"
      )


    /** *
      * 拿出增量数据
      */

    val dwTAccountsUnEmployerDetail = readMysqlTable(sqlContext, "t_accounts_un_employer", "mysql.username.103",
      "mysql.password.103", "mysql.driver", "mysql_url.103.odsdb")
      .selectExpr("policy_no as policy_no_salve")

    val res = policyAndPlanAndTeamAndProductRes.join(dwTAccountsUnEmployerDetail, 'policy_no === 'policy_no_salve, "leftouter")
      .selectExpr("id", "batch_no", "policy_no",
        "policy_no_salve", "preserve_id", "preserve_status", "add_batch_code", "del_batch_code",
        "data_source", "project_name", "product_code", "product_name", "product_detail", "channel_name", "business_owner",
        "business_region", "business_source", "business_type", "performance_accounting_day", "holder_name", "insurer_name", "underwriting_company",
        "policy_effect_date", "policy_effective_time", "policy_expire_time", "policy_status", "plan_coverage", "premium_total", "premium_pay_status", "behalf_number",
        "premium_invoice_type", "economy_company", "economy_rates", "economy_fee", "technical_service_rates", "technical_service_fee",
        "consulting_service_rates", "consulting_service_fee", "service_fee_check_time", "service_fee_check_status", "has_brokerage", "brokerage_ratio", "brokerage_fee",
        "brokerage_pay_status", "remake", "create_time", "update_time", "operator")
      .where("policy_no_salve is null and business_type='1'")

    val resfin = res.selectExpr("batch_no", "policy_no", "preserve_id", "preserve_status", "add_batch_code", "del_batch_code",
      "data_source", "project_name", "product_code", "product_name", "product_detail", "channel_name", "business_owner",
      "business_region", "business_source", "business_type", "performance_accounting_day", "holder_name", "insurer_name", "underwriting_company",
      "policy_effect_date", "policy_effective_time", "policy_expire_time", "policy_status", "plan_coverage", "premium_total", "premium_pay_status", "behalf_number",
      "premium_invoice_type", "economy_company", "economy_rates", "economy_fee", "technical_service_rates", "technical_service_fee",
      "consulting_service_rates", "consulting_service_fee", "service_fee_check_time", "service_fee_check_status", "has_brokerage", "brokerage_ratio", "brokerage_fee",
      "brokerage_pay_status", "remake", "create_time", "update_time", "operator")

    resfin


  }

  /**
    * 体育，员福，场景的批单信息
    *
    * @param hqlContext              上下文
    * @param policyAndPlanAndTeamRes 保单与方案团队结果
    * @return
    */
  def getSportsScenMemberPreserveDetail(hqlContext: HiveContext, sqlContext: SQLContext, policyAndPlanAndTeamRes: DataFrame): DataFrame = {
    import hqlContext.implicits._
    hqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    hqlContext.udf.register("clean", (str: String) => clean(str))

    /**
      * 读取保全表
      */
    val odsPreservationDetail = hqlContext.sql("select preserve_id,add_batch_code,add_premium,del_premium,del_batch_code,effective_date,preserve_start_date," +
      "preserve_end_date,preserve_status,pay_status,policy_code as policy_code_preserve,create_time,preserve_type from odsdb.ods_preservation_detail")
      .where("preserve_status = 1")

    val policyAndPlanAndTeamAndProductRes = policyAndPlanAndTeamRes
      .selectExpr(
        "policy_code",
        "source_system",
        "policy_status",
        "policy_effect_date",
        "policy_start_date",
        "policy_end_date",
        "policy_create_time",
        "insure_company_name",
        "channel_id",
        "channel_name",
        "sales_name",
        "team_name",
        "first_premium",
        "holder_name",
        "insured_subject",
        "invoice_type",
        "business_line",
        "product_code",
        "product_name",
        "product_desc",
        "sku_coverage",
        "sku_charge_type",
        "sku_ratio",
        "sku_price",
        "sku_append",
        "tech_service_rate",
        "economic_rate",
        "commission_discount_rate"
      )

    val policyAndPlanAndTeamAndProductPreserveRes = policyAndPlanAndTeamAndProductRes.join(odsPreservationDetail, policyAndPlanAndTeamAndProductRes("policy_code") === odsPreservationDetail("policy_code_preserve"))
      .where("if(preserve_start_date is null," +
        "if(preserve_end_date is not null and preserve_end_date>create_time,preserve_end_date,create_time)," +
        "if(preserve_start_date >= create_time,preserve_start_date,create_time)) >= '2019-01-01 00:00:00'")
      .selectExpr(
        "getUUID() as id",
        "clean('')  as batch_no",
        "policy_code as policy_no",
        "preserve_id",
        "cast(preserve_status as string) as preserve_status",
        "add_batch_code",
        "del_batch_code",
        "source_system as data_source",
        "business_line as project_name",
        "product_code",
        "product_name",
        "product_desc as product_detail",
        "channel_name",
        "sales_name as business_owner",
        "team_name as business_region",
        "clean('')  as business_source",
        "cast(preserve_type as string) as business_type",
        "if(preserve_start_date is null," +
          "if(preserve_end_date is not null and preserve_end_date>create_time,preserve_end_date,create_time)," +
          "if(preserve_start_date >= create_time,preserve_start_date,create_time)) as performance_accounting_day",
        "holder_name",
        "insured_subject as insurer_name",
        "insure_company_name as underwriting_company",
        "effective_date as policy_effect_date",
        "preserve_start_date as policy_effective_time",
        "preserve_end_date as policy_expire_time",
        "cast(policy_status as string) as policy_status",
        "sku_coverage as plan_coverage",
        "cast((if(add_premium is null,0,add_premium) + if(del_premium is null,0,del_premium)) as decimal(14,4)) as premium_total",
        "cast(pay_status as string) as premium_pay_status", //保费实收状态
        "clean('')  as behalf_number",
        "case when invoice_type is null then '0' else cast(invoice_type as string) end as premium_invoice_type",
        "'天津中策' as economy_company",
        "economic_rate as economy_rates",
        "cast(((if(add_premium is null,0,add_premium) + if(del_premium is null,0,del_premium)) * economic_rate) as decimal(14,4)) as economy_fee",
        "tech_service_rate as technical_service_rates",
        "cast(((if(add_premium is null,0,add_premium) + if(del_premium is null,0,del_premium)) * tech_service_rate) as decimal(14,4)) as technical_service_fee",
        "clean('')  as consulting_service_rates",
        "clean('')  as consulting_service_fee",
        "clean('')  as service_fee_check_time",
        "clean('')  as service_fee_check_status",
        "clean('')  as has_brokerage",
        "commission_discount_rate as brokerage_ratio",
        "cast(((if(add_premium is null,0,add_premium) + if(del_premium is null,0,del_premium)) * commission_discount_rate) as decimal(14,4)) as brokerage_fee",
        "clean('')  as brokerage_pay_status",
        "clean('')  as remake",
        "now() as create_time",
        "now() as update_time",
        "cast(clean('') as int) as operator"
      )

    /**
      * 拿到批单增量数据
      */


    val dwTAccountsUnEmployerDetail = readMysqlTable(sqlContext, "t_accounts_un_employer", "mysql.username.103",
      "mysql.password.103", "mysql.driver", "mysql_url.103.odsdb")
      .selectExpr("policy_no as policy_no_salve", "preserve_id as preserve_id_salve")

    val res = policyAndPlanAndTeamAndProductPreserveRes.join(dwTAccountsUnEmployerDetail, 'policy_no === 'policy_no_salve and 'preserve_id === 'preserve_id_salve, "leftouter")
      .selectExpr("id", "batch_no", "policy_no",
        "policy_no_salve", "preserve_id", "preserve_id_salve", "preserve_status", "add_batch_code", "del_batch_code",
        "data_source", "project_name", "product_code", "product_name", "product_detail", "channel_name", "business_owner",
        "business_region", "business_source", "case when business_type = '3' then '5' else  '0' end  as business_type",
        "performance_accounting_day", "holder_name", "insurer_name", "underwriting_company",
        "policy_effect_date", "policy_effective_time", "policy_expire_time", "policy_status", "plan_coverage", "premium_total", "premium_pay_status", "behalf_number",
        "premium_invoice_type", "economy_company", "economy_rates", "economy_fee", "technical_service_rates", "technical_service_fee",
        "consulting_service_rates", "consulting_service_fee", "service_fee_check_time", "service_fee_check_status", "has_brokerage", "brokerage_ratio", "brokerage_fee",
        "brokerage_pay_status", "remake", "create_time", "update_time", "operator")
      .where("preserve_id_salve is null")
    val resfin = res
      .selectExpr("batch_no", "policy_no", "preserve_id", "preserve_status", "add_batch_code", "del_batch_code",
        "data_source", "project_name", "product_code", "product_name", "product_detail", "channel_name", "business_owner",
        "business_region", "business_source", "case when business_type = '3' then '5' else  '0' end  as business_type",
        "performance_accounting_day", "holder_name", "insurer_name", "underwriting_company",
        "policy_effect_date", "policy_effective_time", "policy_expire_time", "policy_status", "plan_coverage", "premium_total", "premium_pay_status", "behalf_number",
        "premium_invoice_type", "economy_company", "economy_rates", "economy_fee", "technical_service_rates", "technical_service_fee",
        "consulting_service_rates", "consulting_service_fee", "service_fee_check_time", "service_fee_check_status", "has_brokerage", "brokerage_ratio", "brokerage_fee",
        "brokerage_pay_status", "remake", "create_time", "update_time", "operator")

    resfin


  }


  /**
    * 健康的批单信息
    *
    * @param sqlContext              上下文
    * @param policyAndPlanAndTeamRes 保单与方案团队结果
    * @return
    */
  def getHealthMemberPreserveDetail(hqlContext: HiveContext, sqlContext: SQLContext, policyAndPlanAndTeamRes: DataFrame): DataFrame = {
    hqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    hqlContext.udf.register("clean", (str: String) => clean(str))
    import hqlContext.implicits._
    /**
      * 读取产品表
      */
    val odsSportProductDetail = hqlContext.sql("select product_code as product_code_slave,product_name as product_name_salve,product_desc as product_desc_salve,business_line as business_line_salve from odsdb.ods_product_detail")
      .where("business_line_salve in ('健康')")
    /**
      * 读取健康批单数据
      */
    val odsHealthPreserceDetail = hqlContext.sql("select insurance_policy_no as policy_code_preserve,preserve_id,premium_total,holder_name as holder_name_master,insurer_name,channel_name as channel_name_master," +
      "policy_effective_time,create_time from odsdb.ods_health_preserce_detail")

    val policyAndPlanAndTeamAndProductRes = policyAndPlanAndTeamRes.join(odsSportProductDetail, policyAndPlanAndTeamRes("product_code") === odsSportProductDetail("product_code_slave"))
      .selectExpr(
        "policy_code",
        "source_system",
        "policy_status",
        "policy_effect_date",
        "policy_start_date",
        "policy_end_date",
        "policy_create_time",
        "insure_company_name",
        "channel_id",
        "channel_name",
        "sales_name",
        "team_name",
        "first_premium",
        "holder_name",
        "insured_subject",
        "invoice_type",
        "business_line",
        "product_code",
        "product_name",
        "product_desc",
        "sku_coverage",
        "sku_charge_type",
        "sku_ratio",
        "sku_price",
        "sku_append",
        "tech_service_rate",
        "economic_rate",
        "commission_discount_rate"
      )

    val policyAndPlanAndTeamAndProductPreserveRes = odsHealthPreserceDetail.join(policyAndPlanAndTeamAndProductRes, odsHealthPreserceDetail("policy_code_preserve") === policyAndPlanAndTeamAndProductRes("policy_code"))
      .where("policy_start_date >= '2019-01-01 00:00:00'")
      .selectExpr(
        "getUUID() as id",
        "clean('')  as batch_no",
        "policy_code as policy_no",
        "preserve_id",
        "clean('') as preserve_status",
        "clean('') as add_batch_code",
        "clean('') as del_batch_code",
        "source_system as data_source",
        "business_line as project_name",
        "product_code",
        "product_name",
        "product_desc as product_detail",
        "channel_name_master as  channel_name",
        "sales_name as business_owner",
        "team_name as business_region",
        "clean('')  as business_source",
        "'1' as business_type",
        "policy_effective_time as performance_accounting_day",
        "holder_name_master",
        "insurer_name",
        "insure_company_name as underwriting_company",
        "cast(clean('') as timestamp) as policy_effect_date",
        "policy_effective_time",
        "cast(clean('') as timestamp) as policy_expire_time",
        "cast(policy_status as string) as policy_status",
        "sku_coverage as plan_coverage",
        "premium_total as premium_total",
        "'1' as premium_pay_status", //保费实收状态
        "clean('')  as behalf_number",
        "case when invoice_type is null then '0' else cast(invoice_type as string) end as premium_invoice_type",
        "'天津中策' as economy_company",
        "economic_rate as economy_rates",
        "cast((premium_total * economic_rate) as decimal(14,4)) as economy_fee",
        "tech_service_rate as technical_service_rates",
        "cast((premium_total * tech_service_rate) as decimal(14,4)) as technical_service_fee",
        "clean('')  as consulting_service_rates",
        "clean('')  as consulting_service_fee",
        "clean('')  as service_fee_check_time",
        "clean('')  as service_fee_check_status",
        "clean('')  as has_brokerage",
        "commission_discount_rate as brokerage_ratio",
        "cast((premium_total * commission_discount_rate) as decimal(14,4)) as brokerage_fee",
        "clean('')  as brokerage_pay_status",
        "clean('')  as remake",
        "now() as create_time",
        "now() as update_time",
        "cast(clean('') as int) as operator"
      )


    /**
      * 拿到批单增量数据
      */


    val dwTAccountsUnEmployerDetail = readMysqlTable(sqlContext, "t_accounts_un_employer", "mysql.username.103",
      "mysql.password.103", "mysql.driver", "mysql_url.103.odsdb")
      .selectExpr("policy_no as policy_no_salve", "preserve_id as preserve_id_salve")

    val res = policyAndPlanAndTeamAndProductPreserveRes.join(dwTAccountsUnEmployerDetail, 'policy_no === 'policy_no_salve and 'preserve_id === 'preserve_id_salve, "leftouter")
      .selectExpr("id", "batch_no", "policy_no",
        "policy_no_salve", "preserve_id", "preserve_id_salve", "preserve_status", "add_batch_code", "del_batch_code",
        "data_source", "project_name", "product_code", "product_name", "product_detail", "channel_name", "business_owner",
        "business_region", "business_source", "case when business_type = '3' then '5' else  '0' end  as business_type",
        "performance_accounting_day", "holder_name_master as holder_name", "insurer_name", "underwriting_company",
        "policy_effect_date", "policy_effective_time", "policy_expire_time", "policy_status", "plan_coverage", "premium_total", "premium_pay_status", "behalf_number",
        "premium_invoice_type", "economy_company", "economy_rates", "economy_fee", "technical_service_rates", "technical_service_fee",
        "consulting_service_rates", "consulting_service_fee", "service_fee_check_time", "service_fee_check_status", "has_brokerage", "brokerage_ratio", "brokerage_fee",
        "brokerage_pay_status", "remake", "create_time", "update_time", "operator")
      .where("preserve_id_salve is null")

    val resfin = res
      .selectExpr("batch_no", "policy_no", "preserve_id", "preserve_status", "add_batch_code", "del_batch_code",
        "data_source", "project_name", "product_code", "product_name", "product_detail", "channel_name", "business_owner",
        "business_region", "business_source", "case when business_type = '3' then '5' else  '0' end  as business_type",
        "performance_accounting_day", "holder_name", "insurer_name", "underwriting_company",
        "policy_effect_date", "policy_effective_time", "policy_expire_time", "policy_status", "plan_coverage", "premium_total", "premium_pay_status", "behalf_number",
        "premium_invoice_type", "economy_company", "economy_rates", "economy_fee", "technical_service_rates", "technical_service_fee",
        "consulting_service_rates", "consulting_service_fee", "service_fee_check_time", "service_fee_check_status", "has_brokerage", "brokerage_ratio", "brokerage_fee",
        "brokerage_pay_status", "remake", "create_time", "update_time", "operator")

    resfin
  }


}