package bzn.dw.other

import java.text.SimpleDateFormat
import java.util.Date

import bzn.dw.util.SparkUtil
import bzn.job.common.{MysqlUntil, Until}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext

/*
* @Author:liuxiang
* @Date：2019/11/6
* @Describe:
*/ object DwTAccounts   extends SparkUtil with Until with MysqlUntil{

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "")

    val sc = sparkConf._2
    val hqlContext = sparkConf._4
    val sqlContext = sparkConf._3
    val AddPolicyRes = TAccountsEmployerAddPolicy(hqlContext, sqlContext)

    saveASMysqlTable(AddPolicyRes, "t_update_employer_detail_test", SaveMode.Append, "mysql.username.103",
      "mysql.password.103", "mysql.driver", "mysql_url.103.odsdb")

    val preRes = TAccountsEmployerAddPreserve(hqlContext,sqlContext)

    saveASMysqlTable(preRes, "t_update_employer_detail_test", SaveMode.Append, "mysql.username.103",
      "mysql.password.103", "mysql.driver", "mysql_url.103.odsdb")

    sc.stop()

  }


  /**
    * 添加保单数据
    *
    * @param hqlContext
    */
  def TAccountsEmployerAddPolicy(hqlContext: HiveContext,sqlContext:SQLContext): DataFrame = {
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
    //odsPolicyDetail.printSchema()

    /**
      * 读取客户归属表
      */

    val odsEntGuzhuDetail = hqlContext.sql("select salesman,ent_name,biz_operator,channel_name,business_source from odsdb.ods_ent_guzhu_salesman_detail")
    //odsEntGuzhuDetail.printSchema()

    /**
      * 保单明细表关联客户归属信息表
      */
    val policyAndGuzhuRes = odsPolicyDetail.join(odsEntGuzhuDetail, 'holder_name === 'ent_name, "leftouter")
      .selectExpr("policy_code", "product_code", "data_source", "policy_status", "policy_effect_date", "policy_start_date", "policy_end_date", "insure_company_name", "first_premium",
        "holder_name", "insured_subject", "invoice_type", "salesman", "ent_name", "channel_name", "biz_operator", "business_source", "preserve_policy_no", "policy_create_time")
    //policyAndGuzhuRes.printSchema()


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
      "policy_code as policy_no",
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
      "policy_start_date as policy_effective_time",
      "policy_end_date as policy_expire_time",
      "cast(policy_status as string) as policy_status",
      "first_premium as premium_total",
      "cast(invoice_type as string) as premium_invoice_type",
      "economic_rate as economy_rates",
      "cast(first_premium * economic_rate as decimal(14,4)) as economy_fee",
      "tech_service_rate as technical_service_rates",
      "cast(first_premium * tech_service_rate as decimal(14,4)) as technical_service_fee",
      "cast(now() as timestamp) as service_fee_check_time",
      "case when commission_discount_rate is  null then 0 else commission_discount_rate end as brokerage_ratio",
      "cast(case when (first_premium * commission_discount_rate) is not null then (first_premium * commission_discount_rate) else 0 end as decimal(14,4)) as brokerage_fee",
      "cast(now() as timestamp) as create_time",
      "cast(now() as timestamp) as update_time")


    /**
      * 将结果注册成临时表
      */
    res.registerTempTable("t_accounts_employer_intermediate_temp")

    /**
      * 读取保单和批单的数据
      */
    val dwTaccountEmployerIntermeditae = hqlContext.sql("select distinct policy_no,project_name,product_code,product_name,channel_name,data_source,business_owner,business_region,business_source," +
      "business_type,performance_accounting_day,operational_name,holder_name,insurer_name,plan_price,plan_coverage,plan_append,plan_disability_rate,plan_pay_type,underwriting_company,policy_effect_date,policy_effective_time," +
      "policy_expire_time,policy_status,premium_total,premium_invoice_type,economy_rates,economy_fee,technical_service_rates,technical_service_fee,service_fee_check_time," +
      "brokerage_ratio,brokerage_fee,create_time,update_time from t_accounts_employer_intermediate_temp")


    /**
      * 读取业务表的数据
      */

    val dwTAccountsEmployerDetail = readMysqlTable(sqlContext, "t_update_employer_detail_test", "mysql.username.103",
      "mysql.password.103", "mysql.driver", "mysql_url.103.odsdb")
      .selectExpr("policy_no as policy_no_salve")

    /**
      * 关联两个表 过滤出保单数据的增量数据
      */
    val resTemp = dwTaccountEmployerIntermeditae.join(dwTAccountsEmployerDetail, 'policy_no === 'policy_no_salve, "leftouter")
      .selectExpr("policy_no","policy_no_salve", "data_source", "project_name", "product_code", "product_name", "channel_name",
        "business_owner", "business_region", "business_source", "business_type", "performance_accounting_day", "operational_name", "holder_name", "insurer_name",
        "plan_price", "plan_coverage", "plan_append", "plan_disability_rate", "plan_pay_type", "underwriting_company",
        "policy_effect_date", "policy_effective_time", "policy_expire_time", "policy_status", "premium_total", "premium_invoice_type",
        "economy_rates", "economy_fee", "technical_service_rates", "technical_service_fee",  "service_fee_check_time","brokerage_ratio", "brokerage_fee", "create_time", "update_time")
      .where("policy_no_salve is null")


    //增量数据
    val res1 = resTemp.selectExpr(
      "getUUID() as id",
      "policy_no",
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
      "policy_effect_date",  "policy_effective_time", "policy_expire_time",
      "policy_status",
      "premium_total",
      "premium_invoice_type",
      "economy_rates",
      "economy_fee",
      "technical_service_rates",
      "technical_service_fee",
      "service_fee_check_time",
      "brokerage_ratio",
      "brokerage_fee",
      "create_time",
      "update_time")

    //更新数据

    /**
      * 读取销售渠道表
      */

    val odsGuzhuSalemanDetail = hqlContext.sql("select ent_name as ent_name_salve,channel_name as channel_name_salve,biz_operator as biz_operator_salve," +
      "salesman as salesman_salve,business_source as business_source_salve from odsdb.ods_ent_guzhu_salesman_detail")

    //关联两个表,更新渠道信息
    val updateTemp = res1.join(odsGuzhuSalemanDetail, 'holder_name === 'ent_name_salve, "leftouter")
      .selectExpr(
        "id",
        "policy_no",
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
        "policy_effective_time",
        "policy_expire_time",
        "policy_status",
        "premium_total",
        "premium_invoice_type",
        "economy_rates",
        "economy_fee",
        "technical_service_rates",
        "technical_service_fee",
        "service_fee_check_time",
        "brokerage_fee",
        "create_time",
        "update_time")
    //读取保单信息
    val odsPolicyDetailTemp = hqlContext.sql("select  DISTINCT policy_code,cast(policy_status as string) as policy_status_salve from odsdb.ods_policy_detail")

    val update = updateTemp.join(odsPolicyDetailTemp, 'policy_no === 'policy_code, "leftouter")
      .selectExpr(
        "policy_no",
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
        "policy_effective_time",
        "policy_expire_time",
        "case when policy_no is not null then policy_status_salve else policy_status end as policy_status",
        "premium_total",
        "premium_invoice_type",
        "economy_rates",
        "economy_fee",
        "technical_service_rates",
        "technical_service_fee",
        "service_fee_check_time",
        "brokerage_fee",
        "create_time",
        "update_time")
    update






  }


  /**
    * 批单数据
    * @param hqlContext
    * @return
    */
  def TAccountsEmployerAddPreserve(hqlContext: HiveContext,sqlContext:SQLContext): DataFrame = {
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
      * 读取批单表
      */

    val odsPolicyPreserveDetail = hqlContext.sql("select policy_id,preserve_id,policy_code,add_batch_code," +
      "del_batch_code,add_premium,del_premium,preserve_start_date,preserve_end_date,effective_date,preserve_type,pay_status,create_time,preserve_status from odsdb.ods_preservation_detail")


    /**
      * 读取保单明细表
      */

    val odsPolicyDetail = hqlContext.sql("select policy_id as policy_id_salve,product_code as insure_code,holder_name,insure_company_name,source_system,invoice_type,insured_subject,policy_status from odsdb.ods_policy_detail")


    /**
      * 批单表关联保单明细表
      */

    val policyAndPreserveDetailRes = odsPolicyPreserveDetail.join(odsPolicyDetail, 'policy_id === 'policy_id_salve, "leftouter")
      .selectExpr("policy_id", "preserve_id", "policy_code", "add_batch_code", "del_batch_code", "add_premium", "del_premium", "preserve_start_date", "preserve_end_date", "effective_date",
        "preserve_type", "pay_status", "create_time", "preserve_status", "insure_code", "holder_name", "insure_company_name", "source_system", "invoice_type", "insured_subject", "policy_status")


    /**
      * 读取产品方案表
      */

    val productPlan = hqlContext.sql("select policy_code as policy_code_salve, sku_charge_type,sku_price,sku_ratio,sku_append,sku_coverage,economic_rate,tech_service_rate,commission_discount_rate from odsdb.ods_policy_product_plan_detail")

    //关联产品方案表
    val preserveAndPorductPlan = policyAndPreserveDetailRes.join(productPlan, 'policy_code === 'policy_code_salve, "leftouter")
      .selectExpr("policy_id", "preserve_id", "policy_code", "add_batch_code", "del_batch_code", "add_premium", "del_premium", "preserve_start_date", "preserve_end_date", "effective_date",
        "preserve_type", "pay_status", "create_time", "preserve_status", "insure_code", "holder_name", "insure_company_name", "source_system", "invoice_type", "insured_subject", "policy_status",
        "sku_charge_type", "sku_price", "sku_ratio", "sku_append", "sku_coverage", "economic_rate", "tech_service_rate", "commission_discount_rate")

    /**
      * 读取销售表和团队表
      */


    val odsEntGuzhuSale = hqlContext.sql("select salesman,biz_operator,ent_name,channel_name,business_source from odsdb.ods_ent_guzhu_salesman_detail")

    val odsEntSaleTeam = hqlContext.sql("select sale_name,team_name from odsdb.ods_ent_sales_team_dimension")


    /**
      * 关联 销售表和团队表
      */
    val preserveAndSale = preserveAndPorductPlan.join(odsEntGuzhuSale, 'holder_name === 'ent_name, "leftouter")
      .selectExpr("policy_id", "preserve_id", "policy_code", "add_batch_code", "del_batch_code", "add_premium", "del_premium", "preserve_start_date", "preserve_end_date", "effective_date",
        "preserve_type", "pay_status", "create_time", "preserve_status", "insure_code", "holder_name", "insure_company_name", "source_system", "invoice_type", "insured_subject",
        "policy_status", "sku_charge_type", "sku_price", "sku_ratio", "sku_append", "sku_coverage", "economic_rate", "tech_service_rate", "commission_discount_rate", "salesman", "biz_operator", "business_source", "ent_name", "channel_name")


    val preserveAndsaleAndTeam = preserveAndSale.join(odsEntSaleTeam, 'salesman === 'sale_name, "leftouter")
      .selectExpr("policy_id", "preserve_id", "policy_code", "add_batch_code", "del_batch_code", "add_premium", "del_premium", "preserve_start_date", "preserve_end_date", "effective_date",
        "preserve_type", "pay_status", "create_time", "preserve_status", "insure_code", "holder_name", "insure_company_name", "source_system", "invoice_type", "insured_subject",
        "policy_status", "sku_charge_type", "sku_price", "sku_ratio", "sku_append", "sku_coverage", "economic_rate", "tech_service_rate", "commission_discount_rate", "salesman", "biz_operator", "business_source", "ent_name", "channel_name", "team_name")


    /**
      * 读取产品表
      */


    val odsPolicyProduct = hqlContext.sql("select product_desc,product_code,product_name,two_level_pdt_cate from odsdb.ods_product_detail")

    /**
      * 关联产品表
      */
    val resTempRes = preserveAndsaleAndTeam.join(odsPolicyProduct, 'insure_code === 'product_code, "leftouter")
      .selectExpr("policy_id", "preserve_id", "policy_code", "add_batch_code", "del_batch_code",
        "case when add_premium is null then 0 else add_premium end as add_premium",
        "case when del_premium is null then 0 else del_premium end as del_premium",
        "preserve_start_date", "preserve_end_date", "effective_date",
        "preserve_type", "pay_status", "create_time", "preserve_status", "insure_code", "holder_name", "insure_company_name", "source_system", "invoice_type", "insured_subject",
        "policy_status", "sku_charge_type", "sku_price", "sku_ratio", "sku_append", "sku_coverage", "economic_rate", "tech_service_rate", "commission_discount_rate", "salesman", "biz_operator", "business_source", "ent_name", "channel_name", "team_name", "product_desc", "product_name", "two_level_pdt_cate")
      .where("policy_status in (0,1,-1) and if(preserve_start_date is null," +
        "if(preserve_end_date is null,create_time>=cast('2019-01-01' as timestamp),preserve_end_date>=cast('2019-01-01' as timestamp)),preserve_start_date >=cast('2019-01-01' as timestamp)) and preserve_status = 1 " +
        "and two_level_pdt_cate in ('外包雇主', '骑士保', '大货车', '零工保')")


    val res = resTempRes.selectExpr(
      "getUUID() as id",
      "policy_code as policy_no",
      "preserve_id",
      "add_batch_code",
      "del_batch_code",
      "cast(preserve_status as string) as preserve_status",
      "case source_system when '1.0' then '1' when '2.0' then '2' else source_system end as data_source",
      "product_desc as project_name",
      "insure_code as product_code",
      "product_name",
      "if(trim(channel_name)='直客',trim(ent_name),channel_name)  as channel_name",
      "salesman as business_owner",
      "team_name as business_region",
      "business_source",
      "cast(case preserve_type when 1 then 0 when 2 then 2 when 5 then 5 else preserve_type end as string) as business_type",
      "if(preserve_start_date is null,if(preserve_end_date is not null and preserve_end_date>=create_time,preserve_end_date,create_time),if(preserve_start_date>=create_time,preserve_start_date,create_time)) as performance_accounting_day",
      "biz_operator as operational_name",
      "holder_name",
      "insured_subject as insurer_name",
      "sku_price as plan_price",
      "sku_coverage as plan_coverage",
      "sku_append  as plan_append",
      "cast(if(sku_ratio is null,0,if(sku_ratio = 1,0.05,if(sku_ratio=2,0.1,sku_ratio))) as decimal(14,4)) as plan_disability_rate",
      "if(two_level_pdt_cate = '零工保','3',sku_charge_type) as plan_pay_type",
      "insure_company_name as underwriting_company",
      "effective_date as policy_effect_date",
      "preserve_start_date as policy_start_time",
      "case when preserve_start_date is null then (case when preserve_end_date is null then effective_date end) end as policy_effective_time",
      "preserve_end_date as policy_expire_time",
      "cast(policy_status as string) as policy_status",
      "cast((add_premium + del_premium) as decimal(14,4)) as premium_total",
      "cast(case pay_status when 1 then 0 when 2 then 1 when 3 then 3 else pay_status end as string) as premium_pay_status",
      "cast(invoice_type as string) as premium_invoice_type",
      "economic_rate as economy_rates",
      "cast((add_premium + del_premium) * economic_rate as decimal(14,4))as economy_fee",
      "tech_service_rate as technical_service_rates",
      "round(cast((add_premium + del_premium) * tech_service_rate as decimal(14,4)),4) as technical_service_fee",
      "case when commission_discount_rate is null then 0 else commission_discount_rate end as brokerage_ratio",
      "cast((add_premium + del_premium) * commission_discount_rate as decimal(14,4))  as brokerage_fee",
      "cast(getNow() as timestamp) as create_time",
      "cast(getNow() as timestamp) as update_time")
    res.registerTempTable("t_accounts_employer_intermediate_temp")


    /**
      * 读取保单和批单的数据
      */
    val dwTaccountEmployerIntermeditae = hqlContext.sql("select distinct policy_no,preserve_id,add_batch_code,del_batch_code,preserve_status,data_source,project_name,product_code,product_name,channel_name,business_owner,business_region,business_source," +
      "business_type,performance_accounting_day,operational_name,holder_name,insurer_name,plan_price,plan_coverage,plan_append,plan_disability_rate,plan_pay_type,underwriting_company,policy_effect_date,policy_effective_time,policy_start_time," +
      "policy_expire_time,policy_status,premium_total,premium_pay_status,premium_invoice_type,economy_rates,economy_fee,technical_service_rates,technical_service_fee," +
      "brokerage_ratio,brokerage_fee,create_time,update_time from t_accounts_employer_intermediate_temp")

    /**
      * 读取业务表的数据
      */

    val dwTAccountsEmployerDetail = readMysqlTable(sqlContext, "t_update_employer_detail_test", "mysql.username.103",
      "mysql.password.103", "mysql.driver", "mysql_url.103.odsdb")
      .selectExpr("policy_no as policy_no_salve","preserve_id as preserve_id_salve")

    /**
      * 关联两个表 拿到批单数据的增量数据
      */
    val resTemp = dwTaccountEmployerIntermeditae.join(dwTAccountsEmployerDetail, 'policy_no === 'policy_no_salve and 'preserve_id === 'preserve_id_salve, "leftouter")
      .selectExpr("policy_no","policy_no_salve", "preserve_id","preserve_id_salve", "add_batch_code", "del_batch_code", "preserve_status", "data_source", "project_name", "product_code", "product_name", "channel_name",
        "business_owner", "business_region", "business_source", "business_type", "performance_accounting_day", "operational_name", "holder_name", "insurer_name",
        "plan_price", "plan_coverage", "plan_append", "plan_disability_rate", "plan_pay_type", "underwriting_company",
        "policy_effect_date", "policy_start_time", "policy_effective_time", "policy_expire_time", "policy_status", "premium_total", "premium_pay_status", "premium_invoice_type",
        "economy_rates", "economy_fee", "technical_service_rates", "technical_service_fee",
        "brokerage_ratio", "brokerage_fee", "brokerage_fee",  "create_time", "update_time")
      .where("preserve_id is not null and preserve_id_salve is null")


    val res1 = resTemp.selectExpr(
      "getUUID() as id",
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
      "policy_effect_date", "policy_start_time", "policy_effective_time", "policy_expire_time",
      "policy_status",
      "premium_total",
      "premium_pay_status",
      "premium_invoice_type",
      "economy_rates",
      "economy_fee",
      "technical_service_rates",
      "technical_service_fee",
      "brokerage_ratio",
      "brokerage_fee",
      "create_time", "update_time")

    //更新数据

    /**
      * 读取销售渠道表
      */

    val odsGuzhuSalemanDetail = hqlContext.sql("select ent_name as ent_name_salve,channel_name as channel_name_salve,biz_operator as biz_operator_salve," +
      "salesman as salesman_salve,business_source as business_source_salve from odsdb.ods_ent_guzhu_salesman_detail")

    //关联两个表,更新渠道信息
    val updateTemp = res1.join(odsGuzhuSalemanDetail, 'holder_name === 'ent_name_salve, "leftouter")
      .selectExpr(
        "id",
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
        "policy_effect_date","policy_start_time", "policy_effective_time", "policy_expire_time",
        "policy_status",
        "premium_total",
        "premium_pay_status",
        "premium_invoice_type",
        "economy_rates",
        "economy_fee",
        "technical_service_rates",
        "technical_service_fee",
        "brokerage_ratio",
        "brokerage_fee",
        "create_time",
        "update_time")

    //读取保单信息
    val odsPolicyDetailTemp = hqlContext.sql("select  DISTINCT policy_code,cast(policy_status as string) as policy_status_salve from odsdb.ods_policy_detail")

    val update = updateTemp.join(odsPolicyDetailTemp, 'policy_no === 'policy_code, "leftouter")
      .selectExpr(
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
        "case when policy_no is not null then policy_status_salve else policy_status end as policy_status",
        "premium_total",
        "premium_pay_status",
        "premium_invoice_type",
        "economy_rates",
        "economy_fee",
        "technical_service_rates",
        "technical_service_fee",
        "brokerage_ratio",
        "brokerage_fee",
        "create_time",
        "update_time")
    update

  }


}