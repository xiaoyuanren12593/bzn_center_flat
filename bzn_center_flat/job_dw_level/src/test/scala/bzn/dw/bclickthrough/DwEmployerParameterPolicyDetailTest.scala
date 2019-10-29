package bzn.dw.bclickthrough


import java.text.SimpleDateFormat
import java.util.Date

import bzn.dw.util.SparkUtil
import bzn.job.common.Until
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext


/*
* @Author:liuxiang
* @Date：2019/10/29
* @Describe:
*/ object DwEmployerParameterPolicyDetailTest  extends SparkUtil with Until{

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "local[*]")

    val sc = sparkConf._2
    val hqlContext = sparkConf._4
    EmployerParameterPolicyDetai(hqlContext)


  }



  def EmployerParameterPolicyDetai(hqlCotext:HiveContext): Unit ={
    import hqlCotext.implicits._
    hqlCotext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      //设置日期格式
      val date = df.format(new Date()) // new Date()为获取当前系统时间
      date + ""
    })
    /**
      *  读取保单明细表
      */

    val odsPolicyDetail = hqlCotext.sql("select distinct policy_code,case source_system when '1.0' then '1' when '2.0' then '2' end as data_source," +
      "policy_status,policy_effect_date,policy_start_date,policy_end_date,product_code,insure_company_name,f" +
      "irst_premium,holder_name,insured_subject,invoice_type,preserve_policy_no,policy_create_time from odsdb.ods_policy_detail")
    //odsPolicyDetail.printSchema()

    /**
      * 读取客户归属表
      */

    val odsEntGuzhuDetail = hqlCotext.sql("select salesman,ent_name,biz_operator,channel_name from odsdb.ods_ent_guzhu_salesman_detail")
    //odsEntGuzhuDetail.printSchema()


    /**
      * 保单明细表关联客户归属信息表
      */
    val policyAndGuzhuRes = odsPolicyDetail.join(odsEntGuzhuDetail,'holder_name==='ent_name,"leftouter")
      .selectExpr("policy_code","product_code","data_source","policy_status","policy_effect_date","policy_start_date","policy_end_date","insure_company_name","first_premium",
        "holder_name","insured_subject","invoice_type","salesman","ent_name","channel_name","biz_operator","preserve_policy_no","policy_create_time")
    //policyAndGuzhuRes.printSchema()


    /**
      * 读取销售团队表
      */

    val odsEntSalesTeam = hqlCotext.sql("select team_name,sale_name from odsdb.ods_ent_sales_team_dimension")

    /**
      * 将结果关联销售团队表
      */

    val policyAndGuzhuSalve = policyAndGuzhuRes.join(odsEntSalesTeam, 'salesman === 'sale_name, "leftouter")
      .selectExpr("policy_code","product_code", "data_source", "policy_status", "policy_effect_date", "policy_start_date", "policy_end_date",
        "insure_company_name", "first_premium", "holder_name", "insured_subject", "invoice_type", "salesman", "team_name", "ent_name","channel_name", "biz_operator","preserve_policy_no","policy_create_time")

    /**
      * 读取方案类别表
      */

    val odsPolicyProductPlanDetail = hqlCotext.sql("select policy_code as policy_code_salve,sku_price,sku_ratio,sku_append,sku_coverage,economic_rate,tech_service_rate,sku_charge_type,commission_discount_rate from odsdb.ods_policy_product_plan_detail")


    /**
      * 将上述结果关联方案类别表
      */
    val policyAndProductPlan = policyAndGuzhuSalve.join(odsPolicyProductPlanDetail,'policy_code==='policy_code_salve,"leftouter")
      .selectExpr("policy_code","product_code", "data_source", "policy_status", "policy_effect_date", "policy_start_date", "policy_end_date",
        "insure_company_name", "first_premium", "holder_name", "insured_subject", "invoice_type", "salesman", "team_name", "ent_name","channel_name","biz_operator",
        "sku_price","sku_ratio","sku_append","sku_coverage","economic_rate","tech_service_rate","sku_charge_type","preserve_policy_no","commission_discount_rate","policy_create_time")

    /**
      * 读取产品表
      */

    val odsProductDetail = hqlCotext.sql("select product_desc,product_code as insure_code,product_name,two_level_pdt_cate from odsdb.ods_product_detail")


    /**
      * 关联产品表
      */
    val resTemp = policyAndProductPlan.join(odsProductDetail, 'product_code === 'insure_code, "leftouter")
      .selectExpr("policy_code", "product_code", "product_desc", "product_name","two_level_pdt_cate","data_source", "policy_status", "policy_effect_date", "policy_start_date", "policy_end_date",
        "insure_company_name", "first_premium", "holder_name", "insured_subject", "invoice_type", "salesman", "team_name", "ent_name","channel_name","biz_operator", "sku_price", "sku_ratio","sku_append","sku_coverage",
        "economic_rate",
        "tech_service_rate","sku_charge_type","preserve_policy_no","commission_discount_rate","policy_create_time")
      .where("policy_code !='' and policy_status in(0, 1) and policy_start_date >='2019-01-01 00:00:00'and policy_start_date <='getNow()' and two_level_pdt_cate in ('外包雇主', '骑士保', '大货车', '零工保')")

    /**
      * 将结果注册成临时表
      */


    resTemp.registerTempTable("policyAndProductPlanRes")



    val res=  hqlCotext.sql("select policy_code as policy_no,data_source,policy_status,policy_effect_date,policy_start_date as policy_effective_time ,policy_end_date as policy_expire_time," +
      "insure_company_name as underwriting_company,if(trim(channel_name)='直客',trim(ent_name),trim(channel_name)) as channel_name,first_premium as premium_total,holder_name ,insured_subject as insurer_name," +
      "if(two_level_pdt_cate = '零工保','3',sku_charge_type) as plan_pay_type,invoice_type as premium_invoice_type,if(preserve_policy_no is null,1,2) as business_type," +
      "sku_price as plan_price,if(sku_ratio is null,0,if(sku_ratio = 1, 0.05,if(sku_ratio = 2, 0.1,sku_ratio))) as plan_disability_rate,sku_append as plan_append," +
      "sku_coverage as plan_coverage,economic_rate as economy_rates, " +
      "round(cast(first_premium * economic_rate as decimal(14,4)),4) as economy_fee,tech_service_rate as technical_service_rates," +
      "round(cast(first_premium * tech_service_rate as decimal(14,4)),4) as technical_service_fee," +
      "product_desc as project_name,product_code,product_name,salesman as business_owner,team_name as business_region,biz_operator as operational_name," +
      "if(policy_start_date >=policy_create_time,policy_start_date,policy_create_time) as performance_accounting_day," +
      "case when commission_discount_rate !=null then commission_discount_rate else 0 end as brokerage_ratio ," +
      "round(cast(case when first_premium * commission_discount_rate !=null then first_premium * commission_discount_rate else 0 end as decimal(14,4)),4) as brokerage_fee," +
      "''as business_source,getNow() as create_time,getNow() as update_time from policyAndProductPlanRes")


    res.printSchema()



  }



}

