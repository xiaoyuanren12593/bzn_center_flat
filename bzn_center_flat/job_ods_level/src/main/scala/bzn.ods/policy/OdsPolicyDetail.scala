package bzn.ods.policy

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import bzn.job.common.Until
import bzn.ods.util.SparkUtil
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source
import scala.math.BigDecimal.RoundingMode

/**
  * author:xiaoYuanRen
  * Date:2019/5/21
  * Time:9:47
  * describe: 1.0 系统保全表
  **/
object OdsPolicyDetail extends SparkUtil with Until{

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName,"")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    val odsPolicyDetail = oneOdsPolicyDetail(hiveContext).unionAll(twoOdsPolicyDetail(hiveContext))
    odsPolicyDetail.cache()

    hiveContext.sql("truncate table odsdb.ods_policy_detail")
    odsPolicyDetail.repartition(10).write.mode(SaveMode.Append).saveAsTable("odsdb.ods_policy_detail")
//    odsPolicyDetail.repartition(1).write.mode(SaveMode.Overwrite).parquet("/dw_data/ods_data/OdsPolicyDetail")
    sc.stop()
  }

  /**
   * 2.0系统保单明细表
   * @param sqlContext 上下文
   */
  def twoOdsPolicyDetail(sqlContext:HiveContext) :DataFrame = {
    import sqlContext.implicits._
    sqlContext.udf.register("clean", (str: String) => clean(str))
    sqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    sqlContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")//设置日期格式
      val date = df.format(new Date())// new Date()为获取当前系统时间
      date + ""
    })
    sqlContext.udf.register("getNull", (line:String) =>  {
      if (line == "" || line == null || line == "NULL") 9 else line.toInt
    })

    /**
      * 读取保单表
      */
    val bPolicyBzncen: DataFrame = readMysqlTable(sqlContext,"b_policy_bzncen")
      .selectExpr("id as policy_id","holder_name","policy_no as master_policy_no","insurance_policy_no as policy_code","policy_type",
        "proposal_no as order_id","product_code",
        "proposal_no as order_code","user_code as user_id","first_insure_premium as first_premium","sum_premium as premium",
        "status","sell_channel_code as channel_id",
        "sell_channel_name as channel_name","start_date","end_date","start_date as effect_date","proposal_time as order_date",
        "continued_policy_no","insurance_name","premium_price",
        "first_insure_master_num","create_time","update_time","commission_discount_percent","policy_source_code","policy_source_name","big_policy","invoice_type")
      .cache()

    /**
      * 首先续投保单号不能为空，如果续投保单号存在，用保险公司保单号替换续投保单号，否则为空
      */
    val continuedPolicyNo = bPolicyBzncen.selectExpr("master_policy_no as policy_no","policy_code as policy_code_slave")
      .where("policy_no is not null")
      .cache()

    /**
      * 读取投保人企业
      */
    val bPolicyHolderCompanyBzncen: DataFrame = readMysqlTable(sqlContext,"b_policy_holder_company_bzncen")
      .selectExpr("policy_no","name","industry_code","industry_name","province_code","city_code","county_code")
      .map(f = x => {
        val policyNo = x.getAs [String]("policy_no")
        val name = x.getAs [String]("name")
        val holderType = if( x.getAs [String]("name") != null ) {1} else {-1}
        val industryCode = x.getAs [String]("industry_code")
        val industryName = x.getAs [String]("industry_name")
        val provinceCode = x.getAs [String]("province_code")
        val cityCode = x.getAs [String]("city_code")
        val countyCode = x.getAs [String]("county_code")
        var belongArea = ""
        if( provinceCode != null && cityCode != null && countyCode != null ) {
          if( provinceCode.length == 6 && cityCode.length == 6 && countyCode.length == 6 )
            belongArea = provinceCode.substring (0, 2) + cityCode.substring (2, 4) + countyCode.substring (4, 6)
        }
        if( belongArea == "" ) {
          belongArea = null
        }
        (policyNo, name, industryCode, industryName, belongArea,holderType)
      }).toDF("policy_no","name","industry_code","industry_name","belongArea","holder_type")

    /**
      * 读取投保人个人
      */
    val bPolicyHolderPersonBzncen: DataFrame = readMysqlTable(sqlContext,"b_policy_holder_person_bzncen")
      .selectExpr("policy_no","name","industry_code","industry_name","province_code","city_code","county_code")
      .map(x => {
        val policyNo = x.getAs[String]("policy_no")
        val name = x.getAs[String]("name")
        val holderType = if( x.getAs [String]("name") != null ) {2} else {-1}
        val industryCode = x.getAs[String]("industry_code")
        val industryName = x.getAs[String]("industry_name")
        val provinceCode = x.getAs[String]("province_code")
        val cityCode = x.getAs[String]("city_code")
        val countyCode = x.getAs[String]("county_code")
        var belongArea = ""
        if(provinceCode!=null && cityCode != null && countyCode != null){
          if(provinceCode.length == 6 && cityCode.length == 6 && countyCode.length == 6)
            belongArea = provinceCode.substring(0,2)+cityCode.substring(2,4)+countyCode.substring(4,6)
        }
        if(belongArea == ""){
          belongArea = null
        }
        (policyNo,name,industryCode,industryName,belongArea,holderType)
      }).toDF("policy_no","name","industry_code","industry_name","belongArea","holder_type")

    val bPolicyHolderCompanyUnion = bPolicyHolderCompanyBzncen.unionAll(bPolicyHolderPersonBzncen)

    /**
      * 读取产品表
      */
    val bsProductBzncen: DataFrame = readMysqlTable(sqlContext,"bs_product_bzncen")
      .selectExpr("product_code as product_code_2","product_name")

    /**
      * 读取被保人企业
      */
    val bPolicySubjectCompanyBzncen: DataFrame = readMysqlTable(sqlContext,"b_policy_subject_company_bzncen")
      .selectExpr("policy_no","name as insured_name")

    /**
      * 读取订单信息
      */
    val bPolicyPayBzncen: DataFrame = readMysqlTable(sqlContext, "b_policy_pay_bzncen")
      .selectExpr("policy_no as pay_policy_no", "pay_type")

    val bPolicySubject = bPolicySubjectCompanyBzncen

    val bPolicyBzncenTemp = bPolicyBzncen.join(continuedPolicyNo,bPolicyBzncen("continued_policy_no") ===continuedPolicyNo("policy_no"),"leftouter")
      .selectExpr("policy_id","holder_name","master_policy_no","policy_code","policy_type","order_id","product_code","order_code","user_id","first_premium","premium",
        "status","channel_id","channel_name","start_date","end_date","effect_date","order_date","commission_discount_percent","policy_source_code"
        ,"policy_source_name","big_policy","invoice_type",
        "case when continued_policy_no is null then continued_policy_no else policy_code_slave end as continued_policy_no",
        "insurance_name","premium_price","first_insure_master_num","create_time","update_time")

    val bPolicyBzncenTemp2 = bPolicyBzncenTemp.join(bPolicyPayBzncen, bPolicyBzncenTemp("master_policy_no") === bPolicyPayBzncen("pay_policy_no"), "leftouter")
      .selectExpr("policy_id","holder_name","master_policy_no","policy_code","policy_type","order_id","product_code","order_code","user_id","first_premium","premium",
        "status","channel_id","channel_name","start_date","end_date","effect_date","order_date","pay_type","commission_discount_percent","policy_source_code",
        "policy_source_name","big_policy","invoice_type","continued_policy_no", "insurance_name","premium_price","first_insure_master_num","create_time","update_time")

    /**
      * 保单表和投保人表进行关联
      */
    val bPolicyHolderCompany = bPolicyBzncenTemp2.join(bPolicyHolderCompanyUnion,bPolicyBzncenTemp2("master_policy_no") ===bPolicyHolderCompanyUnion("policy_no"),"leftouter")
      .selectExpr("policy_id","holder_name","name as holder_company_person_name","master_policy_no","policy_code","policy_type","order_id","product_code","order_code","user_id","first_premium","premium",
        "status","channel_id","channel_name","start_date","end_date","effect_date","order_date","pay_type","commission_discount_percent","policy_source_code","policy_source_name","big_policy","invoice_type",
        "continued_policy_no","insurance_name","industry_code","industry_name","belongArea","premium_price","first_insure_master_num","holder_type","create_time","update_time")

    /**
      * 上结果与产品表进行关联
      */
    val bPolicyHolderCompanyProductTemp = bPolicyHolderCompany.join(bsProductBzncen,bPolicyHolderCompany("product_code")===bsProductBzncen("product_code_2"),"leftouter")
      .selectExpr("policy_id","holder_name","holder_company_person_name","master_policy_no","policy_code","policy_type","order_id","product_code","product_name","order_code","user_id","first_premium","premium",
        "status","channel_id","channel_name","start_date","end_date","effect_date","order_date","pay_type","commission_discount_percent","policy_source_code","policy_source_name","big_policy","invoice_type",
        "continued_policy_no","insurance_name","industry_code","industry_name","belongArea","premium_price","first_insure_master_num","holder_type","create_time","update_time")

    /**
      * 上述结果与产品方案表进行关联
      */
    val bPolicyHolderCompanyProduct = bPolicyHolderCompanyProductTemp

    /**
      * 与被保人信息表关联
      */
    val bPolicyHolderCompanyProductInsured = bPolicyHolderCompanyProduct.join(bPolicySubject,bPolicyHolderCompanyProduct("master_policy_no") ===bPolicySubject("policy_no"),"leftouter")
      .selectExpr("policy_id","holder_name","holder_company_person_name","master_policy_no","policy_code","policy_type","order_id","product_code","product_name",
        "order_code","user_id","first_premium","premium","insured_name","status","channel_id","channel_name","start_date","end_date","effect_date","order_date","pay_type",
        "commission_discount_percent","policy_source_code","policy_source_name","big_policy","invoice_type","continued_policy_no","insurance_name","industry_code","industry_name",
        "belongArea","premium_price","first_insure_master_num","holder_type","create_time","update_time")

    bPolicyHolderCompanyProductInsured.registerTempTable("bPolicyHolderCompanyProductTemp")

    /**
      * 创建一个临时表
      */
    val bPolicyHolderCompanyProductNew = sqlContext.sql(
      "select *,case when `status` = 1 and end_date > NOW() then 1  when (`status` = 4 or (`status` = 1 and end_date < NOW())) then 0  when `status` = -1 then -1 else 99  end  as policy_status," +
        "case when a.holder_company_person_name = null then a.holder_name ELSE a.holder_company_person_name end as holder_name_new from bPolicyHolderCompanyProductTemp a")
      .selectExpr("getUUID() as id","master_policy_no","order_id","order_code","user_id","product_code","product_name","policy_id","policy_code","policy_type","first_premium",
        "premium as sum_premium","holder_name_new as holder_name","insured_name as insured_subject","start_date as policy_start_date",
        "end_date as policy_end_date","effect_date as policy_effect_date","order_date","pay_type","commission_discount_percent","policy_source_code","policy_source_name","big_policy","invoice_type","policy_status","continued_policy_no as preserve_policy_no",
        "insurance_name as insure_company_name","belongArea as belongs_regional","industry_code as belongs_industry","industry_name as belongs_industry_name","channel_id","channel_name",
        "product_code as sku_id","first_insure_master_num as num_of_preson_first_policy","holder_type","create_time as policy_create_time",
        "update_time as policy_update_time","getNow() as dw_create_time")


    /**
      * 方案信息
      * "product_code as sku_id","sku_coverage",
        "case when product_code = '17000001' then '3' else sku_append end as sku_append",
        "case when product_code = '17000001' then '2' else sku_ratio end as sku_ratio","premium_price as sku_price",
        "case when product_code = '17000001' then '2' else sku_charge_type end sku_charge_type","tech_service_rate",
      */

    /**
      * 读取产品明细表,将蓝领外包以外的数据进行处理，用总保费替换初投保费
      */
    /* val odsProductDetail = sqlContext.sql("select product_code as product_code_slave,one_level_pdt_cate from odsdb.ods_product_detail")
       .where("one_level_pdt_cate <> '蓝领外包'")

     val resEnd =
       bPolicyHolderCompanyProductNew.join(odsProductDetail,bPolicyHolderCompanyProductNew("product_code")===odsProductDetail("product_code_slave"),"leftouter")
         .selectExpr("id","master_policy_no","order_id","order_code","user_id","product_code","product_name","policy_id ","policy_code","policy_type",
           "case when product_code_slave is not null then first_premium else sum_premium end first_premium","sum_premium",
           "holder_name","insured_subject","policy_start_date","policy_end_date","policy_effect_date","order_date","pay_type","commission_discount_percent","policy_source_code","policy_source_name","big_policy","invoice_type","policy_status",
           "preserve_policy_no","insure_company_name","belongs_regional","belongs_industry","channel_id","channel_name","num_of_preson_first_policy",
           "policy_create_time","policy_update_time","dw_create_time")*/

    /**
      * 读取初投保费表
      */
    val policyFirstPremiumBznprd: DataFrame = readMysqlTable(sqlContext,"policy_first_premium_bznprd")
      .where("id in ('121212','121213','121214','121215')")
      .selectExpr("policy_id as policy_id_premium","pay_amount")

    //读取bs_channel_bznmana 拿到chanl_id 和 sale_name
    val bsChannelBznmana = readMysqlTable(sqlContext,"bs_channel_bznmana")
      .selectExpr("channel_id as channelId","sale_responsible_person_name as sales_name")

    val restemp = bPolicyHolderCompanyProductNew.join(policyFirstPremiumBznprd,bPolicyHolderCompanyProductNew("policy_id") === policyFirstPremiumBznprd("policy_id_premium"),"leftouter")
      .selectExpr("clean(id) as id",
        "clean(master_policy_no) as master_policy_no",
        "clean(order_id) as order_id",
        "clean(order_code) as order_code",
        "clean(user_id) as user_id",
        "clean(product_code) as product_code",
        "clean(product_name) as product_name",
        "clean(cast(policy_id as String)) as policy_id",
        "clean(policy_code) as policy_code",
        "policy_type",
        "cast((case when (case when policy_id_premium is not null then cast(pay_amount as decimal(14,4)) else cast(first_premium as decimal(14,4)) end) is null then sum_premium else first_premium end) as decimal(14,4)) as first_premium",
        "cast(sum_premium as decimal(14,4)) as sum_premium",
        "clean(holder_name) as holder_name",
        "clean(insured_subject) as insured_subject",
        "policy_start_date",
        "policy_end_date",
        "policy_effect_date",
        "order_date",
        "pay_type",
        "commission_discount_percent",
        "policy_source_code",
        "policy_source_name",
        "big_policy",
        "invoice_type",
        "policy_status",
        "clean(preserve_policy_no) as preserve_policy_no",
        "clean(insure_company_name) as insure_company_name",
        "clean(belongs_regional) as belongs_regional",
        "clean(belongs_industry) as belongs_industry",
        "clean(belongs_industry_name) as belongs_industry_name",
        "clean(channel_id) as channel_id",
        "clean(channel_name) as channel_name",
        "num_of_preson_first_policy",
        "holder_type",
        "policy_create_time",
        "policy_update_time",
        "dw_create_time")

    val res = restemp.join(bsChannelBznmana, restemp("channel_id") === bsChannelBznmana("channelId"), "leftouter")
      .selectExpr("id",
        "master_policy_no as policy_no",
        "order_id",
        "policy_type",
        "order_code",
        "user_id",
        "product_code",
        "product_name",
        "policy_id",
        "policy_code",
        "first_premium",
        "sum_premium",
        "holder_name",
        "case when insured_subject is null then holder_name else insured_subject end as insured_subject",
        "policy_start_date",
        "policy_end_date",
        "policy_effect_date",
        "case when pay_type in (1,2) then pay_type else -1 end as pay_type",
        "commission_discount_percent",
        "policy_source_code",
        "policy_source_name",
        "big_policy",
        "invoice_type",
        "policy_status",
        "preserve_policy_no",
        "order_date",
        "insure_company_name",
        "belongs_regional",
        "belongs_industry",
        "belongs_industry_name",
        "channel_id",
        "channel_name",
        "clean(case when sales_name = '' then null when sales_name = '客户运营负责人' then null " + "when sales_name ='销售默认' then null when sales_name = '运营默认' then null else sales_name end) as sales_name",
        "num_of_preson_first_policy",
        "holder_type",
        "policy_create_time",
        "policy_update_time",
        "'2.0' as source_system",
        "dw_create_time")

    res
  }

  /**
   * 1.0系统保单明细表
   * @param sqlContext //上下文
   */
  def oneOdsPolicyDetail(sqlContext:HiveContext) :DataFrame ={
    import sqlContext.implicits._
    sqlContext.udf.register("clean", (str: String) => clean(str))
    sqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    sqlContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")//设置日期格式
      val date = df.format(new Date())// new Date()为获取当前系统时间
      date + ""
    })
    sqlContext.udf.register("getNull", (line: String) => {
      if (line == "" || line == null || line == "NULL") 9 else line.toInt
    })

    /**
      * 读取订单信息表
      */
    val odrOrderInfoBznprd: DataFrame = readMysqlTable(sqlContext,"odr_order_info_bznprd")
      .selectExpr("id as master_order_id","order_code","user_id","pay_amount as pay_amount_master","sales_name","pay_type") //1是线上 2是线下

    /**
      * 读取初投保费表
      */
    val policyFirstPremiumBznprd: DataFrame = readMysqlTable(sqlContext,"policy_first_premium_bznprd")
      .selectExpr("policy_id as policy_id_premium","pay_amount")

    /**
      * 读取1.0保单信息
      */
    val odrPolicyBznprd: DataFrame = readMysqlTable(sqlContext,"odr_policy_bznprd")
      .selectExpr("id as master_policy_id","policy_code","order_id","policy_type","insure_code","premium","status","channelId","channel_name",
        "start_date","end_date","effect_date","'' as commission_discount_percent","'' as policy_source_code","'' as policy_source_name","'' as big_policy","invoice_type","renewal_policy_code","order_date",
        "insure_company_name","create_time","update_time")

    /**
      * 读取投保人信息表
      */
    val odrPolicyHolderBznprd: DataFrame = readMysqlTable(sqlContext,"odr_policy_holder_bznprd")
      .selectExpr("policy_id","name","holder_type","province","city","district","company_name","business_nature","business_line")
      .map(x => {
        val policyId = x.getAs[String]("policy_id")
        var name = x.getAs[String]("name")
        val holderType = if(x.getAs[String]("holder_type") == "1") 1 else if(x.getAs[String]("holder_type") == "2") 2 else -1
        val companyName = x.getAs[String]("company_name")
        val industryCode = x.getAs[String]("business_nature")
        val industryName = x.getAs[String]("business_line")
        if(companyName != null && companyName.length >0){
          name = companyName
        }
        val province = x.getAs[String]("province")
        val city = x.getAs[String]("city")
        val district = x.getAs[String]("district")
        var belongArea = ""
        if(province!=null && city != null && district != null){
          if(province.length == 6 && city.length == 6 && district.length == 6)
            belongArea = province.substring(0,2)+city.substring(2,4)+district.substring(4,6)
        }
        (policyId,name,industryCode,industryName,belongArea,holderType)
      }).toDF("policy_id","holder_subject","industry_code","industry_name","belongArea","holder_type")

    /**
      * 读取被保企业信息表
      */
    val odrPolicyInsurantBznprd: DataFrame = readMysqlTable(sqlContext,"odr_policy_insurant_bznprd")
      .selectExpr("policy_id","trim(company_name) as insured_subject")

    /**
      * 读取产品表
      */
    val pdtProductBznprd: DataFrame = readMysqlTable(sqlContext,"pdt_product_bznprd")
      .selectExpr("code as product_code","name as product_name")

    /**
      * 读取子保单表
      */
    val odrOrderItemInfoBznprd: DataFrame = readMysqlTable(sqlContext,"odr_order_item_info_bznprd")
      .selectExpr("order_id","quantity")

    val orderPolicyTemp = odrOrderInfoBznprd.join(odrPolicyBznprd,odrOrderInfoBznprd("master_order_id") === odrPolicyBznprd("order_id"),"leftouter")
      .where("order_id is not null")
      .selectExpr("master_order_id","policy_type","order_code","user_id","pay_amount_master","master_policy_id","policy_code","insure_code","premium","status",
        "channelId","channel_name","sales_name","start_date","end_date","effect_date","commission_discount_percent","policy_source_code",
        "policy_source_name", "big_policy", "invoice_type","pay_type",
        "renewal_policy_code","order_date","insure_company_name","create_time","update_time")

    val orderPolicy = orderPolicyTemp.join(policyFirstPremiumBznprd,orderPolicyTemp("master_policy_id")===policyFirstPremiumBznprd("policy_id_premium"),"leftouter")
      .selectExpr("master_order_id","policy_type","order_code","user_id","case when policy_id_premium is not null then pay_amount else pay_amount_master end as pay_amount",
        "master_policy_id","policy_code","insure_code","premium","status","channelId","channel_name","sales_name","start_date","end_date","effect_date","commission_discount_percent",
        "policy_source_code","policy_source_name","big_policy","invoice_type","pay_type",
        "renewal_policy_code","order_date","insure_company_name","create_time","update_time")

    val orderPolicyProductTemp = orderPolicy.join(pdtProductBznprd,orderPolicy("insure_code") === pdtProductBznprd("product_code"),"leftouter")
      .selectExpr("master_order_id","policy_type","order_code","user_id","pay_amount","master_policy_id","policy_code","insure_code","product_name","premium","status",
        "channelId","channel_name","sales_name","start_date","end_date","effect_date","commission_discount_percent","policy_source_code",
        "policy_source_name","big_policy","invoice_type","pay_type","renewal_policy_code","order_date","insure_company_name","create_time","update_time")

    val orderPolicyProduct = orderPolicyProductTemp

    val orderPolicyProductHolder = orderPolicyProduct.join(odrPolicyHolderBznprd,orderPolicyProduct("master_policy_id") === odrPolicyHolderBznprd("policy_id"),"leftouter")
      .selectExpr("master_order_id","policy_type","order_code","user_id","pay_amount","master_policy_id","policy_code","insure_code","product_name","premium",
        "holder_subject","status","channelId","channel_name","sales_name","start_date","end_date","effect_date","commission_discount_percent","policy_source_code","policy_source_name",
        "big_policy","invoice_type","pay_type", "renewal_policy_code","order_date","insure_company_name","industry_code","industry_name","belongArea","holder_type","create_time","update_time")

    val orderPolicyProductHolderInsurant = orderPolicyProductHolder.join(odrPolicyInsurantBznprd,orderPolicyProductHolder("master_policy_id") === odrPolicyInsurantBznprd("policy_id"),"leftouter")
      .selectExpr("master_order_id","policy_type","order_code","user_id","pay_amount","master_policy_id","policy_code","insure_code","product_name","premium","holder_subject",
        "insured_subject","status","channelId","channel_name","sales_name","start_date","end_date","effect_date","commission_discount_percent","policy_source_code","policy_source_name",
        "big_policy","invoice_type","pay_type", "renewal_policy_code","order_date", "insure_company_name","industry_code","industry_name","belongArea","holder_type","create_time","update_time")

    val orderPolicyProductHolderInsurantItemOrder = orderPolicyProductHolderInsurant.join(odrOrderItemInfoBznprd,orderPolicyProductHolderInsurant("master_order_id") === odrOrderItemInfoBznprd("order_id"),"leftouter")
      .selectExpr("getUUID() as id",
        "'' as policy_no",
        "master_order_id as order_id",
        "policy_type",
        "order_code","user_id",
        "insure_code as product_code",
        "product_name",
        "master_policy_id as policy_id ",
        "policy_code",
        "pay_amount as first_premium",
        "premium as sum_premium",
        "holder_subject as holder_name",
        "insured_subject",
        "start_date as policy_start_date",
        "end_date as policy_end_date",
        "effect_date as policy_effect_date",
        "case when commission_discount_percent = '' then null end as commission_discount_percent " ,
        "case when policy_source_code = '' then null end as policy_source_code",
        "case when policy_source_name = '' then null end as policy_source_name",
        "case when big_policy = '' then null end as big_policy",
        "invoice_type",
        "pay_type",
        "case when `status` = 1 or `status`= 7 or `status` = 6 then 1  when `status` = 0 or `status` = 9  or `status` = 10 then 0 else 99  end  as policy_status",
        "renewal_policy_code as preserve_policy_no",
        "order_date",
        "insure_company_name",
        "belongArea as belongs_regional",
        "industry_code",
        "industry_name",
        "channelId","channel_name","sales_name",
        "quantity as num_of_preson_first_policy",
        "holder_type",
        "create_time as policy_create_time",
        "update_time as policy_update_time",
        "getNow() as dw_create_time")
      .cache()

    val orderPolicyProductHolderInsurantItemOrderone = orderPolicyProductHolderInsurantItemOrder
      .where("order_id not in ('934cec7f92f54be7812cfcfa23a093cb') and (user_id not in ('10100080492') or user_id is null) and product_code not in ('15000001')")

    val orderPolicyProductHolderInsurantItemOrderTwo = orderPolicyProductHolderInsurantItemOrder
      .where("product_code in ('15000001') and (user_id not in ('10100080492') or user_id is null)")

    val res = orderPolicyProductHolderInsurantItemOrderone.unionAll(orderPolicyProductHolderInsurantItemOrderTwo)
      .where("policy_code != '21010000889180002031' and policy_code != '21010000889180002022' and policy_code != '21010000889180002030'")
    /* /**
       * 读取产品明细表,将蓝领外包以外的数据进行处理，用总保费替换初投保费
       */
     val odsProductDetail = sqlContext.sql("select product_code as product_code_slave,one_level_pdt_cate from odsdb.ods_product_detail")
       .where("one_level_pdt_cate <> '蓝领外包'")*/
    val resEnd = res.selectExpr(
      "clean(id) as id",
      "clean(policy_no) as policy_no",
      "clean(order_id) as order_id",
      "policy_type",
      "clean(order_code) as order_code",
      "clean(user_id) as user_id",
      "clean(product_code) as product_code",
      "clean(product_name) as product_name",
      "clean(policy_id) as policy_id",
      "clean(policy_code) as policy_code",
      "case when first_premium is null then sum_premium else first_premium end as first_premium",
      "sum_premium",
      "trim(holder_name) as holder_name",
      "case when clean(insured_subject) is null then trim(holder_name) else clean(insured_subject) end as insured_subject",
      "policy_start_date","policy_end_date","policy_effect_date",
      "case when pay_type in (1,2) then pay_type else -1 end as pay_type",
      "commission_discount_percent",
      "policy_source_code","policy_source_name","big_policy", "invoice_type",
      "policy_status",
      "clean(preserve_policy_no) as preserve_policy_no",
      "order_date",
      "clean(insure_company_name) as insure_company_name",
      "clean(belongs_regional) as belongs_regional",
      "clean(industry_code) as belongs_industry",
      "clean(industry_name) as belongs_industry_name",
      "clean(channelId) as channel_id","clean(channel_name) as channel_name",
      "case when sales_name = '' then null " +
        "when sales_name = '客户运营负责人' then null " +
        "when sales_name ='销售默认' then null when sales_name = '运营默认' then null else sales_name end as sales_name ",
      "num_of_preson_first_policy",
      "holder_type",
      "policy_create_time",
      "policy_update_time",
      "'1.0' as source_system",
      "dw_create_time")

    resEnd
  }
  /**
    * 获取 Mysql 表的数据
    *
    * @param sqlContext 上下文
    * @param tableName 读取Mysql表的名字
    * @return 返回 Mysql 表的 DataFrame
    */
  def readMysqlTable(sqlContext: SQLContext, tableName: String): DataFrame = {
    val properties: Properties = getProPerties
    sqlContext
      .read
      .format("jdbc")
      .option("url", properties.getProperty("mysql.url.106"))
      .option("driver", properties.getProperty("mysql.driver"))
      .option("user", properties.getProperty("mysql.username.106"))
      .option("password", properties.getProperty("mysql.password.106"))
      .option("numPartitions","10")
      .option("partitionColumn","id")
      .option("lowerBound", "0")
      .option("upperBound","200")
      .option("dbtable", tableName)
      .load()
  }

  /**
    * 获取配置文件
    * @return
    */
  def getProPerties : Properties= {
    val lines_source = Source.fromURL(getClass.getResource("/config_scala.properties")).getLines.toSeq
    val properties: Properties = new Properties()
    for (elem <- lines_source) {
      val split = elem.split("==")
      val key = split(0)
      val value = split(1)
      properties.setProperty(key,value)
    }
    properties
  }
}
