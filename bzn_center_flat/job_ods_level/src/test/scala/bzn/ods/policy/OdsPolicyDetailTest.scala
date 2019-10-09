package bzn.ods.policy

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import bzn.job.common.Until
import bzn.ods.util.SparkUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source
import scala.math.BigDecimal.RoundingMode

/**
  * author:xiaoYuanRen
  * Date:2019/5/21
  * Time:9:47
  * describe: 1.0 系统保全表
  **/
object OdsPolicyDetailTest extends SparkUtil with Until{

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName,"local[*]")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4

    val one = oneOdsPolicyDetail(hiveContext)
    val two = twoOdsPolicyDetail(hiveContext)
    val all = one.unionAll(two)
    all.printSchema()
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
      .selectExpr("id as policy_id","holder_name","policy_no as master_policy_no","insurance_policy_no as policy_code","proposal_no as order_id","product_code",
        "proposal_no as order_code","user_code as user_id","first_insure_premium as first_premium","sum_premium as premium","status","sell_channel_code as channel_id",
        "sell_channel_name as channel_name","start_date","end_date","continued_policy_no","insurance_name","premium_price",
        "first_insure_master_num","create_time","update_time","commission_discount_percent")
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
      .selectExpr("policy_no","name","industry_code","province_code","city_code","county_code")
      .map(x => {
        val policyNo = x.getAs[String]("policy_no")
        val name = x.getAs[String]("name")
        val industryCode = x.getAs[String]("industry_code")
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
        (policyNo,name,industryCode,belongArea)
      }).toDF("policy_no","name","industry_code","belongArea")

    /**
      * 读取投保人个人
      */
    val bPolicyHolderPersonBzncen: DataFrame = readMysqlTable(sqlContext,"b_policy_holder_person_bzncen")
      .selectExpr("policy_no","name","industry_code","province_code","city_code","county_code")
      .map(x => {
        val policyNo = x.getAs[String]("policy_no")
        val name = x.getAs[String]("name")
        val industryCode = x.getAs[String]("industry_code")
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
        (policyNo,name,industryCode,belongArea)
      }).toDF("policy_no","name","industry_code","belongArea")

    val bPolicyHolderCompanyUnion = bPolicyHolderCompanyBzncen.unionAll(bPolicyHolderPersonBzncen)

    /**
      * 读取产品表
      */
    val bsProductBzncen: DataFrame = readMysqlTable(sqlContext,"bs_product_bzncen")
      .selectExpr("product_code as product_code_2","product_name")

    /**
      * 读取产品方案表
      */
    val bPolicyProductPlanBzncen = readMysqlTable(sqlContext,"b_policy_product_plan_bzncen")
      .selectExpr("policy_no","plan_amount","contain_trainee","payment_type","injure_percent","technology_fee","brokerage_fee")
      .map(x => {
        val policyNo = x.getAs[String]("policy_no")
        val planAmount = x.getAs[Double]("plan_amount") //方案保额
        val containTrainee = x.getAs[String]("contain_trainee") //2.0 1 2 3 null 特约（是否包含实习生）
        val paymentType = x.getAs[Int]("payment_type") //类型 年缴月缴
        val injurePercent = x.getAs[Double]("injure_percent")  //伤残比例
        var technologyFee = x.getAs[Double]("technology_fee") //技术服务费
        var brokerageFee = x.getAs[Double]("brokerage_fee") //经纪费
        var injurePercentRes = ""
        if(injurePercent == 0.05){
          injurePercentRes = "1"
        }else if (injurePercent == 0.10){
          injurePercentRes = "2"
        }else {
          injurePercentRes = null
        }

        if(technologyFee != null){
          technologyFee = technologyFee/100
        }
        if(brokerageFee != null){
          brokerageFee = brokerageFee/100
        }
        (policyNo,(planAmount,containTrainee,injurePercentRes,paymentType,technologyFee,brokerageFee))
      })
      .reduceByKey((x1,x2)=>{
        val res = x1
        res
      })
      .map(x => {
        (x._1,x._2._1,x._2._2,x._2._3,x._2._4,x._2._5,x._2._6)
      })
      .toDF("policy_no_plan","sku_coverage","sku_append","sku_ratio","sku_charge_type","tech_service_rate","economic_rate")

    /**
      * 读取被保人企业
      */
    val bPolicySubjectCompanyBzncen: DataFrame = readMysqlTable(sqlContext,"b_policy_subject_company_bzncen")
      .selectExpr("policy_no","name as insured_name")

    /**
      * 读取订单信息
      */
    val bPolicyPayBzncen: DataFrame = readMysqlTable(sqlContext, "b_policy_pay_bzncen")
      .selectExpr("policy_no as pay_policy_no", "pay_way")

    val bPolicySubject = bPolicySubjectCompanyBzncen

    val bPolicyBzncenTemp = bPolicyBzncen.join(continuedPolicyNo,bPolicyBzncen("continued_policy_no") ===continuedPolicyNo("policy_no"),"leftouter")
      .selectExpr("policy_id","holder_name","master_policy_no","policy_code","order_id","product_code","order_code","user_id","first_premium","premium",
        "status","channel_id","channel_name","start_date","end_date","commission_discount_percent",
        "case when continued_policy_no is null then continued_policy_no else policy_code_slave end as continued_policy_no",
        "insurance_name","premium_price","first_insure_master_num","create_time","update_time")

    val bPolicyBzncenTemp2 = bPolicyBzncenTemp.join(bPolicyPayBzncen, bPolicyBzncenTemp("master_policy_no") === bPolicyPayBzncen("pay_policy_no"), "leftouter")
      .selectExpr("policy_id","holder_name","master_policy_no","policy_code","order_id","product_code","order_code","user_id","first_premium","premium",
        "status","channel_id","channel_name","start_date","end_date","pay_way","commission_discount_percent",
        "continued_policy_no", "insurance_name","premium_price","first_insure_master_num","create_time","update_time")

    /**
      * 保单表和投保人表进行关联
      */
    val bPolicyHolderCompany = bPolicyBzncenTemp2.join(bPolicyHolderCompanyUnion,bPolicyBzncenTemp2("master_policy_no") ===bPolicyHolderCompanyUnion("policy_no"),"leftouter")
      .selectExpr("policy_id","holder_name","name as holder_company_person_name","master_policy_no","policy_code","order_id","product_code","order_code","user_id","first_premium","premium",
        "status","channel_id","channel_name","start_date","end_date","pay_way","commission_discount_percent","continued_policy_no","insurance_name","industry_code","belongArea","premium_price","first_insure_master_num","create_time","update_time")

    /**
      * 上结果与产品表进行关联
      */
    val bPolicyHolderCompanyProductTemp = bPolicyHolderCompany.join(bsProductBzncen,bPolicyHolderCompany("product_code")===bsProductBzncen("product_code_2"),"leftouter")
      .selectExpr("policy_id","holder_name","holder_company_person_name","master_policy_no","policy_code","order_id","product_code","product_name","order_code","user_id","first_premium","premium",
        "status","channel_id","channel_name","start_date","end_date","pay_way","commission_discount_percent","continued_policy_no","insurance_name","industry_code","belongArea","premium_price","first_insure_master_num","create_time","update_time")

    /**
      * 上述结果与产品方案表进行关联
      */
    val bPolicyHolderCompanyProduct = bPolicyHolderCompanyProductTemp

    /**
      * 与被保人信息表关联
      */
    val bPolicyHolderCompanyProductInsured = bPolicyHolderCompanyProduct.join(bPolicySubject,bPolicyHolderCompanyProduct("master_policy_no") ===bPolicySubject("policy_no"),"leftouter")
      .selectExpr("policy_id","holder_name","holder_company_person_name","master_policy_no","policy_code","order_id","product_code","product_name",
        "order_code","user_id","first_premium","premium","insured_name","status","channel_id","channel_name","start_date","end_date","pay_way",
        "commission_discount_percent","continued_policy_no","insurance_name","industry_code","belongArea","premium_price","first_insure_master_num",
        "create_time","update_time")

    bPolicyHolderCompanyProductInsured.registerTempTable("bPolicyHolderCompanyProductTemp")

    /**
      * 创建一个临时表
      */
    val bPolicyHolderCompanyProductNew = sqlContext.sql(
      "select *,case when `status` = 1 and end_date > NOW() then 1  when (`status` = 4 or (`status` = 1 and end_date < NOW())) then 0  when `status` = -1 then -1 else 99  end  as policy_status," +
      "case when a.holder_company_person_name = null then a.holder_name ELSE a.holder_company_person_name end as holder_name_new from bPolicyHolderCompanyProductTemp a")
      .selectExpr("getUUID() as id","order_id","order_code","user_id","product_code","product_name","policy_id","policy_code","first_premium",
        "premium as sum_premium","holder_name_new as holder_name","insured_name as insured_subject","start_date as policy_start_date",
        "end_date as policy_end_date","pay_way","commission_discount_percent","policy_status","continued_policy_no as preserve_policy_no",
        "insurance_name as insure_company_name","belongArea as belongs_regional","industry_code as belongs_industry","channel_id","channel_name",
        "product_code as sku_id","first_insure_master_num as num_of_preson_first_policy","create_time as policy_create_time",
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
    val odsProductDetail = sqlContext.sql("select product_code as product_code_slave,one_level_pdt_cate from odsdb.ods_product_detail")
      .where("one_level_pdt_cate <> '蓝领外包'")

    val resEnd =
      bPolicyHolderCompanyProductNew.join(odsProductDetail,bPolicyHolderCompanyProductNew("product_code")===odsProductDetail("product_code_slave"),"leftouter")
        .selectExpr("id","order_id","order_code","user_id","product_code","product_name","policy_id ","policy_code",
          "case when product_code_slave is not null then sum_premium else first_premium end first_premium","sum_premium",
          "holder_name","insured_subject","policy_start_date","policy_end_date","pay_way","commission_discount_percent","policy_status",
          "preserve_policy_no","insure_company_name","belongs_regional","belongs_industry","channel_id","channel_name","num_of_preson_first_policy",
          "policy_create_time","policy_update_time","dw_create_time")

    /**
      * 读取初投保费表
      */
    val policyFirstPremiumBznprd: DataFrame = readMysqlTable(sqlContext,"policy_first_premium_bznprd")
      .where("id in ('121212','121213','121214','121215')")
      .selectExpr("policy_id as policy_id_premium","pay_amount")

    //读取bs_channel_bznmana 拿到chanl_id 和 sale_name
    val bsChannelBznmana = readMysqlTable(sqlContext,"bs_channel_bznmana")
      .selectExpr("channel_id as channelId","sale_responsible_person_name as sales_name")

    val restemp = resEnd.join(policyFirstPremiumBznprd,resEnd("policy_id") === policyFirstPremiumBznprd("policy_id_premium"),"leftouter")
      .selectExpr("clean(id) as id",
        "clean(order_id) as order_id",
        "clean(order_code) as order_code",
        "clean(user_id) as user_id",
        "clean(product_code) as product_code",
        "clean(product_name) as product_name",
        "clean(cast(policy_id as String)) as policy_id",
        "clean(policy_code) as policy_code",
        "case when policy_id_premium is not null then cast(pay_amount as decimal(14,4)) else cast(first_premium as decimal(14,4)) end as first_premium",
        "cast(sum_premium as decimal(14,4)) as sum_premium",
        "clean(holder_name) as holder_name",
        "clean(insured_subject) as insured_subject",
        "policy_start_date",
        "policy_end_date",
        "case when getNull(pay_way) = 9 then null else getNull(pay_way) end  as pay_way",
        "commission_discount_percent",
        "policy_status",
        "clean(preserve_policy_no) as preserve_policy_no",
        "clean(insure_company_name) as insure_company_name",
        "clean(belongs_regional) as belongs_regional","clean(belongs_industry) as belongs_industry",
        "clean(channel_id) as channel_id",
        "clean(channel_name) as channel_name",
        "num_of_preson_first_policy",
        "policy_create_time",
        "policy_update_time",
        "dw_create_time")

    val res = restemp.join(bsChannelBznmana, restemp("channel_id") === bsChannelBznmana("channelId"), "leftouter")
      .selectExpr("id",
        "order_id",
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
        "pay_way",
        "commission_discount_percent",
        "policy_status",
        "preserve_policy_no",
        "insure_company_name",
        "belongs_regional", "belongs_industry",
        "channel_id",
        "channel_name",
        "clean(case when sales_name = '' then null when sales_name = '客户运营负责人' then null " + "when sales_name ='销售默认' then null when sales_name = '运营默认' then null else sales_name end) as sales_name",
        "num_of_preson_first_policy",
        "policy_create_time",
        "policy_update_time",
        "'2.0' as source_system",
        "dw_create_time")
   res

    println("2.0")
    res.printSchema()
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
      .selectExpr("id as master_order_id","order_code","user_id","pay_amount as pay_amount_master","sales_name")
      .where("master_order_id = 'ed6020c04f5342dba8247c2bd2cbda8f'")


    /**
      * 读取初投保费表
      */
    val policyFirstPremiumBznprd: DataFrame = readMysqlTable(sqlContext,"policy_first_premium_bznprd")
      .selectExpr("policy_id as policy_id_premium","pay_amount")

    /**
      * 读取1.0保单信息
      */
    val odrPolicyBznprd: DataFrame = readMysqlTable(sqlContext,"odr_policy_bznprd")
      .selectExpr("id as master_policy_id","policy_code","order_id","insure_code","premium","status","channelId","channel_name",
        "start_date","end_date","'' as pay_way","'' as commission_discount_percent","renewal_policy_code",
        "insure_company_name","create_time","update_time")
      .where("master_policy_id = 'dda19e31ff9d4ca0b6e3bd9af5d51398'")

    /**
      * 读取投保人信息表
      */
    val odrPolicyHolderBznprd: DataFrame = readMysqlTable(sqlContext,"odr_policy_holder_bznprd")
      .selectExpr("policy_id","name","province","city","district","company_name")
      .map(x => {
        val policyId = x.getAs[String]("policy_id")
        var name = x.getAs[String]("name")
        val companyName = x.getAs[String]("company_name")
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
        (policyId,name,belongArea)
      }).toDF("policy_id","holder_subject","belongArea")

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
      .selectExpr("order_id","industry_code","quantity")

    /**
      * 读取保单表和方案表作为临时表
      */
    val odrPolicyBznprdTemp = readMysqlTable(sqlContext,"odr_policy_bznprd")
      .selectExpr("id","sku_id")

    /**
      * 读取产品方案表
      */
    val pdtProductSkuBznprd: DataFrame = readMysqlTable(sqlContext,"pdt_product_sku_bznprd")
      .selectExpr("id as sku_id_slave","term_one","term_three","price")

    /**
      * 从产品方案表中获取保费，特约，保费类型，伤残赔付比例
      */
    val policyRes = odrPolicyBznprdTemp.join(pdtProductSkuBznprd,odrPolicyBznprdTemp("sku_id") === pdtProductSkuBznprd("sku_id_slave"),"leftouter")
      .selectExpr("id as policy_id_sku","sku_id","term_one","term_three","price")
      .map(f = x => {
        val policyIdSku = x.getAs [String]("policy_id_sku")
        val skuId = x.getAs [String]("sku_id")
        val termOne = x.getAs [Int]("term_one")
        val termThree = x.getAs [Int]("term_three")
        var price = x.getAs [java.math.BigDecimal]("price")
        if( price != null ) {
          price = price.setScale (4, RoundingMode (3)).bigDecimal
        }

        var skuCoverage = "" //保费
        var skuAppend = "" //特约
        var sku_charge_type = "" //保费类型  年缴或者月缴
        var sku_ratio = "" //伤残赔付比例
        if( termThree != null && termThree.toString.length == 5 ) {
          skuCoverage = termThree.toString.substring (0, 2)
        } else {
          if( termOne != null ) {
            skuCoverage = termOne.toString
          } else {
            skuCoverage = null
          }
        }

        if( termThree != null && termThree.toString.length == 5 ) {
          skuAppend = termThree.toString.substring (2, 3)
        } else {
          skuAppend = null
        }

        if( termThree != null && termThree.toString.length == 5 ) {
          sku_charge_type = termThree.toString.substring (3, 4)
        } else {
          sku_charge_type = null
        }

        if( termThree != null && termThree.toString.length == 5 ) {
          sku_ratio = termThree.toString.substring (4, 5)
        } else {
          sku_ratio = null
        }
        var tech_service_rate = ""
        var economic_rate = ""

        if( tech_service_rate == "" && economic_rate == "" ) {
          tech_service_rate = null
          economic_rate = null
        }

        (policyIdSku, skuId, skuCoverage, skuAppend, sku_ratio, price, sku_charge_type, tech_service_rate, economic_rate)
      })
      .toDF("policy_id_sku","sku_id","sku_coverage","sku_append","sku_ratio","sku_price","sku_charge_type","tech_service_rate","economic_rate")
      .distinct()

    val orderPolicyTemp = odrOrderInfoBznprd.join(odrPolicyBznprd,odrOrderInfoBznprd("master_order_id") === odrPolicyBznprd("order_id"),"leftouter")
      .where("order_id is not null")
      .selectExpr("master_order_id","order_code","user_id","pay_amount_master","master_policy_id","policy_code","insure_code","premium","status",
        "channelId","channel_name","sales_name","start_date","end_date","pay_way","commission_discount_percent","renewal_policy_code","insure_company_name","create_time","update_time")

    val orderPolicy = orderPolicyTemp.join(policyFirstPremiumBznprd,orderPolicyTemp("master_policy_id")===policyFirstPremiumBznprd("policy_id_premium"),"leftouter")
      .selectExpr("master_order_id","order_code","user_id","case when policy_id_premium is not null then pay_amount else pay_amount_master end as pay_amount",
        "master_policy_id","policy_code","insure_code","premium","status","channelId","channel_name","sales_name","start_date","end_date","pay_way","commission_discount_percent",
        "renewal_policy_code","insure_company_name","create_time","update_time")

    val orderPolicyProductTemp = orderPolicy.join(pdtProductBznprd,orderPolicy("insure_code") === pdtProductBznprd("product_code"),"leftouter")
      .selectExpr("master_order_id","order_code","user_id","pay_amount","master_policy_id","policy_code","insure_code","product_name","premium","status",
        "channelId","channel_name","sales_name","start_date","end_date","pay_way","commission_discount_percent","renewal_policy_code","insure_company_name","create_time","update_time")

    val orderPolicyProduct = orderPolicyProductTemp

    val orderPolicyProductHolder = orderPolicyProduct.join(odrPolicyHolderBznprd,orderPolicyProduct("master_policy_id") === odrPolicyHolderBznprd("policy_id"),"leftouter")
      .selectExpr("master_order_id","order_code","user_id","pay_amount","master_policy_id","policy_code","insure_code","product_name","premium",
        "holder_subject","status","channelId","channel_name","sales_name","start_date","end_date","pay_way","commission_discount_percent","renewal_policy_code",
        "insure_company_name","belongArea","create_time","update_time")

    val orderPolicyProductHolderInsurant = orderPolicyProductHolder.join(odrPolicyInsurantBznprd,orderPolicyProductHolder("master_policy_id") === odrPolicyInsurantBznprd("policy_id"),"leftouter")
      .selectExpr("master_order_id","order_code","user_id","pay_amount","master_policy_id","policy_code","insure_code","product_name","premium","holder_subject",
        "insured_subject","status","channelId","channel_name","sales_name","start_date","end_date","pay_way","commission_discount_percent","renewal_policy_code",
        "insure_company_name","belongArea","create_time","update_time")

    val orderPolicyProductHolderInsurantItemOrder = orderPolicyProductHolderInsurant.join(odrOrderItemInfoBznprd,orderPolicyProductHolderInsurant("master_order_id") === odrOrderItemInfoBznprd("order_id"),"leftouter")
      .selectExpr("getUUID() as id",
        "master_order_id as order_id",
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
        "pay_way",
        "case when commission_discount_percent = '' then null end as commission_discount_percent " ,
        "case when `status` = 1 then 1  when `status` = 0 then 0 else 99  end  as policy_status",
        "renewal_policy_code as preserve_policy_no" ,
        "insure_company_name",
        "belongArea as belongs_regional",
        "industry_code as belongs_industry",
        "channelId","channel_name","sales_name",
        "quantity as num_of_preson_first_policy",
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
    /**
      * 读取产品明细表,将蓝领外包以外的数据进行处理，用总保费替换初投保费
      */
    val odsProductDetail = sqlContext.sql("select product_code as product_code_slave,one_level_pdt_cate from odsdb.ods_product_detail")
      .where("one_level_pdt_cate <> '蓝领外包'")
    val resEnd = res.join(odsProductDetail,res("product_code")===odsProductDetail("product_code_slave"),"leftouter")
      .selectExpr(
        "clean(id) as id",
        "clean(order_id) as order_id",
        "clean(order_code) as order_code",
        "clean(user_id) as user_id",
        "clean(product_code) as product_code",
        "clean(product_name) as product_name",
        "clean(policy_id) as policy_id",
        "clean(policy_code) as policy_code",
        "case when product_code_slave is not null then sum_premium else first_premium end first_premium",
        "sum_premium",
        "trim(holder_name) as holder_name",
        "case when clean(insured_subject) is null then trim(holder_name) else clean(insured_subject) end as insured_subject",
        "policy_start_date","policy_end_date",
        "case when getNull(pay_way) = 9 then null else getNull(pay_way) end  as pay_way",
        "commission_discount_percent",
        "policy_status",
        "clean(preserve_policy_no) as preserve_policy_no",
        "clean(insure_company_name) as insure_company_name",
        "clean(belongs_regional) as belongs_regional",
        "clean(belongs_industry) as belongs_industry",
        "clean(channelId) as channel_id","clean(channel_name) as channel_name",
        "case when sales_name = '' then null " +
          "when sales_name = '客户运营负责人' then null " +
          "when sales_name ='销售默认' then null when sales_name = '运营默认' then null else sales_name end as sales_name ",
        "num_of_preson_first_policy",
        "policy_create_time",
        "policy_update_time",
        "'1.0' as source_system",
        "dw_create_time")
//      .where("policy_code != '21010000889180002031' and policy_code != '21010000889180002022' and policy_code != '21010000889180002030'")
    resEnd.show()
//    resEnd.show()
    println("1.0")
   resEnd.printSchema()

   // val test = resEnd.selectExpr("sales_name").where("sales_name is not null")
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
    val properties: Properties = getProPerties()
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
  def getProPerties() : Properties= {
    val lines_source = Source.fromURL(getClass.getResource("/config_scala.properties")).getLines.toSeq
    var properties: Properties = new Properties()
    for (elem <- lines_source) {
      val split = elem.split("==")
      val key = split(0)
      val value = split(1)
      properties.setProperty(key,value)
    }
    properties
  }
}