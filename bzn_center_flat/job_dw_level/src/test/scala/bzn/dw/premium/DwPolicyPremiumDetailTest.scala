package bzn.dw.premium

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import bzn.dw.util.SparkUtil
import bzn.job.common.Until
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * author:xiaoYuanRen
  * Date:2019/6/10
  * Time:9:42
  * describe: dw层  所有费用明细表 保费，经纪费，技术服务费，手续费
  **/
object DwPolicyPremiumDetailTest extends SparkUtil with Until{
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName,"local[*]")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    val res = DwPolicyPremiumDetail(hiveContext)

    sc.stop()
  }

  /**
    * 出单保费
    * @param sqlContext 上下文
    */
  def DwPolicyPremiumDetail(sqlContext:HiveContext) :DataFrame={
    import sqlContext.implicits._
    sqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    sqlContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")//设置日期格式
      val date = df.format(new Date())// new Date()为获取当前系统时间
      date + ""
    })

    /**
      * 读取保单明细表
      */
    val odsPolicyDetail =
      sqlContext.sql("select policy_id,policy_code,product_code,policy_status,first_premium,preserve_policy_no," +
        "num_of_preson_first_policy,policy_start_date,insure_company_name,holder_name,insured_subject,belongs_regional " +
        "from odsdb.ods_policy_detail")
          .where("cast(policy_start_date as string) like '%2019-09-01%'")
        .cache()


    /**
      * 读取保全明细表
      */
    val odsPreseveDetail =
      sqlContext.sql("select preserve_id,policy_id as policy_id_preserve,add_batch_code,del_batch_code,add_person_count,add_premium," +
        "del_person_count,del_premium,preserve_effect_date,preserve_status,preserve_type from odsdb.ods_preservation_detail")
        .cache()

    /**
      * 读取产品表
      */
    val odsProductDetail =
      sqlContext.sql("select product_code as product_code_slave,product_name,one_level_pdt_cate,two_level_pdt_cate,business_line from odsdb.ods_product_detail")
      .where("product_code_slave is not null")

    /**
      * 保单新投保费和续投保费
      */
    val newPolicy = odsPolicyDetail
      .where("policy_status in (0,1,-1) and preserve_policy_no is null")
      .map(x => {
        val policyId = x.getAs[String]("policy_id")
        val policyCode = x.getAs[String]("policy_code")
        val productCode = x.getAs[String]("product_code")
        var firstPremium = x.getAs[java.math.BigDecimal]("first_premium")
        if(firstPremium == null){
          firstPremium = java.math.BigDecimal.valueOf(0.0)
        }
        val preservePolicyNo = x.getAs[String]("preserve_policy_no")
        val policyStartDate = x.getAs[Timestamp]("policy_start_date")
        val insureCompanyName = x.getAs[String]("insure_company_name")
        val holderName = x.getAs[String]("holder_name")
        val insuredSubject = x.getAs[String]("insured_subject")
        val belongsRegional = x.getAs[String]("belongs_regional")
        var policyStartDateRes = ""
        if(policyStartDate != null && policyStartDate.toString.length > 10){
          policyStartDateRes = policyStartDate.toString.substring(0,10).replaceAll("-","")
        }else{
          policyStartDateRes = null
        }
        val numOfPresonFirstPolicy = x.getAs[Int]("num_of_preson_first_policy")

        var add_batch_code = ""
        var del_batch_code = ""

        if(add_batch_code =="" &&del_batch_code==""){
          add_batch_code = null
          del_batch_code = null
        }

        (policyId,policyCode,productCode,insureCompanyName,add_batch_code,del_batch_code,preservePolicyNo,4,holderName,insuredSubject,belongsRegional,
          firstPremium,numOfPresonFirstPolicy,0.0,0,policyStartDateRes)
      })
      .toDF("policy_id","policy_code","product_code","insure_company_name","add_batch_code","del_batch_code","preserve_id","premium_type","holder_name",
        "insured_subject","belongs_regional","add_premium","add_person_count","del_premium","del_person_count","day_id")
      .selectExpr("policy_id","policy_code","product_code","insure_company_name","add_batch_code","del_batch_code","preserve_id","premium_type","holder_name",
        "insured_subject","belongs_regional","cast(add_premium as decimal(14,4)) as add_premium","add_person_count","cast(del_premium as decimal(14,4)) as del_premium","del_person_count","day_id")

    /**
      * 续投
      */
    val renewPolicy = odsPolicyDetail
      .where("policy_status in (0,1,-1) and length(preserve_policy_no) > 0")
      .map(x => {
        val policyId = x.getAs[String]("policy_id")
        val policyCode = x.getAs[String]("policy_code")
        val productCode = x.getAs[String]("product_code")
        var firstPremium = x.getAs[java.math.BigDecimal]("first_premium")
        if(firstPremium == null){
          firstPremium = java.math.BigDecimal.valueOf(0.0)
        }
        val preservePolicyNo = x.getAs[String]("preserve_policy_no")
        val numOfPresonFirstPolicy = x.getAs[Int]("num_of_preson_first_policy")
        val policyStartDate = x.getAs[Timestamp]("policy_start_date")
        val insureCompanyName = x.getAs[String]("insure_company_name")
        val holderName = x.getAs[String]("holder_name")
        val insuredSubject = x.getAs[String]("insured_subject")
        val belongsRegional = x.getAs[String]("belongs_regional")
        var policyStartDateRes = ""
        if(policyStartDate != null && policyStartDate.toString.length > 10){
          policyStartDateRes = policyStartDate.toString.substring(0,10).replaceAll("-","")
        }else{
          policyStartDateRes = null
        }

        var add_batch_code = ""
        var del_batch_code = ""

        if(add_batch_code =="" &&del_batch_code==""){
          add_batch_code = null
          del_batch_code = null
        }

        (policyId,policyCode,productCode,insureCompanyName,add_batch_code,del_batch_code,preservePolicyNo,2,holderName,insuredSubject,belongsRegional,firstPremium,
          numOfPresonFirstPolicy,0.0,0,policyStartDateRes)
      })
      .toDF("policy_id","policy_code","product_code","insure_company_name","add_batch_code","del_batch_code","preserve_id","premium_type","holder_name",
        "insured_subject","belongs_regional","add_premium","add_person_count","del_premium","del_person_count","day_id")
      .selectExpr("policy_id","policy_code","product_code","insure_company_name","add_batch_code","del_batch_code","preserve_id","premium_type","holder_name",
        "insured_subject","belongs_regional","cast(add_premium as decimal(14,4)) as add_premium","add_person_count","cast(del_premium as decimal(14,4)) as del_premium","del_person_count","day_id")

    //renewPolicy.printSchema()
    /**
      * 保单中的退保  不算
      */
    val cancelPolicy = odsPolicyDetail
      .where("policy_status = -1")
      .map(x => {
        val policyId = x.getAs[String]("policy_id")
        val policyCode = x.getAs[String]("policy_code")
        var delPremium = x.getAs[Double]("first_premium")
        if(delPremium != null){
          delPremium = 0.0-delPremium
        }else{
          delPremium = 0.0
        }
        val preservePolicyNo = x.getAs[String]("preserve_policy_no")
        val numOfPresonFirstPolicy = x.getAs[Int]("num_of_preson_first_policy")
        val policyStartDate = x.getAs[Timestamp]("policy_start_date")
        val insureCompanyName = x.getAs[String]("insure_company_name")
        val holderName = x.getAs[String]("holder_name")
        val insuredSubject = x.getAs[String]("insured_subject")
        val belongsRegional = x.getAs[String]("belongs_regional")
        var policyStartDateRes = ""
        if(policyStartDate != null && policyStartDate.toString.length > 10){
          policyStartDateRes = policyStartDate.toString.substring(0,10).replaceAll("-","")
        }else{
          policyStartDateRes = null
        }

        var add_batch_code = ""
        var del_batch_code = ""

        if(add_batch_code =="" &&del_batch_code==""){
          add_batch_code = null
          del_batch_code = null
        }

        (policyId,policyCode,insureCompanyName,add_batch_code,del_batch_code,preservePolicyNo,3,holderName,insuredSubject,
          belongsRegional,0.0,0,delPremium,numOfPresonFirstPolicy,policyStartDateRes)
      })
      .toDF("policy_id","policy_code","insure_company_name","add_batch_code","del_batch_code","preserve_id","premium_type","holder_name","insured_subject","belongs_regional",
        "add_premium","add_person_count","del_premium","del_person_count","day_id")

    /**
      * 保全中的续投
      */
    val preserveReNewPremium = odsPolicyDetail.join(odsPreseveDetail,odsPolicyDetail("policy_id") === odsPreseveDetail("policy_id_preserve"))
      .where("preserve_status = 1 and preserve_type = 2")
      .selectExpr("policy_id","policy_code","product_code","insure_company_name","add_batch_code","del_batch_code","preserve_id","preserve_type as premium_type","holder_name",
        "insured_subject","belongs_regional","cast(case when add_premium is null then 0.0 else add_premium end as decimal(14,4)) as add_premium",
        "case when add_person_count is null then 0 else add_person_count end as add_person_count",
        "cast(case when del_premium is null then 0.0 else del_premium end as decimal(14,4)) as del_premium",
        "case when del_person_count is null then 0 else del_person_count end as del_person_count","preserve_effect_date as day_id")

   // preserveReNewPremium.printSchema()

    /**
      * 保全中的增减员
      */
    val preserveAddAndDelPremium = odsPolicyDetail.join(odsPreseveDetail,odsPolicyDetail("policy_id") === odsPreseveDetail("policy_id_preserve"))
      .where("preserve_status = 1 and preserve_type = 1 and preserve_id = '344093941244760064'")
      .selectExpr("policy_id","policy_code","product_code","insure_company_name","add_batch_code","del_batch_code","preserve_id","preserve_type as premium_type","holder_name",
        "insured_subject","belongs_regional","cast(case when add_premium is null then 0.0 else add_premium end as decimal(14,4)) as add_premium",
        "case when add_person_count is null then 0 else add_person_count end as add_person_count",
        "cast(case when del_premium is null then 0.0 else del_premium end as decimal(14,4)) as del_premium",
        "case when del_person_count is null then 0 else del_person_count end as del_person_count","preserve_effect_date as day_id")

   // preserveAddAndDelPremium.printSchema()
    /**
      * 保全类型中的退保
      */
    val preserveCancelPremium = odsPolicyDetail.join(odsPreseveDetail,odsPolicyDetail("policy_id") ===odsPreseveDetail("policy_id_preserve"))
      .where("preserve_type = 3 ")
      .selectExpr("policy_id","policy_code","product_code","insure_company_name","add_batch_code","del_batch_code","preserve_id","preserve_type as premium_type","holder_name",
        "insured_subject","belongs_regional","cast(case when add_premium is null then 0.0 else add_premium end as decimal(14,4)) as add_premium",
        "case when add_person_count is null then 0 else add_person_count end as add_person_count",
        "cast(case when del_premium is null then 0.0 else del_premium end as decimal(14,4)) as del_premium",
        "case when del_person_count is null then 0 else del_person_count end as del_person_count","preserve_effect_date as day_id")

    // preserveCancelPremium.printSchema()

    val res = newPolicy.unionAll(renewPolicy)
//      .unionAll(cancelPolicy)
      .unionAll(preserveReNewPremium)
      .unionAll(preserveAddAndDelPremium)
      .unionAll(preserveCancelPremium)
      .selectExpr("policy_id","policy_code","product_code","insure_company_name","add_batch_code","del_batch_code","preserve_id","premium_type",
        "holder_name","insured_subject","belongs_regional","add_premium","add_person_count","del_premium","del_person_count",
        "(add_person_count+del_person_count) as sum_preson","(add_premium+del_premium) as sum_premium","day_id")

   // res.printSchema()
    /**
      * 读取方案表
      */
    val odsPolicyProductPlan = sqlContext.sql("select policy_code as policy_code_plan,sku_coverage,sku_append,sku_ratio,sku_price," +
      "sku_charge_type,tech_service_rate,economic_rate,commission_discount_rate,commission_rate from odsdb.ods_policy_product_plan_detail")


    val resPlan = res.join(odsPolicyProductPlan,res("policy_code")===odsPolicyProductPlan("policy_code_plan"),"leftouter")
      .selectExpr("policy_id","policy_code","product_code","sku_coverage","sku_ratio","sku_append","sku_charge_type","sku_price","insure_company_name",
        "add_batch_code","del_batch_code","preserve_id","premium_type","holder_name","insured_subject","belongs_regional","commission_rate",
        "tech_service_rate","economic_rate","commission_discount_rate","add_premium","add_person_count","del_premium","del_person_count","sum_preson","sum_premium","day_id")

    /**
      * 读取销售数据
      */
    val odsSalesmanDetail = sqlContext.sql("select sale_name,team_name from odsdb.ods_salesman_detail")

    /**
      * 读取销售渠道表
      */
    val odsEntGuzhuSalesmanDetail = sqlContext.sql("select ent_id,ent_name,channel_name,salesman from odsdb.ods_ent_guzhu_salesman_detail")
    val entAndSale =
      odsEntGuzhuSalesmanDetail.join(odsSalesmanDetail,odsEntGuzhuSalesmanDetail("salesman")===odsSalesmanDetail("sale_name"),"leftouter")
      .selectExpr("ent_name","case when channel_name = '直客' then ent_name else channel_name end as channel_name","salesman","team_name")

    /**
      * 和渠道销售进行关联
      */
    val endRes = resPlan.join(entAndSale,resPlan("holder_name")===entAndSale("ent_name"),"leftouter")
      .selectExpr("policy_id","policy_code","product_code","sku_coverage","sku_ratio","sku_append","sku_charge_type","sku_price","insure_company_name",
        "add_batch_code","del_batch_code","preserve_id","premium_type","holder_name","insured_subject","salesman as sale_name","team_name",
        "belongs_regional","commission_rate", "tech_service_rate","economic_rate","commission_discount_rate","add_premium","add_person_count","del_premium","del_person_count",
        "sum_preson","sum_premium","day_id")

    endRes.where("preserve_id = '344093941244760064'").show(1000)
    val result = endRes.join(odsProductDetail,endRes("product_code")===odsProductDetail("product_code_slave"),"leftouter")
      .selectExpr("getUUID() as id","policy_id","policy_code","sku_coverage","sku_ratio","sku_append","sku_charge_type","sku_price","insure_company_name",
        "product_code","product_name","one_level_pdt_cate","two_level_pdt_cate","business_line","add_batch_code","del_batch_code",
        "preserve_id","premium_type","holder_name","insured_subject","sale_name","team_name","belongs_regional","commission_rate",
        "tech_service_rate","economic_rate","commission_discount_rate","add_premium","add_person_count","del_premium","del_person_count",
        "sum_preson","sum_premium","day_id","getNow() as dw_create_time")

    result
  }
}

