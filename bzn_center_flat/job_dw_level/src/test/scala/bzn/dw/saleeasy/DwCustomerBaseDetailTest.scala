package bzn.dw.saleeasy

import bzn.dw.saleeasy.DwSaleEasyDetailTest.sparkConfInfo
import bzn.dw.util.SparkUtil
import bzn.job.common.Until
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

/*
* @Author:liuxiang
* @Date：2019/10/16
* @Describe:客户基础信息表
*/ object DwCustomerBaseDetailTest extends SparkUtil with  Until{


  /**
    *  获取配置信息
    * @param args
    */
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "local[*]")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    val res = CustomerBase(hiveContext)

  }

  /**
    *
    * @param hqlContext
    */


  def  CustomerBase(hqlContext:HiveContext): Unit ={
    import hqlContext.implicits._

    // 读取企业信息表
    val odsEnterprise = hqlContext.sql("select ent_id,ent_name,office_province,office_city from odsdb.ods_enterprise_detail")

    //读取客户归属信息表
    val odsEntGuzhuSalesman = hqlContext.sql("select channel_id,channel_name,ent_id as entId,ent_name as entName,salesman,biz_operator,consumer_category,business_source from odsdb.ods_ent_guzhu_salesman_detail")

    //读取销售团队表,拿到team
    val odsEntSaleTeam = hqlContext.sql("select sale_name,team_name from odsdb.ods_ent_sales_team_dimension ")

    //客户归属信息表关联销售团队表
    val saleAndTeamRes = odsEntGuzhuSalesman.join(odsEntSaleTeam, odsEntGuzhuSalesman("salesman") === odsEntSaleTeam("sale_name"), "leftouter")
      .selectExpr("channel_id", "channel_name", "entId", "entName", "salesman", "team_name", "biz_operator", "consumer_category", "business_source")
    //saleAndTeamRes.show(10)


    // 读取保单明细表
    val odsPolicyDetail = hqlContext.sql("select policy_id ,product_code,holder_name,order_date,policy_status,insure_company_name,policy_start_date,belongs_regional from odsdb.ods_policy_detail")


    val policyAndSale = odsPolicyDetail.join(saleAndTeamRes,odsPolicyDetail("holder_name")===saleAndTeamRes("entName"),"leftouter")
      .selectExpr("entId","holder_name","policy_start_date")
    policyAndSale.show(10)

    //注册临时表

    policyAndSale.registerTempTable("policyAndSaleTemp")

    val PolicyTemp = hqlContext.sql("select entId as ent_id,holder_name,min(policy_start_date) as start_date from policyAndSaleTemp group by entId,holder_name")

    PolicyTemp.show(10)

    odsPolicyDetail.show(10)
   // 读取在保人信息表

    val policyCurrInsured = hqlContext.sql("select policy_id as id,day_id,count from dwdb.dw_policy_curr_insured_detail")

    //读取产品表
    val odsProduct = hqlContext.sql("select product_code as insure_code,two_level_pdt_cate from odsdb.ods_product_detail")

    /**
      * 渠道在保峰值
      */

     //在保人表关联保单明细表
    val res1 = policyCurrInsured.join(odsPolicyDetail, policyCurrInsured("id") === odsPolicyDetail("policy_id"), "leftouter")
      .selectExpr( "id","product_code", "holder_name", "policy_status", "day_id","count")


    //将结果关联产品明细表
    val res2 = res1.join(odsProduct, res1("product_code") === odsProduct("insure_code"), "leftouter")
      .selectExpr("id", "product_code", "holder_name", "policy_status", "day_id","count", "two_level_pdt_cate")

    // 关联读取客户归属信息表
    val res3 = res2.join(odsEntGuzhuSalesman, res2("holder_name") === odsEntGuzhuSalesman("entName"), "leftouter")
      .selectExpr("id", "product_code", "holder_name", "policy_status", "day_id","count", "two_level_pdt_cate", "channel_name", "entName","salesman")
      .map(x => {
        val policyID = x.getAs[String]("id")
        val productCode = x.getAs[String]("product_code")
        val holderName = x.getAs[String]("holder_name")
        val policyStatus = x.getAs[Int]("policy_status")
        val dayID = x.getAs[String]("day_id")
        val count = x.getAs[Int]("count")
        val twoLevel = x.getAs[String]("two_level_pdt_cate")
        val channelName = x.getAs[String]("channel_name")
        val entName = x.getAs[String]("entName")
        val saleMan = x.getAs[String]("salesman")

        //获取当前时间
        val nowTime = getNowTime().substring(0, 10).replaceAll("-", "")
        (policyID, productCode, holderName, policyStatus, dayID,count ,twoLevel, channelName, entName,saleMan, nowTime)

      }).toDF("policy_id", "product_code", "holder_name", "policy_status", "day_id","curr_insured" ,"two_level_pdt_cate", "channel_name", "ent_name", "salesman","now_time")
        .where("two_level_pdt_cate in ('外包雇主','骑士保','大货车') and policy_status in (0,1) and day_id <= now_time")

    //将res3注册成临时表
    val res4 = res3.selectExpr("policy_id", "product_code", "holder_name", "policy_status",
      "day_id", "curr_insured", "two_level_pdt_cate", "case when trim(channel_name) ='直客' then trim(ent_name) else trim(channel_name) end as holder_company ", "ent_name", "salesman", "now_time")
    res4.registerTempTable("resTemp")

    val res5 = hqlContext.sql("select day_id,sum(curr_insured) as curr_insured, holder_company, salesman from resTemp group by day_id,holder_company,salesman")

    //将res5 注册成临时表
    res5.registerTempTable("res_temp")
    val res6 = hqlContext.sql("select holder_company,salesman as sales_man ,max(curr_insured) as channel_curr_insured from res_temp group by holder_company, salesman")

    /**
      * 历史在保峰值
      */
    //再保人表关联保单明细表  拿到投保人
    val df1 = policyCurrInsured.join(odsPolicyDetail, policyCurrInsured("id") === odsPolicyDetail("policy_id"), "leftouter")
      .selectExpr("policy_id", "product_code", "holder_name", "policy_status", "day_id", "count")

    //将上述结果关联产品表 过滤出最新信息和 '外包雇主','骑士保','大货车'

    val df3 = df1.join(odsProduct, df1("product_code") === odsProduct("insure_code"), "leftouter")
      .selectExpr("policy_id", "product_code", "holder_name", "policy_status", "day_id", "count", "two_level_pdt_cate")
      .map(x => {
        val policyID = x.getAs[String]("policy_id")
        val productCode = x.getAs[String]("product_code")
        val holderName = x.getAs[String]("holder_name")
        val policyStatus = x.getAs[Int]("policy_status")
        val dayID = x.getAs[String]("day_id")
        val count = x.getAs[Int]("count")
        val twoLevel = x.getAs[String]("two_level_pdt_cate")
        val nowTime = getNowTime().substring(0, 10).replaceAll("-", "")
        (policyID, productCode, holderName, policyStatus, dayID, count, twoLevel, nowTime)

      }).toDF("policy_id", "product_code", "holder_name", "policy_status", "day_id", "curr_insured", "two_level_pdt_cate", "now_time")
        .where("two_level_pdt_cate in ('外包雇主','骑士保','大货车') and policy_status in (0,1) and day_id <= now_time")

    //将df3 注册成临时表
    df3.registerTempTable("dfTemp")

    val df4 = hqlContext.sql("select day_id,max(curr_insured) as counts,holder_name from dfTemp group by day_id,holder_name")

    /**
      * 已赚保费
      */

    //读取每日已赚保费

    val dwPolicyPremium = hqlContext.sql("select policy_id as id,day_id,premium from dwdb.dw_policy_everyday_premium_detail")

    //保单明细表关联产品表
    val tem1 = odsPolicyDetail.join(odsProduct, odsPolicyDetail("product_code") === odsProduct("insure_code"), "leftouter")
      .selectExpr("policy_id", "product_code", "holder_name", "policy_status","two_level_pdt_cate")

    //将上述结果关联每日已赚保费表

    val tem2 = tem1.join(dwPolicyPremium, tem1("policy_id") === dwPolicyPremium("id"), "leftouter")
      .selectExpr("policy_id", "product_code", "holder_name", "policy_status", "two_level_pdt_cate", "day_id", "premium")
      .map(x => {
        val policyID = x.getAs[String]("policy_id")
        val productCode = x.getAs[String]("product_code")
        val holderName = x.getAs[String]("holder_name")
        val policyStatus = x.getAs[Int]("policy_status")
        val twoLevel = x.getAs[String]("two_level_pdt_cate")
        val dayID = x.getAs[String]("day_id")
        val premium = x.getAs[java.math.BigDecimal]("premium")
        val nowTime = getNowTime().substring(0, 10).replaceAll("-", "")
        (policyID, productCode, holderName, policyStatus, twoLevel, dayID, premium, nowTime)

      }).toDF("policy_id", "product_code", "holder_name", "policy_status", "two_level_pdt_cate", "day_id", "premium", "now_time")
      .where("two_level_pdt_cate in ('外包雇主','骑士保','大货车') and policy_status in (0,1) and day_id <= now_time")

    tem2.registerTempTable("peimiumTemp")

    val tem3 = hqlContext.sql("select holder_name,cast(sum(premium) as decimal(14,4)) as premium from peimiumTemp group by holder_name ")

    /**
      * 在保人数
      */

    val Dframe = tem1.join(policyCurrInsured,tem1("policy_id")===policyCurrInsured("id"),"leftouter")
      .selectExpr("policy_id", "product_code", "holder_name", "policy_status", "two_level_pdt_cate", "day_id", "count")
      .map(x => {
        val policyID = x.getAs[String]("policy_id")
        val productCode = x.getAs[String]("product_code")
        val holderName = x.getAs[String]("holder_name")
        val policyStatus = x.getAs[Int]("policy_status")
        val twoLevel = x.getAs[String]("two_level_pdt_cate")
        val dayID = x.getAs[String]("day_id")
        val premium = x.getAs[Int]("count")
        val nowTime = getNowTime().substring(0, 10).replaceAll("-", "")
        (policyID, productCode, holderName, policyStatus, twoLevel, dayID, premium, nowTime)
      }).toDF("policy_id", "product_code", "holder_name", "policy_status", "two_level_pdt_cate", "day_id", "curr_insured", "now_time")
      .where("two_level_pdt_cate in ('外包雇主','骑士保','大货车') and policy_status in (0,1) and day_id = now_time")

     Dframe.registerTempTable("currInsureTemp")

    val currInsureRes = hqlContext.sql("select holder_name,sum(curr_insured) as curr_insured from currInsureTemp group by holder_name ")

    /**
      * 累计投保人次
      */

    //再保人表关联保单明细表
    val currInsuredAndPolicy = policyCurrInsured.join(odsPolicyDetail, policyCurrInsured("id") === odsPolicyDetail("policy_id"), "leftouter")
      .selectExpr( "policy_id","product_code", "holder_name", "policy_status", "day_id","count")

    val currInsuredCount = currInsuredAndPolicy.join(odsProduct, currInsuredAndPolicy("product_code") === odsProduct("insure_code"), "leftouter")
      .selectExpr("policy_id", "product_code", "holder_name", "policy_status", "day_id", "count","two_level_pdt_cate")
      .map(x => {
        val policyID = x.getAs[String]("policy_id")
        val productCode = x.getAs[String]("product_code")
        val holderName = x.getAs[String]("holder_name")
        val policyStatus = x.getAs[Int]("policy_status")
        val dayID = x.getAs[String]("day_id")
        val count = x.getAs[Int]("count")
        val twoLevel = x.getAs[String]("two_level_pdt_cate")  //	two_level_pdt_cate
        val nowTime = getNowTime().substring(0, 10).replaceAll("-", "")
        (policyID, productCode, holderName, policyStatus, twoLevel, dayID, count, nowTime)
      }).toDF("policy_id", "product_code", "holder_name", "policy_status", "two_level_pdt_cate", "day_id", "curr_insured", "now_time")
      .where("two_level_pdt_cate in ('外包雇主','骑士保','大货车') and policy_status in (0,1) and day_id <= now_time")

       currInsuredCount.registerTempTable("currInsuredCountsTemp")


    val currInsuredCounts = hqlContext.sql("select holder_name,sum(curr_insured) as curr_insured_counts from currInsuredCountsTemp group by holder_name")



    //读取理赔表
    val policyClaim = hqlContext.sql("select policy_id as id,ent_name,ent_id,disable_level,case_type,case_status,res_pay,case_no from dwdb.dw_policy_claim_detail")

    //保单明细表关联理赔表 拿到保险公司

    val policyAndClaim = odsPolicyDetail.join(policyClaim, odsPolicyDetail("policy_id") === policyClaim("id"), "leftouter")
      .selectExpr("policy_id", "insure_company_name", "holder_name", "ent_name", "disable_level", "case_type", "case_status", "res_pay","case_no")
    policyAndClaim.registerTempTable("policyAndClaimTemp")


    //读取理赔表,拿出企业级别的预估赔付
    val odsClaimDetail = hqlContext.sql("select sum(res_pay) as res_pay,ent_name,insure_company_name from policyAndClaimTemp group by ent_name,insure_company_name")

    //拿到企业级别的案件号
    val caseCounts = hqlContext.sql("select count(case_no) as case_no_counts,ent_name,insure_company_name as company_name from policyAndClaimTemp group by ent_name,insure_company_name")


    //拿到企业级别的伤残案件数 employer_liability_claims 表中 disable_level>0 的案件个数

    val disableLevel = hqlContext.sql("select ent_name,count(disable_level) as disable_level_counts,insure_company_name as company_name1 from policyAndClaimTemp " +
      "where disable_level>'0' and disable_level IS NOT NULL and disable_level !='死亡'  GROUP BY ent_name,insure_company_name ")

    //拿到企业级别的死亡案件数

   val  dieLevel=  hqlContext.sql("SELECT ent_name,count(case_type) as die_case_counts ,insure_company_name as company_name2 from policyAndClaimTemp WHERE case_type = '死亡' GROUP BY ent_name,insure_company_name")

    //企业级别的结案案件数

    val finalLevel = hqlContext.sql("SELECT ent_name,count(case_type) as final_case_counts,insure_company_name as company_name3  from policyAndClaimTemp WHERE  case_status IS NOT NULL GROUP BY ent_name,insure_company_name")

    finalLevel.printSchema()

    //保单明细表关联企业信息表  拿到城市信息
   // val policyAndEnter = odsPolicyDetail.join(odsEnterprise, odsPolicyDetail("holder_name") === odsEnterprise("ent_name"), "leftouter")

/*

    //读取城市码表

    val odsArea = hqlContext.sql("select code,province, short_name from odsdb.ods_area_info_dimension")

    policyAndEnter.join(odsArea,policyAndEnter("office_province")===odsArea("code"),"leftouter")
      .map(x=>{





      })




*/
               //保单明细表 关联saleAndTeamRes


    //保单分组后的表关联 体育销售渠道团队表

    val resTemp1 = PolicyTemp.join(saleAndTeamRes, PolicyTemp("ent_id") === saleAndTeamRes("entId"), "leftouter")
      .selectExpr("ent_id", "holder_name as holderName", "start_date", "channel_id", "channel_name", "entId", "entName", "salesman", "team_name", "biz_operator", "consumer_category", "business_source")


    //resTemp1 关联在保人数

    val resTemp2 = resTemp1.join(currInsureRes, resTemp1("holderName") === currInsureRes("holder_name"), "leftouter")
      .selectExpr("ent_id", "holderName", "start_date", "channel_id", "channel_name", "entId", "entName", "salesman", "team_name", "biz_operator", "consumer_category", "business_source", "curr_insured")


    //resTemp2 关联历史在保峰值
    val resTemp3 = resTemp2.join(df4, resTemp2("holderName") === df4("holder_name"), "leftouter")
      .selectExpr("ent_id", "holderName", "start_date", "channel_id", "channel_name", "entId", "entName", "salesman", "team_name", "biz_operator", "consumer_category", "business_source", "curr_insured", "counts")


    //resTemp3 关联渠道在保峰值
    val resTemp4 = resTemp3.join(res6, resTemp3("channel_name") === res6("holder_company"), "leftouter")
      .selectExpr("ent_id", "holderName", "start_date", "channel_id", "channel_name", "entId", "entName", "salesman", "team_name", "biz_operator", "consumer_category", "business_source", "curr_insured", "counts", "channel_curr_insured")


    //resTemp4 关联已赚保费
    val resTemp5 = resTemp4.join(tem3, resTemp4("holderName") === tem3("holder_name"), "leftouter")
      .selectExpr("ent_id", "holderName", "start_date", "channel_id", "channel_name", "entId", "entName", "salesman", "team_name", "biz_operator", "consumer_category", "business_source", "curr_insured", "counts", "channel_curr_insured", "premium")


    //resTemp5 关联预估赔付
    val resTemp6 = resTemp5.join(odsClaimDetail, resTemp5("holderName") === odsClaimDetail("ent_name"), "leftouter")
      .selectExpr("ent_id", "holderName", "start_date", "channel_id", "channel_name", "entId", "entName", "salesman", "team_name", "biz_operator",
        "consumer_category", "business_source", "curr_insured", "counts", "channel_curr_insured", "premium", "res_pay", "insure_company_name")

    //resTemp6 关联累计投保人次
    val resTemp7 = resTemp6.join(currInsuredCounts, resTemp6("holderName") === currInsuredCounts("holder_name"), "leftouter")
      .selectExpr("ent_id", "holderName", "start_date", "channel_id", "channel_name", "entId", "entName", "salesman", "team_name", "biz_operator",
        "consumer_category", "business_source", "curr_insured", "counts", "channel_curr_insured", "premium", "res_pay", "insure_company_name", "curr_insured_counts")

    //resTemp7 关联 拿到企业级别的案件数
    val resTemp8 = resTemp7.join(caseCounts, resTemp7("holderName") === caseCounts("ent_name"), "leftouter")
      .selectExpr("ent_id", "holderName", "start_date", "channel_id", "channel_name", "entId", "entName", "salesman", "team_name", "biz_operator",
        "consumer_category", "business_source", "curr_insured", "counts", "channel_curr_insured", "premium", "res_pay", "insure_company_name", "curr_insured_counts", "case_no_counts")


    val resTemp9 = resTemp8.join(disableLevel, resTemp8("holderName") === disableLevel("ent_name"), "leftouter")
      .selectExpr("ent_id", "holderName", "start_date", "channel_id", "channel_name", "entId", "entName", "salesman", "team_name", "biz_operator",
        "consumer_category", "business_source", "curr_insured", "counts", "channel_curr_insured", "premium", "res_pay", "insure_company_name", "curr_insured_counts", "case_no_counts","disable_level_counts")
    val resTemp10 = resTemp9.join(dieLevel, resTemp9("holderName") === dieLevel("ent_name"), "leftouter")
      .selectExpr("ent_id", "holderName", "start_date", "channel_id", "channel_name", "entId", "entName", "salesman", "team_name", "biz_operator",
        "consumer_category", "business_source", "curr_insured", "counts", "channel_curr_insured", "premium", "res_pay", "insure_company_name", "curr_insured_counts", "case_no_counts", "disable_level_counts", "die_case_counts")


    val resTemp11 =  resTemp10.join(finalLevel,resTemp10("holderName")===finalLevel("ent_name"),"leftouter")
    .selectExpr("ent_id", "holderName", "start_date", "channel_id", "channel_name", "entId", "entName", "salesman", "team_name", "biz_operator",
      "consumer_category", "business_source", "curr_insured", "counts", "channel_curr_insured", "premium", "res_pay", "insure_company_name", "curr_insured_counts", "case_no_counts", "disable_level_counts", "die_case_counts", "final_case_counts")


    resTemp11.registerTempTable("finalResTemp")
    val finalRes = hqlContext.sql(" select ent_id, holderName, start_date, channel_id, channel_name, entId, entName, salesman, team_name,biz_operator, consumer_category," +
      " business_source, curr_insured, counts, channel_curr_insured, premium, res_pay, round(cast(res_pay as DOUBLE)/cast(premium  as DOUBLE),5) as loss_ration ,insure_company_name, curr_insured_counts, case_no_counts," +
      "round(cast(case_no_counts as DOUBLE)/cast(curr_insured_counts *365 as DOUBLE),5) as risk_ration ,disable_level_counts, die_case_counts, final_case_counts from finalResTemp")
    finalRes.printSchema()

  }


}
