package bzn.dm.saleeasy

import java.text.SimpleDateFormat
import java.util.Date

import bzn.dm.util.SparkUtil
import bzn.job.common.{MysqlUntil, Until}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext

/*
* @Author:liuxiang
* @Date：2019/11/8
* @Describe:
*/ object DmSaleEasyEnterpriseInquireDetail extends SparkUtil with Until with MysqlUntil {

  /**
    * 获取配置信息
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "")

    val sc = sparkConf._2
    val hqlContext = sparkConf._4
    val res = CustomerBase(hqlContext)
    hqlContext.sql("truncate table dmdb.dm_saleeasy_enterprise_inquire_detail")
    res.write.mode(SaveMode.Append).saveAsTable("dmdb.dm_saleeasy_enterprise_inquire_detail")
    saveASMysqlTable(res, "dm_saleseasy_customer_base_detail", SaveMode.Overwrite,
      "mysql.username.106", "mysql.password.106", "mysql.driver", "mysql_url.106.dmdb")

    sc.stop()

  }


  def CustomerBase(hqlContext: HiveContext): DataFrame = {
    import hqlContext.implicits._
    hqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    hqlContext.udf.register("clean", (str: String) => clean(str))
    hqlContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      //设置日期格式
      val date = df.format(new Date()) // new Date()为获取当前系统时间
      (date + "")
    })

    //读取雇主基础信息表
    val dwCustomerHolder = hqlContext.sql("select holder_name,insure_company_name,ent_id,ent_name,channel_id,channel_name,sale_name as salesman,max(policy_start_date) as start_date,team_name," +
      "biz_operator,consumer_category,business_source,holder_province as province,holder_city as city" +
      " from dwdb.dw_employer_baseinfo_detail group by holder_name,ent_id,ent_name,channel_id,channel_name,sale_name,team_name,biz_operator," +
      "consumer_category,business_source,holder_province,holder_city,insure_company_name")
    //读取客户信息表
    val dwEmpBaseInfo = hqlContext.sql("select policy_id ,holder_name,channel_name,insure_company_name from dwdb.dw_employer_baseinfo_detail")

    //读取再保人表
    val policyCurrInsured = hqlContext.sql("select policy_id as id,day_id,count from dwdb.dw_policy_curr_insured_detail")

    /**
      * 渠道在保峰值,历史在保峰值
      */

    //客户信息表(保单级别) 关联再保人表

    val dwEmpBaseInfoAndInsured = dwEmpBaseInfo.join(policyCurrInsured, 'policy_id === 'id, "leftouter")
      .selectExpr("policy_id", "holder_name", "channel_name", "day_id", "count")
      .map(x => {
        val policyID = x.getAs[String]("policy_id")
        val holderName = x.getAs[String]("holder_name")
        val channelName = x.getAs[String]("channel_name")
        val dayID = x.getAs[String]("day_id")
        val count = x.getAs[Int]("count")
        //获取当前时间
        val nowTime = getNowTime().substring(0, 10).replaceAll("-", "")

        (policyID, holderName, channelName, dayID, count, nowTime)
      }).toDF("policy_id", "holder_name_salve", "holder_company", "day_id", "curr_insured", "now_time")
      .where("day_id <= now_time")

    dwEmpBaseInfoAndInsured.registerTempTable("customerAndCurrInsured")
    val topChannelInsured = hqlContext.sql("select day_id,sum(curr_insured) as curr_insured, holder_company from customerAndCurrInsured group by day_id,holder_company")

    topChannelInsured.registerTempTable("TopChannelInsuredEveryDay")

    val channelInsuredOfTop = hqlContext.sql("select holder_company,max(curr_insured) as channel_curr_insured_top from TopChannelInsuredEveryDay group by holder_company")


    //历史在保峰值

    val insuredEverydDay = hqlContext.sql("select holder_name_salve,sum(curr_insured) as count,day_id from customerAndCurrInsured group by holder_name_salve,day_id ")
    insuredEverydDay.registerTempTable("InsuredEverydDay")
    val historyInsuredOfTop = hqlContext.sql("select max(count) as history_top_insured,holder_name_salve from InsuredEverydDay group by holder_name_salve")

    //累计投保人次

    //客户信息表(保单级别) 关联再保人表

    val dwEmpBaseInfoAndInsuredSalve = dwEmpBaseInfo.join(policyCurrInsured, 'policy_id === 'id, "leftouter")
      .selectExpr("policy_id", "holder_name", "insure_company_name", "channel_name", "day_id", "count")
      .map(x => {
        val policyID = x.getAs[String]("policy_id")
        val holderName = x.getAs[String]("holder_name")
        val channelName = x.getAs[String]("channel_name")
        val companyName = x.getAs[String]("insure_company_name")
        val dayID = x.getAs[String]("day_id")
        val count = x.getAs[Int]("count")
        //获取当前时间
        val nowTime = getNowTime().substring(0, 10).replaceAll("-", "")

        (policyID, holderName, channelName, dayID, count, companyName, nowTime)
      }).toDF("policy_id", "holder_name_salve", "holder_company", "day_id", "curr_insured", "company_name", "now_time")
      .where("day_id <= now_time")

    dwEmpBaseInfoAndInsuredSalve.registerTempTable("EmpBaseInfoAndInsuredSalve")

    val addupCounts = hqlContext.sql("select holder_name_salve,sum(curr_insured) as curr_insured_counts,company_name from EmpBaseInfoAndInsuredSalve group by holder_name_salve,company_name")

    /**
      * 已赚保费
      */

    //读取每日已赚保费

    val dwPolicyPremium = hqlContext.sql("select policy_id as id,day_id,premium from dwdb.dw_policy_everyday_premium_detail")

    val dwEmpBaseInfoAndPremiumSalve = dwEmpBaseInfo.join(dwPolicyPremium, 'policy_id === 'id, "leftouter")
      .selectExpr("policy_id", "holder_name", "insure_company_name", "channel_name", "day_id", "premium")
      .map(x => {
        val policyID = x.getAs[String]("policy_id")
        val holderName = x.getAs[String]("holder_name")
        val channelName = x.getAs[String]("channel_name")
        val companyName = x.getAs[String]("insure_company_name")
        val dayID = x.getAs[String]("day_id")
        val premium = x.getAs[java.math.BigDecimal]("premium")
        //获取当前时间
        val nowTime = getNowTime().substring(0, 10).replaceAll("-", "")

        (policyID, holderName, channelName, dayID, premium, companyName, nowTime)
      }).toDF("policy_id", "holder_name_salve", "holder_company", "day_id", "premium", "company_name", "now_time")
      .where("day_id <= now_time")

    dwEmpBaseInfoAndPremiumSalve.registerTempTable("EmpBaseInfoAndPremiumSalve")

    val sumPremium = hqlContext.sql("select holder_name_salve,cast(sum(premium) as decimal(14,4)) as charged_premium,company_name from EmpBaseInfoAndPremiumSalve group by holder_name_salve,company_name")


    //在保人数
    val dwEmpBaseInfoAndInsuredRes = dwEmpBaseInfo.join(policyCurrInsured, 'policy_id === 'id, "leftouter")
      .selectExpr("policy_id", "holder_name", "insure_company_name", "channel_name", "day_id", "count")
      .map(x => {
        val policyID = x.getAs[String]("policy_id")
        val holderName = x.getAs[String]("holder_name")
        val channelName = x.getAs[String]("channel_name")
        val companyName = x.getAs[String]("insure_company_name")
        val dayID = x.getAs[String]("day_id")
        val count = x.getAs[Int]("count")
        //获取当前时间
        val nowTime = getNowTime().substring(0, 10).replaceAll("-", "")

        (policyID, holderName, channelName, dayID, count, companyName, nowTime)
      }).toDF("policy_id", "holder_name_salve", "holder_company", "day_id", "curr_insured", "company_name", "now_time")
      .where("day_id = now_time")

    dwEmpBaseInfoAndInsuredRes.registerTempTable("EmpBaseInfoAndInsuredRes")

    val insuredIntraday = hqlContext.sql("select holder_name_salve,company_name,sum(curr_insured) as curr_insured_count from EmpBaseInfoAndInsuredRes group by company_name,holder_name_salve")

    //读取理赔表
    val policyClaim = hqlContext.sql("select policy_id as id,ent_name,ent_id,disable_level,case_type,case_status,res_pay,case_no from dwdb.dw_policy_claim_detail")

    //读取客户信息表(保单级别)关联理赔表拿到保险公司
    val policyAndClaim = dwEmpBaseInfo.join(policyClaim, 'policy_id === 'id, "leftouter")
      .selectExpr("insure_company_name", "holder_name as holder_name_salve", "ent_name", "disable_level", "case_type", "case_status", "res_pay", "case_no")

    policyAndClaim.registerTempTable("policyAndClaimTemp")

    //读取理赔表,拿出企业级别的预估赔付
    val odsClaimDetail = hqlContext.sql("select cast(sum(res_pay) as decimal(14,4)) as res_pay,holder_name_salve ,insure_company_name as insure_company from policyAndClaimTemp group by holder_name_salve,insure_company_name")

    //拿到企业级别的案件号
    val caseCounts = hqlContext.sql("select count(case_no) as case_no_counts,holder_name_salve,insure_company_name as company_name from policyAndClaimTemp group by holder_name_salve,insure_company_name")

    //拿到企业级别的伤残案件数 employer_liability_claims 表中 disable_level>0 的案件个数

    val disableLevel = hqlContext.sql("select holder_name_salve,count(disable_level) as disable_level_counts,insure_company_name as company_name1 from policyAndClaimTemp " +
      "where disable_level>'0' and disable_level IS NOT NULL and disable_level !='死亡'  GROUP BY holder_name_salve,insure_company_name")

    //拿到企业级别的死亡案件数

    val dieLevel = hqlContext.sql("SELECT holder_name_salve,count(case_type) as die_case_counts ,insure_company_name as company_name2 from policyAndClaimTemp WHERE case_type = '死亡' GROUP BY holder_name_salve,insure_company_name")

    //企业级别的结案案件数
    val finalLevel = hqlContext.sql("SELECT holder_name_salve,count(case_status) as final_case_counts,insure_company_name as company_name3 " +
      "from policyAndClaimTemp WHERE  case_status IS NOT NULL GROUP BY holder_name_salve,insure_company_name")


    // 关联在保人数

    val resTemp1 = dwCustomerHolder.join(insuredIntraday,'holder_name==='holder_name_salve and 'insure_company_name==='company_name,"leftouter")
      .selectExpr("ent_id","ent_name","holder_name","channel_id","channel_name","start_date","province","city","salesman","team_name","biz_operator",
        "consumer_category","business_source","insure_company_name","curr_insured_count")

    //关联历史在保峰值
    val resTemp2 = resTemp1.join(historyInsuredOfTop, 'holder_name === 'holder_name_salve, "leftouter")
      .selectExpr("ent_id", "ent_name", "holder_name", "channel_id", "channel_name", "start_date", "province", "city", "salesman", "team_name", "biz_operator",
        "consumer_category", "business_source", "insure_company_name", "curr_insured_count", "history_top_insured")

    //关联渠道在保峰值
    val resTemp3 = resTemp2.join(channelInsuredOfTop, 'channel_name === 'holder_company, "leftouter")
      .selectExpr("ent_id", "ent_name", "holder_name", "channel_id", "channel_name", "start_date", "province", "city", "salesman", "team_name", "biz_operator",
        "consumer_category", "business_source", "insure_company_name", "curr_insured_count", "history_top_insured", "channel_curr_insured_top")

    //关联已赚保费
    val resTemp4 = resTemp3.join(sumPremium, 'holder_name === 'holder_name_salve and 'insure_company_name === 'company_name,"leftouter")
      .selectExpr("ent_id", "ent_name", "holder_name", "channel_id", "channel_name", "start_date", "province", "city", "salesman", "team_name", "biz_operator",
        "consumer_category", "business_source", "insure_company_name", "curr_insured_count", "history_top_insured", "channel_curr_insured_top", "charged_premium")

    //关联预估赔付

    val resTemp5 = resTemp4.join(odsClaimDetail, 'holder_name === 'holder_name_salve and 'insure_company_name === 'insure_company, "leftouter")
      .selectExpr("ent_id", "ent_name", "holder_name", "channel_id", "channel_name", "start_date", "province", "city", "salesman", "team_name", "biz_operator",
        "consumer_category", "business_source", "insure_company_name", "curr_insured_count", "history_top_insured", "channel_curr_insured_top", "charged_premium", "res_pay")

    //关联累计投保人次
    val resTemp6 = resTemp5.join(addupCounts, 'holder_name === 'holder_name_salve and 'insure_company_name === 'company_name, "leftouter")
      .selectExpr("ent_id", "ent_name", "holder_name", "channel_id", "channel_name", "start_date", "province", "city", "salesman", "team_name", "biz_operator",
        "consumer_category", "business_source", "insure_company_name", "curr_insured_count", "history_top_insured", "channel_curr_insured_top", "charged_premium", "res_pay", "curr_insured_counts")

    // 关联 拿到企业级别的案件数
    val resTemp8 = resTemp6.join(caseCounts, 'holder_name === 'holder_name_salve and 'insure_company_name === 'company_name, "leftouter")
      .selectExpr("ent_id", "ent_name", "holder_name", "channel_id", "channel_name", "start_date", "province", "city", "salesman", "team_name", "biz_operator",
        "consumer_category", "business_source", "insure_company_name", "curr_insured_count", "history_top_insured", "channel_curr_insured_top", "charged_premium", "res_pay", "curr_insured_counts", "case_no_counts")


    val resTemp9 = resTemp8.join(disableLevel, 'holder_name === 'holder_name_salve and 'insure_company_name === 'company_name1, "leftouter")
      .selectExpr("ent_id", "ent_name", "holder_name", "channel_id", "channel_name", "start_date", "province", "city", "salesman", "team_name", "biz_operator",
        "consumer_category", "business_source", "insure_company_name", "curr_insured_count", "history_top_insured", "channel_curr_insured_top", "charged_premium", "res_pay", "curr_insured_counts", "case_no_counts",
        "disable_level_counts")

    val resTemp10 = resTemp9.join(dieLevel, 'holder_name === 'holder_name_salve and 'insure_company_name === 'company_name2, "leftouter")
      .selectExpr("ent_id", "ent_name", "holder_name", "channel_id", "channel_name", "start_date", "province", "city", "salesman", "team_name", "biz_operator",
        "consumer_category", "business_source", "insure_company_name", "curr_insured_count", "history_top_insured", "channel_curr_insured_top", "charged_premium", "res_pay", "curr_insured_counts", "case_no_counts",
        "disable_level_counts", "die_case_counts")


    val resTemp11 = resTemp10.join(finalLevel, 'holder_name === 'holder_name_salve and 'insure_company_name === 'company_name3, "leftouter")
      .selectExpr("ent_id", "ent_name", "holder_name", "channel_id", "channel_name", "start_date", "province", "city", "salesman", "team_name", "biz_operator",
        "consumer_category", "business_source", "insure_company_name", "curr_insured_count", "history_top_insured", "channel_curr_insured_top", "charged_premium", "res_pay", "curr_insured_counts", "case_no_counts",
        "disable_level_counts", "die_case_counts", "final_case_counts")


    val resTemp12 = resTemp11.selectExpr(
      "getUUID() as id",
      "ent_id",
      "ent_name",
      "holder_name",
      "channel_id",
      "channel_name",
      "substring(channel_name,1,5) as channel_short_name",
      "start_date",
      "province",
      "case when city ='市辖区' then province else city end as city",
      "salesman",
      "team_name",
      "biz_operator",
      "consumer_category",
      "business_source",
      "insure_company_name",
      "cast(curr_insured_count as int) as curr_insured_count", //在保人数
      "cast(history_top_insured as int)  as history_top_insured", //历史在保峰值
      "cast(channel_curr_insured_top as int) as channel_curr_insured_top", //渠道在保峰值
      "charged_premium", //已赚保费
      "res_pay", //预估赔付
      "round(cast(res_pay as DOUBLE)/cast(charged_premium  as DOUBLE),5) as loss_ration", //赔付率
      "cast(curr_insured_counts as int) as total_insured_counts", //累计投保人次
      "cast(case_no_counts as int) as case_no_counts", //案件数
      "round(cast(case_no_counts*365 as DOUBLE)/cast((curr_insured_counts) as DOUBLE),5) as risk_ration", //出险率
      "cast(disable_level_counts as int) as disable_level_counts", //伤残
      "cast(die_case_counts as int) as die_case_counts", //死亡
      "cast(final_case_counts as int) as final_case_counts",//总案件数
      "getNow() as create_time",
      "getNow() as update_time"
    )


    val finalRes = resTemp12.selectExpr(
      "id",
      "ent_id",
      "holder_name",
      "channel_id",
      "channel_name",
      "channel_short_name ",
      "consumer_category",
      "business_source",
      "insure_company_name",
      "province",
      "city",
      "salesman",
      "team_name",
      "biz_operator",
      "start_date",
      "case when curr_insured_count is null then 0 else curr_insured_count end as curr_insured_counts", //在保人数
      "case when history_top_insured is null then 0 else history_top_insured end as top_history_curr_insured", //历史在保峰值
      "case when channel_curr_insured_top is null then 0 else channel_curr_insured_top end as top_channel_curr_insured ", //渠道在保峰值
      "case when charged_premium is null then 0 else charged_premium end as charged_premium", //已赚保费
      "case when res_pay is null then 0 else res_pay end as res_pay",//预估赔付
      "case when loss_ration is null then 0 else loss_ration end as loss_ration", // 赔付率
      "case when total_insured_counts is null then 0 else total_insured_counts end as total_insured_counts ", //累计投保人次
      "case when case_no_counts is null then 0 else case_no_counts end as case_no_counts",//案件数
      "case when risk_ration is null then 0 else risk_ration end as risk_ration",//出险率
      "case when disable_level_counts is null then 0 else disable_level_counts end as disable_level_counts",//伤残案件数
      "case when die_case_counts is null then 0 else die_case_counts end as die_case_counts",//死亡案件数
      "case when final_case_counts is null then 0 else final_case_counts end as final_case_counts",//已结案案件数
      "create_time",
      "update_time"
    )

    finalRes

  }

}