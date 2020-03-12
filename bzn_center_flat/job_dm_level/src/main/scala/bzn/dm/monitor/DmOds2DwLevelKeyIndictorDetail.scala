package bzn.dm.monitor

import bzn.dm.util.SparkUtil
import bzn.job.common.DataBaseUtil
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * author:xiaoYuanRen
  * Date:2020/2/20
  * Time:15:49
  * describe: ods - dm 关键指标一致性统计
  **/
object DmOds2DwLevelKeyIndictorDetail extends SparkUtil with DataBaseUtil{
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4

    val dmData = getOds2DmlevelKeyData(hiveContext)

    val ods2DwMonitorTable = "dm_monitor_ods2dw_level_key_indictor_detail"
    val user106 = "mysql.username.106"
    val pass106 = "mysql.password.106"
    val url106 = "mysql.url.106.dmdb"
    val mysqlDriver = "mysql.driver"

    saveASMysqlTable(dmData: DataFrame, ods2DwMonitorTable: String, SaveMode.Overwrite,user106:String,pass106:String,mysqlDriver:String,url106:String)

    sc.stop()
  }

  /**
    * 得到ods-dm层每一层数据
    * @param sqlContext 上下文
    */
  def getOds2DmlevelKeyData(sqlContext:HiveContext):DataFrame = {

    /**
      * ods-dw层关键指标结果
      */
    val odsAndDwData = dwLevelData(sqlContext:HiveContext).cache()

    odsAndDwData
  }

  /**
    * ods-dw层关键指标
    * @param sqlContext 上下文
    */
  def dwLevelData(sqlContext:HiveContext)= {

    /**
      * 预估赔付，已赚保费，案件数，死亡案件数，伤残那件数，已结案件数，已结赔付，当前在保人，人天数，满期保费，满期赔付
      */
    val dwData = sqlContext.sql(
      """
        |with dw_data
        |as
        |(
        |select a.policy_code,case when to_date(a.policy_end_date) <= to_date(now()) then 1 else 0 end as expire_policy,a.channel_name,a.policy_start_date,a.policy_end_date,
        |case when g.insured_index = 1 then a.channel_name else null end as channel_name_curr,
        |f.case_no,
        |f.dead_case_no,
        |f.disable_case_no,
        |f.end_case_no,
        |f.end_res_pay,
        |f.res_pay,
        |g.curr_insured,
        |g.person_count,
        |h.charge_premium,
        |i.sum_premium
        |from dwdb.dw_employer_baseinfo_detail a
        |left join
        |(
        |select policy_code,count(case_no) as case_no,sum(res_pay) as res_pay,
        |sum(case when case_status = '结案' then res_pay else 0 end) as end_res_pay,
        |sum(case when case_type = '死亡' then 1 else 0 end) as dead_case_no,
        |sum(case when case_type = '伤残' then 1 else 0 end) as disable_case_no,
        |sum(case when case_status = '结案' then 1 else 0 end) as end_case_no
        |from dwdb.dw_policy_claim_detail
        |group by policy_code
        |) f
        |on a.policy_code = f.policy_code
        |left join
        |(
        |select policy_code,sum(case when day_id = regexp_replace(substr(cast(now() as string),1,10),'-','') then count else 0 end) as curr_insured,
        |sum(case when day_id = regexp_replace(substr(cast(now() as string),1,10),'-','') then 1 else 0 end) as insured_index,
        |sum(count) as person_count
        |from dwdb.dw_policy_curr_insured_detail
        |where day_id <= regexp_replace(substr(cast(now() as string),1,10),'-','')
        |group by policy_code
        |) g
        |on a.policy_code = g.policy_code
        |left join
        |(
        |select policy_id,sum(premium) as charge_premium from dwdb.dw_policy_everyday_premium_detail
        |where day_id <= regexp_replace(substr(cast(now() as string),1,10),'-','')
        |group by policy_id
        |) h
        |on a.policy_id = h.policy_id
        |left join
        |(
        |select policy_id,sum(sum_premium) as sum_premium from dwdb.dw_policy_premium_detail
        |where one_level_pdt_cate = '蓝领外包'
        |group by policy_id
        |) i
        |on a.policy_id = i.policy_id
        |where a.product_code not in ('LGB000001','17000001')
        |)
        |
        |select z.*,y.ent_count_curr from (
        |select sum(x.res_pay) as res_pay,sum(x.charge_premium) as charge_premium,sum(x.case_no) as case_no,sum(x.dead_case_no) as dead_case_no,sum(x.disable_case_no) as disable_case_no,
        |sum(x.end_case_no) as end_case_no,sum(x.end_res_pay) as end_res_pay,sum(x.curr_insured) as curr_insured,sum(x.person_count) as person_count,sum(case when x.expire_policy = 1 then x.charge_premium else 0 end) as expire_charge_premium,
        |sum(case when x.expire_policy = 1 then x.res_pay else 0 end) as expire_res_pay,sum(x.sum_premium) as sum_premium,count(distinct channel_name) as ent_count
        |from dw_data x
        |) z
        |left join
        |(
        |select count(distinct channel_name_curr) as ent_count_curr from dw_data
        |) y
        |on 1=1
      """.stripMargin).cache()

    val currInsured = dwData.selectExpr("'dw' as dw_level","'hive' as house","'dwdb' as house_name","'当前在保人' as indicator","cast(curr_insured as string) as dm_indicator_value")
    val caseNum = dwData.selectExpr("'dw' as dw_level","'hive' as house","'dwdb' as house_name","'案件数' as indicator","cast(case_no as string) as dm_indicator_value")
    val deadCaseNum = dwData.selectExpr("'dw' as dw_level","'hive' as house","'dwdb' as house_name","'死亡案件数' as indicator","cast(dead_case_no as string) as dm_indicator_value")
    val disableCaseNum = dwData.selectExpr("'dw' as dw_level","'hive' as house","'dwdb' as house_name","'伤残案件数' as indicator","cast(disable_case_no as string) as dm_indicator_value")
    val settledCaseNum = dwData.selectExpr("'dw' as dw_level","'hive' as house","'dwdb' as house_name","'已结案件数' as indicator","cast(end_case_no as string) as dm_indicator_value")
    val sum_premium = dwData.selectExpr("'dw' as dw_level","'hive' as house","'dwdb' as house_name","'总保费' as indicator","cast(sum_premium as string) as dm_indicator_value")
    val riskRate = dwData.selectExpr("'dw' as dw_level","'hive' as house","'dwdb' as house_name","'人天数' as indicator","cast(person_count as string) as dm_indicator_value")
    val prepareClaimPremium = dwData.selectExpr("'dw' as dw_level","'hive' as house","'dwdb' as house_name","'预估赔付' as indicator","cast(res_pay as string) as dm_indicator_value")
    val chargePremium = dwData.selectExpr("'dw' as dw_level","'hive' as house","'dwdb' as house_name","'已赚保费' as indicator","cast(charge_premium as string) as dm_indicator_value")
    val expirePremium = dwData.selectExpr("'dw' as dw_level","'hive' as house","'dwdb' as house_name","'满期保费' as indicator","cast(expire_charge_premium as string) as dm_indicator_value")
    val expireClaimPremium = dwData.selectExpr("'dw' as dw_level","'hive' as house","'dwdb' as house_name","'满期赔付' as indicator","cast(expire_res_pay as string) as dm_indicator_value")
    val settledClaimPremium = dwData.selectExpr("'dw' as dw_level","'hive' as house","'dwdb' as house_name","'已结赔付' as indicator","cast(end_res_pay as string) as dm_indicator_value")
    val insuredEntContDw = dwData.selectExpr("'dw' as dw_level","'hive' as house","'dwdb' as house_name","'当前在保客户数' as indicator","cast(ent_count_curr as string) as dm_indicator_value")
    val entContDw = dwData.selectExpr("'dw' as dw_level","'hive' as house","'dwdb' as house_name","'企业客户数' as indicator","cast(ent_count as string) as dm_indicator_value")

    val dwDataRes = currInsured.unionAll(caseNum).unionAll(sum_premium).unionAll(riskRate).unionAll(prepareClaimPremium).unionAll(chargePremium)
      .unionAll(expirePremium).unionAll(expireClaimPremium).unionAll(settledClaimPremium).unionAll(deadCaseNum).unionAll(disableCaseNum).unionAll(settledCaseNum)
      .unionAll(insuredEntContDw).unionAll(entContDw)
      .selectExpr("dw_level","house","house_name","indicator","dm_indicator_value as indicator_value","date_format(now(),'yyyy-MM-dd HH:mm:ss') as dw_create_time")

    val odsData = sqlContext.sql(
      """
        |with ods_data
        |as
        |(
        |select a.policy_code,case when to_date(a.policy_end_date) <= to_date(now()) then 1 else 0 end as expire_policy,(if(a.first_premium is null,0,a.first_premium)+if(f.preserve_premium is null,0,f.preserve_premium)) as sum_premium,
        |d.curr_insured,d.person_count,e.case_no,e.dead_case_no,e.disable_case_no,e.end_case_no,e.end_res_pay,e.res_pay,case when b.channel_name = '直客' then b.ent_name else b.channel_name end as channel_name,
        |case when b.channel_name = '直客' and to_date(a.policy_start_date) <= to_date(now()) and to_date(a.policy_end_date) >= to_date(now()) then b.ent_name when b.channel_name != '直客' and to_date(a.policy_start_date) <= to_date(now()) and to_date(a.policy_end_date) >= to_date(now()) then b.channel_name else null end as channel_name_curr
        |from odsdb.ods_policy_detail a
        |left join odsdb.ods_ent_guzhu_salesman_detail b
        |on a.holder_name = b.ent_name
        |left join odsdb.ods_product_detail c
        |on a.product_code = c.product_code
        |left join
        |(
        |select policy_code,sum(case when to_date(start_date) <= to_date(now()) and to_date(end_date) >= to_date(now()) then 1 else 0 end) as curr_insured,
        |sum(case when start_date > end_date then 0 else datediff((case when to_date(now()) > to_date(end_date) then end_date else now() end),start_date)+1 end) as person_count from odsdb.ods_policy_insured_detail
        |group by policy_code
        |) d
        |on a.policy_code = d.policy_code
        |left join
        |(
        |select policy_no as policy_code,count(case_no) as case_no,sum(case when final_payment is null or length(final_payment) = 0 then cast(pre_com as decimal(14,4)) else cast(final_payment as decimal(14,4)) end) as res_pay,
        |sum(case when trim(case_status) = '结案' then (case when final_payment is null or length(final_payment) = 0 then cast(pre_com as decimal(14,4)) else cast(final_payment as decimal(14,4)) end) else 0 end) as end_res_pay,
        |sum(case when trim(case_type) = '死亡' then 1 else 0 end) as dead_case_no,
        |sum(case when trim(case_type) = '伤残' then 1 else 0 end) as disable_case_no,
        |sum(case when trim(case_status) = '结案' then 1 else 0 end) as end_case_no
        |from odsdb.ods_claims_detail
        |group by policy_no
        |) e
        |on a.policy_code = e.policy_code
        |left join
        |(
        |select sum((if(add_premium is null ,0 ,add_premium)+if(del_premium is null,0,del_premium))) as preserve_premium,policy_id from odsdb.ods_preservation_detail
        |where preserve_status = 1
        |group by policy_id
        |) f
        |on a.policy_id = f.policy_id
        |where c.one_level_pdt_cate = '蓝领外包' and a.product_code not in ('LGB000001','17000001') and a.policy_status in (0,-1,1)
        |)
        |
        |select z.*,y.ent_count from (
        |select sum(x.case_no) as case_no,sum(x.curr_insured) as curr_insured,sum(x.person_count) as person_count,
        |sum(x.dead_case_no) as  dead_case_no,sum(x.disable_case_no) as disable_case_no,sum(x.end_case_no) as end_case_no,sum(x.end_res_pay) as end_res_pay,
        |sum(x.res_pay) as res_pay,sum(case when expire_policy = 1 then x.res_pay else 0 end) as expire_res_pay,sum(x.sum_premium) as sum_premium,
        |count(distinct x.channel_name_curr) as ent_count_curr
        |from ods_data x
        |) z
        |left join
        |(
        |select count(distinct channel_name) as ent_count
        |from ods_data
        |) y
        |on 1 = 1
      """.stripMargin).cache()

    val currInsuredOds = odsData.selectExpr("'ods' as dw_level","'hive' as house","'odsdb' as house_name","'当前在保人' as indicator","cast(curr_insured as string) as dm_indicator_value")
    val caseNumOds = odsData.selectExpr("'ods' as dw_level","'hive' as house","'odsdb' as house_name","'案件数' as indicator","cast(case_no as string) as dm_indicator_value")
    val deadCaseNumOds = odsData.selectExpr("'ods' as dw_level","'hive' as house","'odsdb' as house_name","'死亡案件数' as indicator","cast(dead_case_no as string) as dm_indicator_value")
    val disableCaseNumOds = odsData.selectExpr("'ods' as dw_level","'hive' as house","'odsdb' as house_name","'伤残案件数' as indicator","cast(disable_case_no as string) as dm_indicator_value")
    val settledCaseNumOds = odsData.selectExpr("'ods' as dw_level","'hive' as house","'odsdb' as house_name","'已结案件数' as indicator","cast(end_case_no as string) as dm_indicator_value")
    val sum_premiumOds = odsData.selectExpr("'ods' as dw_level","'hive' as house","'odsdb' as house_name","'总保费' as indicator","cast(sum_premium as string) as dm_indicator_value")
    val riskRateOds = odsData.selectExpr("'ods' as dw_level","'hive' as house","'odsdb' as house_name","'人天数' as indicator","cast(person_count as string) as dm_indicator_value")
    val prepareClaimPremiumOds = odsData.selectExpr("'ods' as dw_level","'hive' as house","'odsdb' as house_name","'预估赔付' as indicator","cast(res_pay as string) as dm_indicator_value")
    val expireClaimPremiumOds = odsData.selectExpr("'ods' as dw_level","'hive' as house","'odsdb' as house_name","'满期赔付' as indicator","cast(expire_res_pay as string) as dm_indicator_value")
    val settledClaimPremiumOds = odsData.selectExpr("'ods' as dw_level","'hive' as house","'odsdb' as house_name","'已结赔付' as indicator","cast(end_res_pay as string) as dm_indicator_value")
    val insuredEntContDwOds = odsData.selectExpr("'ods' as dw_level","'hive' as house","'odsdb' as house_name","'当前在保客户数' as indicator","cast(ent_count_curr as string) as dm_indicator_value")
    val entCountOds = odsData.selectExpr("'ods' as dw_level","'hive' as house","'odsdb' as house_name","'企业客户数' as indicator","cast(ent_count as string) as dm_indicator_value")

    val odsDataRes = currInsuredOds
      .unionAll(caseNumOds)
      .unionAll(deadCaseNumOds)
      .unionAll(disableCaseNumOds)
      .unionAll(settledCaseNumOds)
      .unionAll(sum_premiumOds)
      .unionAll(riskRateOds)
      .unionAll(prepareClaimPremiumOds)
      .unionAll(expireClaimPremiumOds)
      .unionAll(settledClaimPremiumOds)
      .unionAll(settledClaimPremiumOds)
      .unionAll(insuredEntContDwOds)
      .unionAll(entCountOds)
      .selectExpr("dw_level","house","house_name","indicator","dm_indicator_value as indicator_value","date_format(now(),'yyyy-MM-dd HH:mm:ss') as dw_create_time")

    val res = odsDataRes.unionAll(dwDataRes)
    res
  }


  /**
    * dm层关键指标
    * @param sqlContext 上下文
    * @return
    */
  def dmLevelData(sqlContext:HiveContext): DataFrame = {
    /**
      * dm层 神盾系统的 全局风险监控
      */
    val tableNameAegisKriName = "emp_risk_monitor_kri_detail"
    val ckUser = "clickhouse.username"
    val ckPass = "clickhouse.password"
    val ckUrl = "clickhouse.url"
    val tableNameAegisClaimName = "dm_aegis_emp_claim_info_detail"
    val tableNameBThrouthPolicyContinueName = "emp_continue_policy_all_info_detail"
    val tableNameSaleEasyInsuredPremiumName = "dm_saleeasy_policy_curr_insured_premium_detail"
    val user106 = "mysql.username.106"
    val pass106 = "mysql.password.106"
    val url106 = "mysql.url.106.dmdb"
    val mysqlDriver = "mysql.driver"

    readClickHouseTable(sqlContext,tableNameAegisKriName: String,ckUrl:String,ckUser:String,ckPass:String)
      .registerTempTable("emp_risk_monitor_kri_detail1")

    val aegisKriData =sqlContext.sql(
      """
        |SELECT SUM(d.curr_insured) as currInsured,
        |SUM(d.acc_case_num) as caseNum,
        |sum(d.acc_sum_premium) as sum_premium,
        |SUM(d.acc_curr_insured) as person_count,
        |SUM(d.acc_prepare_claim_premium) as prepareClaimPremium,
        |sum(acc_charge_premium) as charge_premium,
        |round(SUM(d.acc_expire_premium),2) expirePremium,
        |SUM(d.acc_expire_claim_premium) as expireClaimPremium,
        |SUM(d.acc_settled_claim_premium) as settledClaimPremium
        |from emp_risk_monitor_kri_detail1 d
        |WHERE  d.day_id=SUBSTRING(cast(now() as String),1,10)
      """.stripMargin).cache()

    val currInsured = aegisKriData.selectExpr("'dm' as dw_level","'神盾' as system","'风险监控报表' as function_name","'ck' as house","'odsdb' as house_name","'emp_risk_monitor_kri_detail1' as table_name","'当前在保人' as indicator","cast(currInsured as string) as dm_indicator_value")
    val caseNum = aegisKriData.selectExpr("'dm' as dw_level","'神盾' as system","'风险监控报表' as function_name","'ck' as house","'odsdb' as house_name","'emp_risk_monitor_kri_detail1' as table_name","'案件数' as indicator","cast(caseNum as string) as dm_indicator_value")
    val sum_premium = aegisKriData.selectExpr("'dm' as dw_level","'神盾' as system","'风险监控报表' as function_name","'ck' as house","'odsdb' as house_name","'emp_risk_monitor_kri_detail1' as table_name","'总保费' as indicator","cast(sum_premium as string) as dm_indicator_value")
    val riskRate = aegisKriData.selectExpr("'dm' as dw_level","'神盾' as system","'风险监控报表' as function_name","'ck' as house","'odsdb' as house_name","'emp_risk_monitor_kri_detail1' as table_name","'人天数' as indicator","cast(person_count as string) as dm_indicator_value")
    val prepareClaimPremium = aegisKriData.selectExpr("'dm' as dw_level","'神盾' as system","'风险监控报表' as function_name","'ck' as house","'odsdb' as house_name","'emp_risk_monitor_kri_detail1' as table_name","'预估赔付' as indicator","cast(prepareClaimPremium as string) as dm_indicator_value")
    val charge_premium = aegisKriData.selectExpr("'dm' as dw_level","'神盾' as system","'风险监控报表' as function_name","'ck' as house","'odsdb' as house_name","'emp_risk_monitor_kri_detail1' as table_name","'已赚保费' as indicator","cast(charge_premium as string) as dm_indicator_value")
    val expirePremium = aegisKriData.selectExpr("'dm' as dw_level","'神盾' as system","'风险监控报表' as function_name","'ck' as house","'odsdb' as house_name","'emp_risk_monitor_kri_detail1' as table_name","'满期保费' as indicator","cast(expirePremium as string) as dm_indicator_value")
    val expireClaimPremium = aegisKriData.selectExpr("'dm' as dw_level","'神盾' as system","'风险监控报表' as function_name","'ck' as house","'odsdb' as house_name","'emp_risk_monitor_kri_detail1' as table_name","'满期赔付' as indicator","cast(expireClaimPremium as string) as dm_indicator_value")
    val settledClaimPremium = aegisKriData.selectExpr("'dm' as dw_level","'神盾' as system","'风险监控报表' as function_name","'ck' as house","'odsdb' as house_name","'emp_risk_monitor_kri_detail1' as table_name","'已结赔付' as indicator","cast(settledClaimPremium as string) as dm_indicator_value")

    val aegisKriDataRes = currInsured.unionAll(caseNum).unionAll(sum_premium).unionAll(riskRate).unionAll(prepareClaimPremium).unionAll(charge_premium)
      .unionAll(expirePremium).unionAll(expireClaimPremium).unionAll(settledClaimPremium)

    /**
      * 全局风险监控 伤残案件数与死亡案件数
      */
    readMysqlTable(sqlContext: SQLContext, tableNameAegisClaimName: String,user106:String,pass106:String,mysqlDriver:String,url106:String)
      .registerTempTable("dm_aegis_emp_claim_info_detail")

    val aegisClaimData = sqlContext.sql(
      """
        |select sum(case when case_type = '死亡' then 1 else 0 end) as dead_case_no,
        |sum(case when case_type = '伤残' then 1 else 0 end) as disable_case_no
        |from dm_aegis_emp_claim_info_detail
      """.stripMargin)

    val deadCaseNo = aegisClaimData.selectExpr("'dm' as dw_level","'神盾' as system","'风险监控报表' as function_name","'mysql' as house","'dmdb' as house_name","'emp_risk_monitor_kri_detail1' as table_name","'死亡案件数' as indicator","cast(dead_case_no as string) as dm_indicator_value")
    val disableCseNo = aegisClaimData.selectExpr("'dm' as dw_level","'神盾' as system","'风险监控报表' as function_name","'mysql' as house","'dmdb' as house_name","'emp_risk_monitor_kri_detail1' as table_name","'伤残案件数' as indicator","cast(disable_case_no as string) as dm_indicator_value")

    val aegisClaimDataRes = deadCaseNo.unionAll(disableCseNo)

    /**
      * b点通 续投趋势  在保人数和b端客户数
      */
    readClickHouseTable(sqlContext,tableNameBThrouthPolicyContinueName: String,ckUrl:String,ckUser:String,ckPass:String)
      .registerTempTable("emp_continue_policy_all_info_detail")

    val bThroughPolicyContinueData = sqlContext.sql(
      """
        |select sum(curr_insured) as curr_insured,
        |COUNT(DISTINCT channel_name) as ent_count
        |from emp_continue_policy_all_info_detail
        |where day_id = SUBSTRING(cast(now() as String),1,10)
      """.stripMargin)

    val currInsuredBClickThrough = bThroughPolicyContinueData.selectExpr("'dm' as dw_level","'b_click_through' as system","'续投追踪分析' as function_name","'ck' as house","'odsdb' as house_name","'emp_continue_policy_all_info_detail' as table_name","'当前在保人' as indicator","cast(curr_insured as string) as dm_indicator_value")
    val entCountBClickThrough = bThroughPolicyContinueData.selectExpr("'dm' as dw_level","'b_click_through' as system","'续投追踪分析' as function_name","'ck' as house","'odsdb' as house_name","'emp_continue_policy_all_info_detail' as table_name","'企业客户数' as indicator","cast(ent_count as string) as dm_indicator_value")

    val bThroughPolicyContinueDataRes = currInsuredBClickThrough.unionAll(entCountBClickThrough)

    /**
      * 销售e
      */
    readClickHouseTable(sqlContext,tableNameSaleEasyInsuredPremiumName: String,ckUrl:String,ckUser:String,ckPass:String)
      .registerTempTable("dm_saleeasy_policy_curr_insured_premium_detail")

    val saleeasyPolicyInsuredAndPremiumData = sqlContext.sql(
      """
        |select sum(curr_insured) as curr_insured,COUNT(DISTINCT channel_name) as insured_ent_count from dm_saleeasy_policy_curr_insured_premium_detail
        |where day_id = regexp_replace(SUBSTRING(cast(now() as String),1,10),'-','')
      """.stripMargin)

    val currInsuredSaleEasy = saleeasyPolicyInsuredAndPremiumData.selectExpr("'dm' as dw_level","'saleeasy' as system","'团队业绩达成' as function_name","'ck' as house","'odsdb' as house_name","'dm_saleeasy_policy_curr_insured_premium_detail' as table_name","'当前在保人' as indicator","cast(curr_insured as string) as dm_indicator_value")
    val insuredEntContSaleEasy = saleeasyPolicyInsuredAndPremiumData.selectExpr("'dm' as dw_level","'saleeasy' as system","'团队业绩达成' as function_name","'ck' as house","'odsdb' as house_name","'dm_saleeasy_policy_curr_insured_premium_detail' as table_name","'当前在保客户数' as indicator","cast(insured_ent_count as string) as dm_indicator_value")

    val saleeasyPolicyInsuredAndPremiumDataRes = currInsuredSaleEasy.unionAll(insuredEntContSaleEasy)

    val dmData = aegisKriDataRes.unionAll(aegisClaimDataRes).unionAll(bThroughPolicyContinueDataRes).unionAll(saleeasyPolicyInsuredAndPremiumDataRes)
      .selectExpr("dw_level","system","function_name","house","house_name","table_name","indicator","dm_indicator_value as indicator_value","date_format(now(),'yyyy-MM-dd HH:mm:ss') as dw_create_time")

    dmData
  }
}
