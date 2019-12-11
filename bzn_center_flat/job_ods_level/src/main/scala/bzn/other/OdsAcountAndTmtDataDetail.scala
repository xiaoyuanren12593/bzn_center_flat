package bzn.other

import bzn.job.common.MysqlUntil
import bzn.util.SparkUtil
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * author:xiaoYuanRen
  * Date:2019/12/11
  * Time:16:13
  * describe: 统计电子台账和接口2019年的数据
  **/
object OdsAcountAndTmtDataDetail extends SparkUtil with MysqlUntil{
  def main (args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName,"")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    val res = getAcountAndTmtData(hiveContext)
    hiveContext.sql("truncate table odsdb.accounts_and_tmt_detail")
    res.repartition(10).write.mode(SaveMode.Append).saveAsTable("odsdb.accounts_and_tmt_detail")
    sc.stop()
  }

  def getAcountAndTmtData(sqlContext:HiveContext) = {
    val url = "mysql.url"
    val urldwdb = "mysql.url.dwdb"
    val urltableau = "mysql.url.tableau"
    val user = "mysql.username"
    val pass = "mysql.password"
    val driver = "mysql.driver"
    val tableName1 = "t_accounts_un_employer"
    val tableName2 = "t_accounts_employer"
    val tableName3 = "ods_ent_sales_team"
    val tableName5 = "ods_ent_tmt_salesman"
    val tableName4 = "dw_product_detail"

    val tAccountsUnEmployer =
      readMysqlTable(sqlContext: SQLContext, tableName1: String,user:String,pass:String,driver:String,url:String)
        .select("policy_no","preserve_id","data_source","project_name","product_code","product_name","channel_name","business_region",
          "performance_accounting_day","holder_name","premium_total","economy_rates","economy_fee","business_owner","policy_effective_time",
          "policy_expire_time","underwriting_company",
          "technical_service_rates","technical_service_fee","has_brokerage","brokerage_ratio","brokerage_fee")

    val tAccountsEmployer =
      readMysqlTable(sqlContext: SQLContext, tableName2: String,user:String,pass:String,driver:String,url:String)
        .selectExpr("policy_no","preserve_id","data_source","'雇主' as project_name","product_code","product_name","channel_name","business_region",
          "performance_accounting_day","holder_name","premium_total","economy_rates","economy_fee","business_owner","policy_effective_time",
          "policy_expire_time","underwriting_company",
          "technical_service_rates","technical_service_fee","has_brokerage","brokerage_ratio","brokerage_fee")

    val acountData = tAccountsUnEmployer.unionAll(tAccountsEmployer)

    val odsPolicyDetail =
      sqlContext.sql(
        """
          |select a.policy_code,concat(substr(a.belongs_regional,1,4),'00') as belongs_regional ,
          |a.num_of_preson_first_policy,b.product_code,b.product_name,b.business_line from odsdb.ods_policy_detail  a
          |left join (
          |  select if(business_line='接口','场景',business_line) as business_line ,product_code,product_name
          |  from odsdb.ods_product_detail
          |  group by business_line,product_code,product_name
          |) b
          |on a.product_code = b.product_code
          |where policy_status in (0,1,-1)
        """.stripMargin)

    val odsPreservationDetail = sqlContext.sql("select preserve_id as preserve_id_salve,(if(add_person_count is null,0,add_person_count)-if(del_person_count is null,0,del_person_count)) as preserve_num_count from odsdb.ods_preservation_detail where preserve_status = 1")

    val odsAreaInfoDimension = sqlContext.sql("select code,short_name,province from odsdb.ods_area_info_dimension")

    val policyData = odsPolicyDetail.join(odsAreaInfoDimension,odsPolicyDetail("belongs_regional")===odsAreaInfoDimension("code"),"leftouter")
      .selectExpr("policy_code as policy_code_slave","belongs_regional","num_of_preson_first_policy","business_line",
        "short_name","province")

    val acountDataOneRes = acountData.join(policyData,acountData("policy_no")===policyData("policy_code_slave"),"leftouter")
      .selectExpr("policy_no as policy_code","preserve_id","project_name","product_code","product_name","channel_name","business_region as biz",
        "performance_accounting_day","holder_name","premium_total","economy_rates","economy_fee","business_owner as sale_name","policy_effective_time",
        "policy_expire_time","underwriting_company","technical_service_rates","technical_service_fee","has_brokerage","brokerage_ratio","brokerage_fee",
        "num_of_preson_first_policy as num_person","business_line", "short_name","province","'acount' as source")

    val acountDataRes = acountDataOneRes.join(odsPreservationDetail,acountDataOneRes("preserve_id")===odsPreservationDetail("preserve_id_salve"),"leftouter")
      .selectExpr(
        "policy_code","project_name","product_code","product_name","channel_name","biz",
        "performance_accounting_day","holder_name","premium_total","economy_rates","economy_fee","sale_name","policy_effective_time",
        "policy_expire_time","underwriting_company","technical_service_rates","technical_service_fee","has_brokerage","brokerage_ratio","brokerage_fee",
        "case when preserve_id_salve is not null then preserve_num_count else num_person end as num_person","project_name as business_line", "short_name","province","source"
      )

    //    acountDataRes.show()

    val dwProductDetail = readMysqlTable(sqlContext: SQLContext, tableName4: String,user:String,pass:String,driver:String,urldwdb:String)
      .selectExpr("'' as policy_code","product_code","'' as holder","policy_cnt as num_person","policy_sum as premium",
        "cast(add_date as timestamp) as start_date","cast('' as timestamp) as end_date")

    val odsEntTmtSalesman = readMysqlTable(sqlContext: SQLContext, tableName5: String,user:String,pass:String,driver:String,url:String)
      .selectExpr("product_code as product_code_salve","product_name","sale_name","company_name","brokerage_ratio","economic_rate")

    val dwProductData = dwProductDetail.join(odsEntTmtSalesman,dwProductDetail("product_code")===odsEntTmtSalesman("product_code_salve"),"leftouter")
      .selectExpr("policy_code","product_code","holder as holder_name","num_person","premium as premium_total","product_name",
        "sale_name","start_date","end_date","company_name","brokerage_ratio","economic_rate")

    val odsEntSalesTeam = readMysqlTable(sqlContext: SQLContext, tableName3: String,user:String,pass:String,driver:String,url:String)
      .selectExpr("sale_name as sale_name_salve","team_name")

    val dwProductSaleData = dwProductData.join(odsEntSalesTeam,dwProductData("sale_name")===odsEntSalesTeam("sale_name_salve"),"leftouter")
      .selectExpr("policy_code","product_code","holder_name","num_person","premium_total","product_name",
        "sale_name","company_name as channel_name","start_date","end_date","brokerage_ratio","economic_rate","team_name")

    val odsProductDetail = sqlContext.sql(
      """
        | select if(business_line='接口','场景',business_line) as business_line ,product_code as product_code_slave,company_name
        |  from odsdb.ods_product_detail where business_line = '接口'
        |  group by business_line,product_code,product_name,company_name
      """.stripMargin)

    val dwProductRes = dwProductSaleData.join(odsProductDetail,dwProductSaleData("product_code")===odsProductDetail("product_code_slave"))
      .selectExpr(
        "policy_code","'' as project_name","product_code","product_name","channel_name","team_name as biz",
        "start_date as performance_accounting_day","holder_name","premium_total","economic_rate","(premium_total*economic_rate) as economy_fee",
        "sale_name","start_date as policy_effective_time","end_date as policy_expire_time","company_name as underwriting_company",
        "cast('' as decimal(14,4)) as technical_service_rates","cast('' as decimal(14,4)) as technical_service_fee",
        "'' as has_brokerage","brokerage_ratio","(premium_total*brokerage_ratio) as brokerage_fee","num_person","business_line",
        "'' as short_name","'' as province","'inter' as source"
      )
    val tableName = "accounts_and_tmt_detail"

    val res = dwProductRes.unionAll(acountDataRes)
    saveASMysqlTable(res: DataFrame, tableName: String, SaveMode.Overwrite,user:String,pass:String,driver:String,urltableau:String)
    res

  }
}
