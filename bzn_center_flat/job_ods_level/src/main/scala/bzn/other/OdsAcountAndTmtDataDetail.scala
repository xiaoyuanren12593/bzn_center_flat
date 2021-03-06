package bzn.other

import bzn.job.common.{DataBaseUtil, Until}
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
object OdsAcountAndTmtDataDetail extends SparkUtil with DataBaseUtil with Until{
  def main (args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName,"")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    getAcountAndTmtData(hiveContext)
    sc.stop()
  }

  def getAcountAndTmtData(sqlContext:HiveContext): DataFrame = {
    import sqlContext.implicits._
    sqlContext.udf.register("clean", (str: String) => clean(str))
    val url = "mysql.url"
    val urlDwdb = "mysql.url.dwdb"
    val urlTableau = "mysql.url.tableau"
    val url106 = "mysql.url.106.odsdb"
    val user = "mysql.username"
    val pass = "mysql.password"
    val driver = "mysql.driver"
    val user106 = "mysql.username.106"
    val pass106 =   "mysql.password.106"
    val tableName1 = "ods_t_accounts_un_employer_detail"
    val tableName2 = "ods_t_accounts_employer_detail"
    val tableName5 = "ods_ent_tmt_salesman"
    val tableName4 = "dw_product_detail"
    val tableName6 = "ods_t_accounts_agency_detail"
    val tableName7 = "ods_channel_first_three_month_temp_detail"

    /**
      * 非雇主电子台账数据105
      */
    val tAccountsUnEmployer =
      readMysqlTable(sqlContext: SQLContext, tableName1: String,user106:String,pass106:String,driver:String,url106:String)
        //      .where("performance_accounting_day >= '2019-01-01 00:00:00'")
        .selectExpr("policy_no","preserve_id","data_source","project_name","product_code","product_name","channel_name","business_region",
        "performance_accounting_day","regexp_replace(holder_name,'\\n','') as holder_name","premium_total","economy_rates",
        "economy_fee","business_owner","policy_effective_time","policy_expire_time","underwriting_company",
        "technical_service_rates","technical_service_fee","has_brokerage","brokerage_ratio","brokerage_fee")

    /**
      * 雇主电子台账的数据
      */
    val tAccountsEmployer =
      readMysqlTable(sqlContext: SQLContext, tableName2: String,user106:String,pass106:String,driver:String,url106:String)
        //        .where("performance_accounting_day >= '2019-01-01 00:00:00'")
        .selectExpr("policy_no","preserve_id","data_source","'雇主' as project_name","product_code","product_name","channel_name","business_region",
        "performance_accounting_day","regexp_replace(holder_name,'\\n','') as holder_name","premium_total","economy_rates","economy_fee","business_owner","policy_effective_time",
        "policy_expire_time","underwriting_company",
        "technical_service_rates","technical_service_fee","has_brokerage","brokerage_ratio","brokerage_fee")

    /**
      * 合并雇主和非雇主的电子台账
      */
    val acountData = tAccountsUnEmployer.unionAll(tAccountsEmployer)

    /**
      * 对保单明细表和产品表的数据进行合并，拿出保单投保人的地域编码，并且把接口业务条线归场景
      */
    val odsPolicyDetail =
      sqlContext.sql(
        """
          |select t.policy_code,t.belongs_regional,t.num_of_preson_first_policy,t.product_code,t.product_name,t.business_line from (
          |select distinct a.policy_code,concat(substr(a.belongs_regional,1,4),'00') as belongs_regional ,
          |a.num_of_preson_first_policy,b.product_code,b.product_name,b.business_line
          |from odsdb.ods_policy_detail  a
          |left join (
          |  select if(business_line='接口','场景',business_line) as business_line ,product_code,product_name
          |  from odsdb.ods_product_detail
          |  group by business_line,product_code,product_name
          |) b
          |on a.product_code = b.product_code
          |where policy_status in (0,1,-1) and a.policy_code is not null
          |) t group by t.policy_code,t.belongs_regional,t.num_of_preson_first_policy,t.product_code,t.product_name,t.business_line
        """.stripMargin).cache()

    /**
      * 批单的数据
      */
    val odsPreservationDetail = sqlContext.sql("select preserve_id as preserve_id_salve,(if(add_person_count is null,0,add_person_count)-if(del_person_count is null,0,del_person_count)) as preserve_num_count from odsdb.ods_preservation_detail where preserve_status = 1")

    /**
      * 地码表的数据
      */
    val odsAreaInfoDimension = sqlContext.sql("select code,short_name,province from odsdb.ods_area_info_dimension")

    /**
      * 保单和地域数据
      */
    val policyData = odsPolicyDetail.join(odsAreaInfoDimension,odsPolicyDetail("belongs_regional")===odsAreaInfoDimension("code"),"leftouter")
      .selectExpr("policy_code as policy_code_slave","belongs_regional","num_of_preson_first_policy","business_line",
        "short_name","province")

    /**
      * 台账的数据与保单地域数据
      */
    val acountDataOneRes = acountData.join(policyData,acountData("policy_no")===policyData("policy_code_slave"),"leftouter")
      .selectExpr("policy_no as policy_code","preserve_id","project_name","product_code","product_name","channel_name","business_region as biz",
        "performance_accounting_day","holder_name","premium_total","economy_rates","economy_fee","business_owner as sale_name","policy_effective_time",
        "policy_expire_time","underwriting_company","technical_service_rates","technical_service_fee","has_brokerage","brokerage_ratio","brokerage_fee",
        "num_of_preson_first_policy as num_person","business_line", "short_name","province","'acount' as source")

    /**
      * 上数据结果和批单数据关联，得到增减员人数
      */
    val acountDataRes = acountDataOneRes.join(odsPreservationDetail,acountDataOneRes("preserve_id")===odsPreservationDetail("preserve_id_salve"),"leftouter")
      .selectExpr(
        "policy_code","project_name","product_code","product_name","channel_name","biz",
        "performance_accounting_day","holder_name","premium_total","economy_rates as economic_rate","economy_fee","sale_name","policy_effective_time",
        "policy_expire_time","underwriting_company","technical_service_rates","technical_service_fee","has_brokerage","brokerage_ratio","brokerage_fee",
        "case when preserve_id_salve is not null then preserve_num_count else num_person end as num_person","project_name as business_line", "short_name","province","source"
      )

    /*****
      * 1 读取接口平台的数据
      * 2 读取接口的维护表，通过产品划分出，渠道，返佣，手续费信息
      * 3 将1 和 2 进行关联
      * 4 读取销售团队表
      * 5 将3 和 4 进行关联
      * 6 读取产品表，并进行业务条线分类
      * 7 将5 和 6 的结果进行关联
      */

    /**
      * 得到接口的数据，接口的数据是汇总的，最细粒度是产品。
      */
    val dwProductDetail = readMysqlTable(sqlContext: SQLContext, tableName4: String,user:String,pass:String,driver:String,urlDwdb:String)
      //      .where("cast(add_date as timestamp) >= '2019-01-01 00:00:00'")
      .selectExpr("'' as policy_code","product_code","'' as holder","policy_cnt as num_person","policy_sum as premium",
      "cast(add_date as timestamp) as start_date","cast('' as timestamp) as end_date")

    /**
      * 接口的销售码表，产品对应的销售、渠道、手续费、返佣费
      */
    val odsEntTmtSalesman = readMysqlTable(sqlContext: SQLContext, tableName5: String,user:String,pass:String,driver:String,url:String)
      .selectExpr("product_code as product_code_salve","product_name","sale_name","company_name","brokerage_ratio","economic_rate")

    /**
      * 接口数据和上述销售表数据关联
      */
    val dwProductData = dwProductDetail.join(odsEntTmtSalesman,dwProductDetail("product_code")===odsEntTmtSalesman("product_code_salve"),"leftouter")
      .selectExpr("policy_code","product_code","regexp_replace(holder,'\\n','') as holder_name","num_person","premium as premium_total","product_name",
        "sale_name","start_date","end_date","company_name","brokerage_ratio","economic_rate")

    /**
      * 销售团队表
      */
    val odsEntSalesTeam =
      sqlContext.sql("select sale_name as sale_name_salve,team_name from odsdb.ods_ent_sales_team_dimension")

    /**
      * 雇主初投三月的数据
      */
    val odsChannelFirstThreeMonthTempDetail = readMysqlTable(sqlContext: SQLContext, tableName7: String,user:String,pass:String,driver:String,url:String)
      .selectExpr("channel_name as channel_name_salve","SUBSTRING(cast(first_start_date as string),1,10) as first_start_date",
        "SUBSTRING(cast(reffer_date as string),1,10) as three_month","SUBSTRING(cast(six_date as string),1,10) as six_month",
        "SUBSTRING(cast(twelve_date as string),1,10) as twelve_month",
        "'雇主' as business_line_salve")

    /**
      * 上述结果数据和销售团队表进行关联
      */
    val dwProductSaleData = dwProductData.join(odsEntSalesTeam,dwProductData("sale_name")===odsEntSalesTeam("sale_name_salve"),"leftouter")
      .selectExpr("policy_code","product_code","holder_name","num_person","premium_total","product_name",
        "sale_name","company_name as channel_name","start_date","end_date","brokerage_ratio","economic_rate","team_name")

    /**
      * 对产品表进行分类，将接口的数据归到场景
      */
    val odsProductDetail = sqlContext.sql(
      """
        | select if(business_line='接口','场景',business_line) as business_line ,product_code as product_code_slave,company_name
        |  from odsdb.ods_product_detail where business_line = '接口'
        |  group by business_line,product_code,product_name,company_name
      """.stripMargin)

    /**
      * 接口数据和上述产品分类数据进行关联
      */
    val dwProductRes = dwProductSaleData.join(odsProductDetail,dwProductSaleData("product_code")===odsProductDetail("product_code_slave"))
      .selectExpr(
        "policy_code","'' as project_name","product_code","product_name","channel_name","team_name as biz",
        "start_date as performance_accounting_day","holder_name","premium_total","economic_rate","(premium_total*economic_rate) as economy_fee",
        "sale_name","start_date as policy_effective_time","end_date as policy_expire_time","company_name as underwriting_company",
        "cast('' as decimal(14,4)) as technical_service_rates","cast('' as decimal(14,4)) as technical_service_fee",
        "'' as has_brokerage","brokerage_ratio","(premium_total*brokerage_ratio) as brokerage_fee","num_person","business_line",
        "'' as short_name","'' as province","'inter' as source"
      )

    /**
      * 平台数据
      */
    val tAccountsAgency = readMysqlTable(sqlContext: SQLContext, tableName6: String,user106:String,pass106:String,driver:String,url106:String)
      //      .where("performance_accounting_day >= '2019-01-01 00:00:00'")
      .selectExpr("policy_no as policy_code","project_name","'' as product_code","product_name","channel_name",
      "performance_accounting_day","holder_name","premium_total","economy_rates as economic_rate","economy_fee",
      "business_owner as sale_name","policy_effective_time","policy_expire_time","underwriting_company",
      "cast('' as decimal(14,4)) as technical_service_rates","cast('' as decimal(14,4)) as technical_service_fee",
      "'' as has_brokerage","brokerage_ratio","brokerage_fee","cast('' as int) as num_person","'平台' as business_line",
      "'' as short_name","'' as province","'plat' as source")

    /**
      * 平台的数据和公司全部销售表进行关联
      */
    val tAccountsAgencyRes = tAccountsAgency.join(odsEntSalesTeam,tAccountsAgency("sale_name")===odsEntSalesTeam("sale_name_salve"),"leftouter")
      .selectExpr("policy_code","project_name","product_code","product_name","channel_name","team_name as biz",
        "performance_accounting_day","holder_name","premium_total","economic_rate","economy_fee",
        "sale_name","policy_effective_time","policy_expire_time","underwriting_company",
        "technical_service_rates","technical_service_fee",
        "has_brokerage","brokerage_ratio","brokerage_fee","num_person","business_line",
        "short_name","province","source")

    /**
      * 保险公司简称表
      */
    val odsInsuranceCompanyTempDimension = sqlContext.sql("select insurance_company,short_name as insurance_company_short_name from odsdb.ods_insurance_company_temp_dimension")

    /**
      * 台账，接口，平台数据合并
      */
    val resTemp = dwProductRes.unionAll(acountDataRes).unionAll(tAccountsAgencyRes)

    /**
      * 健康的业务条线，暂时将（经济费率+技术服务费率）》1的数据的经济费率暂时改成0.5，技术服务费率为0，经纪费为总保费*0.5 技术服务费为0
      */
    val resData =
      resTemp.join(odsInsuranceCompanyTempDimension,resTemp("underwriting_company")===odsInsuranceCompanyTempDimension("insurance_company"),"leftouter")
        .selectExpr("policy_code","project_name","product_code","product_name",
          "case when channel_name in ('sem推广','体育线渠道','官网渠道','保准健康员工福利','保准健康自主用户','邹德乾','祥峰测试企业','小赛保2.0版本','健康渠道员工福利','线下渠道保单-陈贝贝','娄聪聪个人渠道','陈俊圣','上海吴千里','彭丹','裴仰军个人渠道','sem推广0320','线下渠道保单-刘晓昆','詹阳','毕英杰','赵晓彤个人渠道（菏泽游泳协会）','刘晓昆','李玉杰个人渠道','房山ZX','陈成杰','芦国忠个人渠道(石家庄)','长沙谌林祥','崔路遥','陈贝贝','线下渠道保单-崔路遥','线下渠道保单-毕英杰','线下渠道保单-张博','彭丹线上渠道','线下渠道保单-彭丹','测试认证','官网','刘晓昆-日常运营','体育赠险业务','刘晓昆-活动营销','彭丹-日常运营','李玲玉-日常运营','赵山','体育赞助','保准牛C端业务','线下渠道保单-赵伟','彭丹-营销活动','蔡文静个人渠道','线下渠道保单-刘超','施何辉个人渠道','体育主动营销客户','董佳保个人渠道','刘晓昆-sem','李柏个人渠道','毕英杰-日常运营','曹洋体育业务','优全智汇分销-北京鼎立保险经纪有限责任公司(北京业务)','sem之团建保险','王正松','崔路遥-日常运营','线下渠道保单-林凯成','体育活动','线下','张旸旸','彭丹-sem','线下渠道保单-曹洋','山东王强个人渠道','曹安铭个人渠道（青岛）','bzn','官网打折','线下渠道保费-史剑','徐文龙个人渠道（北京）','军训保险sem','李玲玉-活动营销','沈长鸿个人渠道','公众号菜单','王艳','陈瑞麒个人渠道（武汉华师）','保准牛测试验证','线下-赞助业务','跆拳道罗家春个人渠道','周玉丽个人渠道','成旭个人渠道','丛艳个人渠道','纪涛个人渠道（青岛）','sem之篮球保险','B端用户激活短信-月月e保','滑雪sem','崔路遥-营销活动','王建云','邹金龙','夏旭锋个人渠道','线下渠道保单-温野','优全智汇保准牛网站直销','体育李永岗个人渠道','线下渠道保单-李玲玉','赵伟线下','场地公责sem','亮中国','唐洁个人渠道','青训保险sem','线下渠道保单-吴昊','线下-跆拳道业务','sem之夏令营保险','吴唯个人渠道','保准牛体育','魏军军个人渠道','毕英杰-营销活动','sem之足球保险','体育SEM','风险评测产品推荐','李戈个人渠道','线下渠道保单-闫磊','柴泽雨个人渠道（深圳）','侯旭声') then null else channel_name end as channel_name",
          "biz",
          "performance_accounting_day","regexp_replace(holder_name,'\\n','') as holder_name","premium_total",
          "economic_rate",
          "economy_fee",
          "sale_name","policy_effective_time", "policy_expire_time","underwriting_company",
          "insurance_company_short_name as insurance_company_short_name",
          "technical_service_rates",
          "technical_service_fee",
          "case when brokerage_ratio is null or brokerage_ratio = 0 then '0' else '1' end has_brokerage","brokerage_ratio","brokerage_fee",
          "num_person","business_line","short_name","province","source"
        )

    /**
      * 新老客
      */
    val newAndOldData = sqlContext.sql(
      """
        |select if(c.channel_name = '直客',c.ent_name,c.channel_name) as channel_name_slave,to_date(min(a.policy_start_date)),
        |if(to_date(min(a.policy_start_date)) >= to_date('2020-01-01'),'新客','老客') as new_and_old
        |from odsdb.ods_policy_detail  a
        |left join odsdb.ods_product_detail b
        |on a.product_code = b.product_code
        |left join odsdb.ods_ent_guzhu_salesman_detail c
        |on a.holder_name = c.ent_name
        |where policy_status in (0,1,-1) and b.one_level_pdt_cate = '蓝领外包'
        |group by if(c.channel_name = '直客',c.ent_name,c.channel_name),c.consumer_new_old
      """.stripMargin)

    val res = resData.join(newAndOldData,'channel_name==='channel_name_slave,"leftouter")
      .selectExpr(
        "clean(policy_code) as policy_code","clean(project_name) as project_name",
        "clean(product_code) as product_code","clean(product_name) as product_name",
        "clean(channel_name) as channel_name",
        "clean(biz) as biz",
        "performance_accounting_day","clean(holder_name) as holder_name",
        "cast(premium_total as decimal(14,4)) as premium_total",
        "cast(economic_rate as decimal(14,4)) as economic_rate",
        "cast(economy_fee  as decimal(14,4)) as economy_fee",
        "clean(sale_name) as sale_name",
        "policy_effective_time", "policy_expire_time","clean(underwriting_company) as underwriting_company",
        "clean(insurance_company_short_name) as insurance_company_short_name",
        "technical_service_rates",
        "technical_service_fee",
        "has_brokerage","brokerage_ratio","cast(brokerage_fee as decimal(14,4)) as brokerage_fee",
        "num_person","business_line", "short_name","province",
        "case when channel_name_slave is null then null when channel_name_slave is not null and business_line = '雇主' then new_and_old else null end as new_old_cus",
        "source"
      )

    sqlContext.sql("truncate table odsdb.ods_accounts_and_tmt_detail")
    res.repartition(10).write.mode(SaveMode.Append).saveAsTable("odsdb.ods_accounts_and_tmt_detail")

    val tableName = "accounts_and_tmt_detail"
    val tableNameRes = "ods_accounts_and_tmt_detail"
//    saveASMysqlTable(res.repartition(100): DataFrame, tableName: String, SaveMode.Overwrite,user:String,pass:String,driver:String,urlTableau:String)
    saveASMysqlTable(res.repartition(100): DataFrame, tableNameRes: String, SaveMode.Overwrite,user106:String,pass106:String,driver:String,url106:String)

    res
  }
}
