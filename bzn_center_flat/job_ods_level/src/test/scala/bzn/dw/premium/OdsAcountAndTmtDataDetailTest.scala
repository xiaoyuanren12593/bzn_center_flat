package bzn.dw.premium

import bzn.job.common.MysqlUntil

import bzn.util.SparkUtil
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * author:xiaoYuanRen
  * Date:2019/12/11
  * Time:16:13
  * describe: 统计电子台账和接口2019年的数据
  **/
object OdsAcountAndTmtDataDetailTest extends SparkUtil with MysqlUntil {
  def main (args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName,"local[*]")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    getAcountAndTmtData(hiveContext)
//    hiveContext.sql("truncate table odsdb.accounts_and_tmt_detail")
//    res.repartition(10).write.mode(SaveMode.Append).saveAsTable("odsdb.accounts_and_tmt_detail")
    sc.stop()
  }

  def getAcountAndTmtData(sqlContext:HiveContext): DataFrame = {
    import sqlContext.implicits._

//    sqlContext.udf.register("clean", (str: String) => clean(str))
    val url = "mysql.url"
    val urlDwdb = "mysql.url.dwdb"
    val urlTableau = "mysql.url.tableau"
    val user = "mysql.username"
    val pass = "mysql.password"
    val driver = "mysql.driver"
    val tableName1 = "t_accounts_un_employer"
    val tableName2 = "t_accounts_employer"
    val tableName3 = "ods_ent_sales_team"
    val tableName5 = "ods_ent_tmt_salesman"
    val tableName4 = "dw_product_detail"
    val tableName6 = "t_accounts_agency"
    val tableName7 = "ods_channel_first_three_month_temp_detail"

    /**
      * 非雇主电子台账数据105
      */
    val tAccountsUnEmployer =
      readMysqlTable(sqlContext: SQLContext, tableName1: String,user:String,pass:String,driver:String,url:String)
//      .where("performance_accounting_day >= '2019-01-01 00:00:00'")
      .selectExpr("policy_no","preserve_id","data_source","project_name","product_code","product_name","channel_name","business_region",
        "performance_accounting_day","regexp_replace(holder_name,'\\n','') as holder_name","premium_total","economy_rates",
        "economy_fee","business_owner","policy_effective_time","policy_expire_time","underwriting_company",
         "technical_service_rates","technical_service_fee","has_brokerage","brokerage_ratio","brokerage_fee")

    /**
      * 雇主电子台账的数据
      */
    val tAccountsEmployer =
      readMysqlTable(sqlContext: SQLContext, tableName2: String,user:String,pass:String,driver:String,url:String)
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
    val odsEntSalesTeam = readMysqlTable(sqlContext: SQLContext, tableName3: String,user:String,pass:String,driver:String,url:String)
      .selectExpr("sale_name as sale_name_salve","team_name")

    /**
      * 雇主初投三月的数据
      */
    val odsChannelFirstThreeMonthTempDetail = readMysqlTable(sqlContext: SQLContext, tableName7: String,user:String,pass:String,driver:String,url:String)
      .selectExpr("channel_name as channel_name_salve","SUBSTRING(cast(first_start_date as string),1,10) as first_start_date",
        "SUBSTRING(cast(reffer_date as string),1,10) as three_month","SUBSTRING(cast(six_date as string),1,10) as six_month","'雇主' as business_line_salve")

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
    val tAccountsAgency = readMysqlTable(sqlContext: SQLContext, tableName6: String,user:String,pass:String,driver:String,url:String)
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
    val res =
      resTemp.join(odsInsuranceCompanyTempDimension,resTemp("underwriting_company")===odsInsuranceCompanyTempDimension("insurance_company"),"leftouter")
        .selectExpr("policy_code","project_name","product_code","product_name",
          "case when channel_name in ('sem推广','体育线渠道','官网渠道','保准健康员工福利','保准健康自主用户','邹德乾','祥峰测试企业','小赛保2.0版本','健康渠道员工福利','线下渠道保单-陈贝贝','娄聪聪个人渠道','陈俊圣','上海吴千里','彭丹','裴仰军个人渠道','sem推广0320','线下渠道保单-刘晓昆','詹阳','毕英杰','赵晓彤个人渠道（菏泽游泳协会）','刘晓昆','李玉杰个人渠道','房山ZX','陈成杰','芦国忠个人渠道(石家庄)','长沙谌林祥','崔路遥','陈贝贝','线下渠道保单-崔路遥','线下渠道保单-毕英杰','线下渠道保单-张博','彭丹线上渠道','线下渠道保单-彭丹','测试认证','官网','刘晓昆-日常运营','体育赠险业务','刘晓昆-活动营销','彭丹-日常运营','李玲玉-日常运营','赵山','体育赞助','保准牛C端业务','线下渠道保单-赵伟','彭丹-营销活动','蔡文静个人渠道','线下渠道保单-刘超','施何辉个人渠道','体育主动营销客户','董佳保个人渠道','刘晓昆-sem','李柏个人渠道','毕英杰-日常运营','曹洋体育业务','优全智汇分销-北京鼎立保险经纪有限责任公司(北京业务)','sem之团建保险','王正松','崔路遥-日常运营','线下渠道保单-林凯成','体育活动','线下','张旸旸','彭丹-sem','线下渠道保单-曹洋','山东王强个人渠道','曹安铭个人渠道（青岛）','bzn','官网打折','线下渠道保费-史剑','徐文龙个人渠道（北京）','军训保险sem','李玲玉-活动营销','沈长鸿个人渠道','公众号菜单','王艳','陈瑞麒个人渠道（武汉华师）','保准牛测试验证','线下-赞助业务','跆拳道罗家春个人渠道','周玉丽个人渠道','成旭个人渠道','丛艳个人渠道','纪涛个人渠道（青岛）','sem之篮球保险','B端用户激活短信-月月e保','滑雪sem','崔路遥-营销活动','王建云','邹金龙','夏旭锋个人渠道','线下渠道保单-温野','优全智汇保准牛网站直销','体育李永岗个人渠道','线下渠道保单-李玲玉','赵伟线下','场地公责sem','亮中国','唐洁个人渠道','青训保险sem','线下渠道保单-吴昊','线下-跆拳道业务','sem之夏令营保险','吴唯个人渠道','保准牛体育','魏军军个人渠道','毕英杰-营销活动','sem之足球保险','体育SEM','风险评测产品推荐','李戈个人渠道','线下渠道保单-闫磊','柴泽雨个人渠道（深圳）','侯旭声') then null else channel_name end as channel_name",
          "biz",
          "performance_accounting_day","regexp_replace(holder_name,'\\n','') as holder_name","premium_total",
          "economic_rate",
          "economy_fee",
          "sale_name","policy_effective_time", "policy_expire_time","underwriting_company",
          "case when insurance_company_short_name is null then underwriting_company else insurance_company_short_name end as insurance_company_short_name",
          "technical_service_rates",
          "technical_service_fee",
          "case when brokerage_ratio is null or brokerage_ratio = 0 then '0' else '1' end has_brokerage","brokerage_ratio","brokerage_fee",
          "num_person","business_line", "short_name","province","source")

//    /**
//      * 得到雇主的渠道数据，渠道直客的数据
//      */
//    val empData = sqlContext.sql(
//      """
//      |select
//      |   c.channel_name as channel_name_new,a.policy_code as policy_code_slave
//      |   from odsdb.ods_policy_detail a
//      |   left join odsdb.ods_product_detail b
//      |   on a.product_code = b.product_code
//      |   join odsdb.ods_ent_guzhu_salesman_detail c
//      |   on a.holder_name =c.ent_name
//      |   where b.one_level_pdt_cate = '蓝领外包' and a.policy_status in (0,1,-1) and c.channel_name <> '直客'
//      """.stripMargin)

    /**
      * 上述结果关联后，将关联不上的雇主业务条线的channel_name字段置空（直客填空值）
      */
//    val result = res
//      .selectExpr("policy_code","project_name","product_code","product_name",
//        "case when policy_code_slave is null and business_line = '雇主' then null else channel_name end as channel_name",
//        "biz",
//        "performance_accounting_day","holder_name","premium_total",
//        "economic_rate",
//        "economy_fee",
//        "sale_name","policy_effective_time", "policy_expire_time","underwriting_company",
//        "insurance_company_short_name",
//        "technical_service_rates",
//        "technical_service_fee",
//        "has_brokerage","brokerage_ratio","brokerage_fee",
//        "num_person","business_line", "short_name","province","source")

    /**
      * 制作个临时表，如果channel_name值为空，将holder_name值赋值给channel_name
      */
    res.selectExpr("policy_code","case when length(channel_name) = 0 or channel_name is null then holder_name else channel_name end as cus","business_line",
        "case when policy_effective_time is null then performance_accounting_day else policy_effective_time end as policy_effective_time")
      .registerTempTable("result_table")


    /**
      * 对上述结果的业务条线和客户进行分组，得到最小的开始时间，作为初投，
      */
    val newAndOldDateReffer = sqlContext.sql(
      """
        |select cus as cus_refer,business_line as business_line_refer,substr(cast(min(policy_effective_time) as string),1,7) as date_refer
        |from result_table
        |where length(cus) > 0
        |GROUP BY cus,business_line
      """.stripMargin)

    /***
      * 将开始时间为空的数据，用业绩核算时间替换，作为比较时间
      */
    val resultTemp = res.selectExpr(
      "policy_code","project_name","product_code","product_name",
      "channel_name","case when length(channel_name) = 0 or channel_name is null then holder_name else channel_name end as cus",
      "biz",
      "performance_accounting_day","holder_name","premium_total",
      "economic_rate",
      "economy_fee","substr(cast((case when policy_effective_time is null then performance_accounting_day else policy_effective_time end) as string),1,7) as date",
      "sale_name","policy_effective_time", "policy_expire_time","underwriting_company",
      "insurance_company_short_name",
      "technical_service_rates",
      "technical_service_fee",
      "has_brokerage","brokerage_ratio","brokerage_fee",
      "num_person","business_line", "short_name","province","source"
    )

    /**
      * 上述结果进行关联的，比较时间和参照时间正在同一个月份作为新客，其他作为老客
      */
     val resultEndTemp =  resultTemp.join(newAndOldDateReffer,'cus === 'cus_refer and 'business_line==='business_line_refer,"leftouter")
       .selectExpr(
         "policy_code","project_name","product_code","product_name",
         "channel_name","cus","substr(cast((case when policy_effective_time is null then performance_accounting_day else policy_effective_time end) as string),1,10) as date",
         "biz",
         "performance_accounting_day","holder_name","premium_total",
         "economic_rate",
         "economy_fee",
         "sale_name","policy_effective_time", "policy_expire_time","underwriting_company",
         "insurance_company_short_name",
         "technical_service_rates",
         "technical_service_fee",
         "has_brokerage","brokerage_ratio","brokerage_fee",
         "num_person","business_line", "short_name","province",
         "case when date = date_refer then '新客' " +
           "when date is not null and date_refer < date then '老客' else null end as new_old_cus","source"
       )

    /**
      * 和雇主的初投+三个月的数据进行关联，得到新的新老客结果
      */
    val resultEnd = resultEndTemp.join(odsChannelFirstThreeMonthTempDetail,'cus==='channel_name_salve and 'business_line==='business_line_salve,"leftouter")
      .selectExpr(
        "policy_code","project_name","product_code","product_name",
        "channel_name",
        "biz",
        "performance_accounting_day","holder_name","premium_total",
        "economic_rate",
        "economy_fee",
        "sale_name","policy_effective_time", "policy_expire_time","underwriting_company",
        "insurance_company_short_name",
        "technical_service_rates",
        "technical_service_fee",
        "has_brokerage","brokerage_ratio","brokerage_fee",
        "num_person","business_line", "short_name","province",
        "new_old_cus","first_start_date",
        "case when business_line = '雇主' and date >= first_start_date and date <= three_month then '新客' " +
          "when business_line = '雇主' and date > three_month then '老客' else null end as new_old_cus_new",
        "case when business_line = '雇主' and date >= first_start_date and date <= six_month then '新客' " +
          "when business_line = '雇主' and date > six_month then '老客' else null end as six_month_new_old_cus_new",
        "source"
      )
      .selectExpr(
        "policy_code","project_name","product_code","product_name",
        "channel_name",
        "biz",
        "performance_accounting_day","holder_name","premium_total",
        "economic_rate",
        "economy_fee",
        "sale_name","policy_effective_time", "policy_expire_time","underwriting_company",
        "insurance_company_short_name",
        "technical_service_rates",
        "technical_service_fee",
        "has_brokerage","brokerage_ratio","brokerage_fee",
        "num_person","business_line", "short_name","province",
        "new_old_cus",
        "new_old_cus_new",
        "six_month_new_old_cus_new",
        "case when new_old_cus_new = '老客' and business_line = '雇主' and first_start_date <= '2017-10-01' then '纯老客' " +
          "when new_old_cus_new = '老客' and business_line = '雇主' and first_start_date > '2017-10-01' and first_start_date <= '2018-10-01' then '2018新转老' " +
          "when new_old_cus_new = '老客' and business_line = '雇主' and first_start_date > '2018-10-01' and first_start_date <= '2019-10-01' then '2019新转老' " +
          "when new_old_cus_new = '老客' and business_line = '雇主' and first_start_date > '2019-10-01' then '2020新转老' " +
          "else null end cus_type_new",
        "source"
      )

    resultEnd.printSchema()
    resultEnd
//    val tableName = "accounts_and_tmt_detail"
    //    saveASMysqlTable(res: DataFrame, tableName: String, SaveMode.Overwrite,user:String,pass:String,driver:String,url:String)
  }
}
