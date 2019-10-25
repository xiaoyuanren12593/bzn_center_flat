package bzn.dw.saleeasy

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import bzn.dw.util.SparkUtil
import bzn.job.common.Until
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * author:xiaoYuanRen
  * Date:2019/6/10
  * Time:9:42
  * describe: 销售易  每日在保人数据明细表
  **/
object DwSaleEasyPolicyCurrInsuredDetailTest extends SparkUtil with Until{
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName,"local[*]")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    val res = DwSaleEasyPolicyCurrInsuredDetail(hiveContext)

    sc.stop()
  }

  /**
    *
    * @param sqlContext 上下文
    */
  def DwSaleEasyPolicyCurrInsuredDetail(sqlContext:HiveContext) :DataFrame= {
    import sqlContext.implicits._
    sqlContext.udf.register ("getUUID", () => (java.util.UUID.randomUUID () + "").replace ("-", ""))
    sqlContext.udf.register ("getNow", () => {
      val df = new SimpleDateFormat ("yyyy-MM-dd HH:mm:ss")
      //设置日期格式
      val date = df.format (new Date ()) // new Date()为获取当前系统时间
      date + ""
    })

    /**
      * 读取地域维度表
      */
    val odsAreaInfoDimension = sqlContext.sql ("select code,short_name,province from odsdb.ods_area_info_dimension")

    /**
      * 读取保单明细表
      */
    val odsPolicyDetailTempOne =
      sqlContext.sql ("select policy_id,policy_code,product_code,policy_status,policy_start_date,policy_end_date,insure_company_name," +
        "holder_name,insured_subject as insurant_company_name,policy_create_time,policy_update_time," +
        "case when belongs_regional is null then belongs_regional else concat(substr(belongs_regional,1,4),'00') end as belongs_regional " +
        "from odsdb.ods_policy_detail")
        .where ("policy_status in (1,0,-1)")
        .cache ()

    /**
      * 保单数据和地域维度信息进行关联
      */
    val odsPolicyDetailTempTwo = odsPolicyDetailTempOne.join(odsAreaInfoDimension,odsPolicyDetailTempOne("belongs_regional")===odsAreaInfoDimension("code"),"leftouter")
      .selectExpr(
        "policy_id","policy_code","product_code","policy_start_date","policy_end_date","insure_company_name","holder_name",
        "insurant_company_name","policy_create_time","policy_update_time",
        "belongs_regional","short_name as holder_city","province as holder_province"
      )

    /**
      * 读取产品表
      */
    val odsProductDetail =
      sqlContext.sql ("select product_code as product_code_slave,product_name,one_level_pdt_cate,two_level_pdt_cate from odsdb.ods_product_detail")
        .where ("one_level_pdt_cate  = '蓝领外包' and product_code_slave not in ('LGB000001','17000001')")
       .cache()

    /**
      * 读取方案表
      */
    val odsPolicyProductPlanDetail =
      sqlContext.sql ("select policy_code as policy_code_plan,sku_coverage,sku_ratio,sku_charge_type,sku_price,sku_append " +
        "from odsdb.ods_policy_product_plan_detail")
        .where("policy_code_plan is not null")
        .cache()

    /***
      * 读取企业信息表
      */
    val odsEnterpriseDetail =
      sqlContext.sql ("select ent_id as ent_id_master,ent_name from odsdb.ods_enterprise_detail")

    /**
      * 读取方案工种级别
      */
    val odsWorkGradeDimension =
      sqlContext.sql ("select policy_code as policy_code_slave,profession_type from odsdb.ods_work_grade_dimension")

    val odsPolicyDetail =
      odsPolicyDetailTempTwo.join(odsWorkGradeDimension,odsPolicyDetailTempTwo("policy_code")===odsWorkGradeDimension("policy_code_slave"),"leftouter")
      .selectExpr( "policy_id","policy_code","product_code","policy_start_date","policy_end_date","insure_company_name","holder_name",
        "insurant_company_name","policy_create_time","policy_update_time","profession_type",
        "belongs_regional","holder_city","holder_province")

    /**
      * 读取渠道表
      */
    val odsEntGuzhuSalesmanDetail =
      sqlContext.sql ("select ent_id,salesman,biz_operator,consumer_category,channel_id," +
        "case when channel_name = '直客' then ent_name else channel_name end as channel_name ," +
        "case when channel_name ='直客' then channel_name else '渠道' end as customer_type from odsdb.ods_ent_guzhu_salesman_detail")

    val entAndChannelRes = odsEnterpriseDetail.join(odsEntGuzhuSalesmanDetail,odsEnterpriseDetail("ent_id_master")===odsEntGuzhuSalesmanDetail("ent_id"))
      .selectExpr("ent_id","ent_name","salesman","biz_operator","consumer_category","channel_id","channel_name","customer_type")

    /**
      * 读取销售信息表
      */
    val odsEntSalesTeamDimension =
      sqlContext.sql ("select sale_name,team_name from odsdb.ods_ent_sales_team_dimension")

    /**
      * 渠道销售和企业的最终结果
      */
    val entAndChannelAndSaleRes = entAndChannelRes.join(odsEntSalesTeamDimension,entAndChannelRes("salesman")===odsEntSalesTeamDimension("sale_name"),"leftouter")
      .selectExpr("ent_id","ent_name","salesman","team_name","biz_operator","consumer_category","channel_id","channel_name","customer_type")
      .distinct()

    /**
      * 读取当前在保人表
      */
    val dwPolicyCurrInsuredDetail = sqlContext.sql("select policy_id as policy_id_insured,day_id,count from dwdb.dw_policy_curr_insured_detail")

    /***
      * 保单和产品进行关联的到结果
      */
    val policyProductRes = odsPolicyDetail.join(odsProductDetail,odsPolicyDetail("product_code")===odsProductDetail("product_code_slave"))
      .selectExpr(
        "policy_id",
        "policy_code",
        "product_code",
        "policy_start_date",
        "policy_end_date",
        "insure_company_name",//保险公司
        "holder_name",
        "insurant_company_name",//被保企业
        "profession_type",
        "product_name",
        "two_level_pdt_cate",
        "holder_province",
        "holder_city",
        "policy_create_time",
        "policy_update_time"
      )

    /**
      * 上述结果和方案表进行关联
      */
    val policyProductPlanRes = policyProductRes.join(odsPolicyProductPlanDetail,policyProductRes("policy_code")===odsPolicyProductPlanDetail("policy_code_plan"))
      .selectExpr(
        "policy_id",
        "policy_code",
        "product_code",
        "policy_start_date",
        "policy_end_date",
        "insure_company_name",//保险公司
        "holder_name",
        "insurant_company_name",//被保企业
        "product_name",
        "two_level_pdt_cate",
        "holder_province",
        "holder_city",
        "sku_coverage",
        "sku_ratio",
        "sku_charge_type",
        "sku_price",
        "sku_append",
        "profession_type",
        "policy_create_time",
        "policy_update_time"
      )
      .cache()

    /***
      * 上述结果和企业和渠道和销售信息关联
      */
    val policyProductPlanEntRes = policyProductPlanRes.join(entAndChannelAndSaleRes,policyProductPlanRes("holder_name")===entAndChannelAndSaleRes("ent_name"),"leftouter")
      .selectExpr(
        "policy_id",
        "policy_code",
        "product_code",
        "policy_start_date",
        "policy_end_date",
        "insure_company_name",//保险公司
        "insurant_company_name",//被保企业
        "product_name",
        "two_level_pdt_cate",
        "holder_province",
        "holder_city",
        "sku_coverage",
        "sku_ratio",
        "sku_charge_type",
        "sku_price",
        "sku_append",
        "profession_type",
        "ent_id",
        "ent_name",
        "salesman",
        "team_name",
        "biz_operator",
        "consumer_category",
        "channel_id",
        "channel_name",
        "customer_type",
        "policy_create_time",
        "policy_update_time"
      )

    val resOne = policyProductPlanEntRes.join(dwPolicyCurrInsuredDetail,policyProductPlanEntRes("policy_id")===dwPolicyCurrInsuredDetail("policy_id_insured"))
      .selectExpr(
        "policy_id",
        "policy_code",
        "product_code",
        "policy_start_date",
        "policy_end_date",
        "insure_company_name",//保险公司
        "insurant_company_name",//被保企业
        "product_name",
        "two_level_pdt_cate",
        "holder_province",
        "holder_city",
        "sku_coverage",
        "sku_ratio",
        "sku_charge_type",
        "sku_price",
        "sku_append",
        "profession_type",
        "ent_id",
        "ent_name",
        "salesman",
        "team_name",
        "biz_operator",
        "consumer_category",
        "channel_id",
        "channel_name",
        "customer_type",
        "count",
        "day_id",
        "substr(day_id,1,6) as new_old_time",
        "policy_create_time",
        "policy_update_time"
      )

    /**
      * 确定新老客时间
      */
    val newOldTimeRes = policyProductPlanEntRes.selectExpr("policy_id","policy_start_date","ent_id")
      .where("policy_start_date is not null and ent_id is not null")
      .map(x => {
        val policyId = x.getAs[String]("policy_id")
        val entId = x.getAs[String]("ent_id")
        val policyStartDate = x.getAs[java.sql.Timestamp]("policy_start_date")
        (entId,(policyId,policyStartDate))
      })
      .reduceByKey((x1,x2) => {
        val res = if(x1._2.compareTo(x2._2) < 0) x1 else x2
        res
      })
      .map(x => {
        val newOldTime = getTimeYearAndMonth(x._2._2)
        (x._2._1,newOldTime)
      })
      .toDF("policy_id_slave","new_old_time_slave")
      .distinct()


    val res = resOne.join(newOldTimeRes,resOne("policy_id")===newOldTimeRes("policy_id_slave"),"leftouter")
      .selectExpr(
        "getUUID() as id",
        "policy_id",
        "policy_code",
        "product_code",
        "policy_start_date",
        "policy_end_date",
        "insure_company_name",//保险公司
        "insurant_company_name",//被保企业
        "product_name",
        "two_level_pdt_cate",
        "holder_province",
        "holder_city",
        "sku_coverage",
        "sku_ratio",
        "sku_charge_type",
        "sku_price",
        "sku_append",
        "profession_type",
        "ent_id",
        "ent_name",
        "salesman",
        "team_name",
        "biz_operator",
        "consumer_category",
        "channel_id",
        "channel_name",
        "customer_type",
        "count as curr_insured",
        "day_id",
        "concat(substr(day_id,1,4),'-',substr(day_id,5,2),'-',substr(day_id,7,2))  as date_time",
        "case when policy_id_slave is not null and new_old_time = new_old_time_slave then '新客' else '老客' end as is_old_customer",
        "policy_create_time",
        "policy_update_time",
        "getNow() as dw_create_time"
      )

    res.printSchema()


    odsPolicyDetail
  }
}

