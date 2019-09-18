package bzn.dw.bclickthrough

import java.text.SimpleDateFormat
import java.util.Date

import bzn.dw.util.SparkUtil
import bzn.job.common.Until
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * author:xiaoYuanRen
  * Date:2019/9/18
  * Time:19:07
  * describe: b点通 雇主业务续投 续保，提升业绩和人员延续
  **/
object DwEmployerPolicyContinueDetailTest extends SparkUtil with Until{
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName,"local[*]")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    continueProposalDetail(hiveContext)
    //    res.write.mode(SaveMode.Overwrite).saveAsTable("dwdb.dw_policy_premium_detail")
    sc.stop()
  }

  /**
    * 续投保保单统计
    * @param sqlContext //上下文
    */
  def continueProposalDetail(sqlContext:HiveContext) = {
    sqlContext.udf.register ("getUUID", () => (java.util.UUID.randomUUID () + "").replace ("-", ""))
    sqlContext.udf.register ("getNow", () => {
      val df = new SimpleDateFormat ("yyyy-MM-dd HH:mm:ss")
      //设置日期格式
      val date = df.format (new Date ()) // new Date()为获取当前系统时间
      date + ""
    })

    sqlContext.udf.register ("getDayId", () => {
      /**
        * 当前时间的day_id
        */
      val nowDayId = getNowTime().substring(0,10).replaceAll("-","")
      nowDayId
    })
    /**
      * 国寿财和中华  保单层级的续投
      */
    /**
      * 读取保单明细表
      */
    val odsPolicyDetail =
      sqlContext.sql ("select policy_id,policy_code,product_code,policy_status,policy_start_date,policy_end_date,insure_company_name," +
        "holder_name,preserve_policy_no from odsdb.ods_policy_detail")
        .where ("policy_status in (1,0,-1)")
        .cache ()

    /**
      * 读取产品表
      */
    val odsProductDetail =
      sqlContext.sql ("select product_code as product_code_slave,product_name,one_level_pdt_cate from odsdb.ods_product_detail")
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
        "product_name",
        "preserve_policy_no"
      )
      .where("preserve_policy_no is not null")

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
        "product_name",
        "preserve_policy_no",
        "sku_coverage",
        "sku_charge_type",
        "sku_price"
      )
      .where("sku_charge_type = '1'")
      .cache()

    /**
      * 上述结果和当前在保人表进行左连接
      */
    policyProductPlanRes.join(dwPolicyCurrInsuredDetail,policyProductPlanRes("policy_id")===dwPolicyCurrInsuredDetail("policy_id_insured"),"leftouter")
      .selectExpr(
        "policy_id",
        "policy_code",
        "product_code",
        "policy_start_date",
        "policy_end_date",
        "insure_company_name",//保险公司
        "holder_name",
        "product_name",
        "preserve_policy_no",
        "sku_coverage",
        "sku_charge_type",
        "sku_price",
        "day_id",
        "case when day_id = getDayId()",
        "count as curr_insured"
      )

    /**
      * 众安
      */
  }

  /**
    * 续保保单统计
    * @param sqlContext //上下文
    */
  def continuePolicyDetail(sqlContext:HiveContext) = {

  }

  /**
    * 读取公共信息
    * @param sqlContext
    */
  def publicInfo(sqlContext:HiveContext) = {
    /**
      * 读取企业信息
      */
    val odsEnterpriseDetail =
      sqlContext.sql ("select ent_id as ent_id_master,ent_name from odsdb.ods_enterprise_detail")

    /**
      * 读取渠道表
      */
    val odsEntGuzhuSalesmanDetail =
      sqlContext.sql ("select ent_id,salesman,biz_operator,consumer_category,channel_id," +
        "case when channel_name = '直客' then ent_name else channel_name end as channel_name" +
        " from odsdb.ods_ent_guzhu_salesman_detail")

    /**
      * 投保人和渠道关联
      */
    val entAndChannelRes = odsEnterpriseDetail.join(odsEntGuzhuSalesmanDetail,odsEnterpriseDetail("ent_id_master")===odsEntGuzhuSalesmanDetail("ent_id"))
      .selectExpr("ent_id","ent_name","salesman","biz_operator","consumer_category","channel_id","channel_name")

    /**
      * 读取销售信息表
      */
    val odsEntSalesTeamDimension =
      sqlContext.sql ("select sale_name,team_name from odsdb.ods_ent_sales_team_dimension")

    /**
      * 渠道销售和企业的最终结果
      */
    val entAndChannelAndSaleRes = entAndChannelRes.join(odsEntSalesTeamDimension,entAndChannelRes("salesman")===odsEntSalesTeamDimension("sale_name"),"leftouter")
      .selectExpr("ent_id","ent_name","salesman","team_name","biz_operator","consumer_category","channel_id","channel_name")
      .distinct()
  }
}
