package bzn.dw.bclickthrough

import java.text.SimpleDateFormat
import java.util.Date

import bzn.dw.util.SparkUtil
import bzn.job.common.Until
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * author:xiaoYuanRen
  * Date:2019/9/23
  * Time:19:34
  * describe: this is new class
  **/
object DwEmployerProposalContinueDetailTest extends SparkUtil with Until{
  def main (args: Array[String]): Unit = {
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
    import sqlContext.implicits._
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
        "holder_name,preserve_policy_no,datediff(policy_end_date,policy_start_date) as diffDate from odsdb.ods_policy_detail")
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

    /**
      * 上述结果和方案表进行关联
      */
    val policyProductPlanRes = policyProductRes.join(odsPolicyProductPlanDetail,policyProductRes("policy_code")===odsPolicyProductPlanDetail("policy_code_plan"))
      .selectExpr(
        "policy_id",
        "policy_code",
        "policy_start_date",
        "policy_end_date",
        "insure_company_name",//保险公司
        "holder_name",
        "product_code",
        "product_name",
        "preserve_policy_no",
        "sku_coverage",
        "sku_charge_type",
        "sku_price"
      )
      .cache()

    /**
      * 月单保单续投结果
      */
    val monthHiveContinuePolicyCodeRes =  monthContinueProposalDetail(sqlContext,odsPolicyDetail,policyProductPlanRes)

    /**
      * 获取去基础数据
      */
    val baseInfoRes = publicInfo(sqlContext)

    /**
      * 月单保单续投结果与基础初级关联
      */
    val proposalRes = monthHiveContinuePolicyCodeRes.join(baseInfoRes,monthHiveContinuePolicyCodeRes("holder_name")===baseInfoRes("ent_name"),"leftouter")
      .selectExpr(
        "getUUID() as id",
        "policy_id",
        "policy_code",
        "policy_start_date",
        "policy_end_date",
        "insure_company_name",//保险公司
        "product_code",
        "product_name",
        "continue_policy_id",
        "preserve_policy_no",
        "cast(sku_coverage as decimal(14,4)) as sku_coverage",
        "sku_charge_type",
        "cast(sku_price as decimal(14,4)) as sku_price",
        "now_date",
        "should_continue_policy_date",
        "realy_continue_policy_date",
        "month",
        "ent_id",
        "ent_name",
        "salesman",
        "team_name",
        "biz_operator",
        "consumer_category",
        "channel_id",
        "channel_name",
        "getNow() as dw_create_time"
      )
    proposalRes.printSchema()

    /**
      * ######################批单续投
      */

  }

  /**
    * 月单保单续投结果
    * @param sqlContext 上下文
    * @param odsPolicyDetail 保单明细数据
    * @param policyProductPlanRes 结果表
    * @return
    */
  def monthContinueProposalDetail(sqlContext: HiveContext,odsPolicyDetail: DataFrame,policyProductPlanRes:DataFrame) = {
    import sqlContext.implicits._
    /**
      * 临时表
      */
    val continueTemp = odsPolicyDetail.selectExpr ("policy_id as continue_policy_id", "preserve_policy_no as continue_policy_code", "policy_start_date as continue_policy_start_date", "policy_end_date as continue_policy_end_date")

    val policyProductPlanTempRes = policyProductPlanRes
      .where ("sku_charge_type = '1'")
      .where ("datediff(policy_end_date,policy_start_date)<=31")

    /**
      * 得到续投保单号的结束时间，判断实际续投人数
      */
    val policyProductPlanContinueTemp = policyProductPlanTempRes
      .join (continueTemp, policyProductPlanRes ("policy_code") === continueTemp ("continue_policy_code"), "leftouter")

    val policyProductPlanContinueRes = policyProductPlanContinueTemp.map (x => {
      val policyId = x.getAs [String]("policy_id")
      val policyCode = x.getAs [String]("policy_code")
      val policyStartDate = x.getAs [java.sql.Timestamp]("policy_start_date")
      val policyEndDate = x.getAs [java.sql.Timestamp]("policy_end_date")
      val continuePolicyEndDate = x.getAs [java.sql.Timestamp]("continue_policy_end_date")
      val insureCompanyName = x.getAs [String]("insure_company_name")
      val holderName = x.getAs [String]("holder_name")
      val productCode = x.getAs [String]("product_code")
      val productName = x.getAs [String]("product_name")
      val preservePolicyNo = x.getAs [String]("preserve_policy_no")
      val continuePolicyId = x.getAs [String]("continue_policy_id")
      val skuCoverage = x.getAs [java.math.BigDecimal]("sku_coverage")
      val skuChargeType = x.getAs [String]("sku_charge_type")
      val skuPrice = x.getAs [java.math.BigDecimal]("sku_price")

      //当前时间
      val nowDate = getNowTime ().substring (0, 10).replaceAll ("-", "")

      //应续投时间  保单结束时间+1天
      val shouldContinuePolicyDate = if( policyEndDate != null ) {
        currTimeFuction (policyEndDate.toString, 1).substring (0, 10).replaceAll ("-", "")
      } else {
        null
      }

      //实际续投时间 确定人数
      val realyContinuePolicyDate = if( continuePolicyEndDate != null ) {
        if( continuePolicyEndDate.toString.compareTo (getNowTime ()) > 0 ) {
          nowDate
        } else {
          continuePolicyEndDate.toString.substring (0, 10).replaceAll ("-", "")
        }
      } else {
        null
      }

      //月份
      val month = if( policyEndDate != null ) {
        getTimeYearAndMonth (currTimeFuction (policyEndDate.toString, 1).substring (0, 7).replaceAll ("-", ""))
      } else {
        null
      }

      (policyId, policyCode, policyStartDate, policyEndDate, insureCompanyName, holderName, productCode, productName, continuePolicyId, preservePolicyNo,
        skuCoverage, skuChargeType, skuPrice, nowDate, shouldContinuePolicyDate, realyContinuePolicyDate, month)
    })
    .toDF (
      "policy_id",
      "policy_code",
      "policy_start_date",
      "policy_end_date",
      "insure_company_name", //保险公司
      "holder_name",
      "product_code",
      "product_name",
      "continue_policy_id",
      "preserve_policy_no",
      "sku_coverage",
      "sku_charge_type",
      "sku_price",
      "now_date",
      "should_continue_policy_date",
      "realy_continue_policy_date",
      "month"
    )
    /**
      * 月单中，保单期间是一个月的并且可以利用续投保单号进行续投的数据
      */
    val monthHiveContinuePolicyCodeRes = policyProductPlanContinueRes
    monthHiveContinuePolicyCodeRes
  }

  /**
  * 续保保单统计
  * @param sqlContext //上下文
  */
  def continuePolicyDetail(sqlContext:HiveContext,dataframe:DataFrame) = {

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
    entAndChannelAndSaleRes
  }
}

