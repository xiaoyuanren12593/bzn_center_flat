package bzn.dw.premium

import bzn.dw.util.SparkUtil
import bzn.job.common.{MysqlUntil, Until}
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * author:xiaoYuanRen
  * Date:2019/10/23
  * Time:16:10
  * describe: 每天新增的数据
  **/
object DwPolicyStreamingDetail extends SparkUtil with Until with MysqlUntil{
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName,"")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    val res = getHolderInfo(hiveContext)
    hiveContext.sql("truncate table dwdb.dw_policy_streaming_detail")
    res.repartition(1).write.mode(SaveMode.Append).saveAsTable("dwdb.dw_policy_streaming_detail")

    sc.stop()
  }


  def getHolderInfo(sqlContext:HiveContext) = {
    sqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    sqlContext.udf.register("clean", (str: String) => clean(str))

    /**
      * 读取近5d新增的保单数据
      */
    val odsPolicyStreamingDetail = sqlContext.sql("select * from odsdb.ods_policy_streaming_detail")
      .selectExpr(
        "proposal_no",
        "policy_code",
        "policy_no",
        "holder_name",
        "channel_id",
        "channel_name",
        "status",
        "payment_status",//支付状态
        "ledger_status",//实收状态
        "big_policy",//是否是大保单
        "proposal_time_policy",//投保时间
        "policy_start_date",//保单起期
        "policy_end_date",//投保止期
        "insured_count",
        "insured_company",//被保人企业
        "sku_charge_type",//方案类别
        "insurance_name",
        "product_code",
        "sales_name",
        "biz_operator"
      )

    /**
      * 产品表
      */
    val odsProductDetail = sqlContext.sql("select * from odsdb.ods_product_detail")
      .selectExpr("product_code as product_code_slave","one_level_pdt_cate")

    /**
      * 读取保单明细表
      */
    val odsPolicyDetail = sqlContext.sql("select policy_code as policy_code_slave,policy_status from odsdb.ods_policy_detail")
      .where("policy_status in (0,1,-1) and policy_code_slave is not null")

    /**
      * 读取批单表
      */
    val odsPreseveDetail = sqlContext.sql("select inc_dec_order_no as inc_dec_order_no_slave,preserve_status from odsdb.ods_preservation_detail")
      .where("preserve_status = 1 and inc_dec_order_no_slave is not null")

    /**
      * 得到全部的雇主信息
      */
    val policyStreaming = odsPolicyStreamingDetail.join(odsProductDetail,odsPolicyStreamingDetail("product_code")===odsProductDetail("product_code_slave"),"leftouter")
      .where("one_level_pdt_cate not in ('17000001','LGB000001') and one_level_pdt_cate = '蓝领外包'")
      .selectExpr(
        "proposal_no",
        "policy_code",
        "policy_no",
        "holder_name",
        "channel_id",
        "channel_name",
        // 1	待提交
        // 2	待审核
        // 3	未支付
        // 4	未承保

        // 5	承保完成
        // 6	保单过期
        // 7	核心已经在保
        // 8	其他
        "case when status = 1 then 1 " +
          "when status = 2 then 2 " +
          "when status = 3 and payment_status = 1 then 3 " +
          "when status = 3 and ledger_status = 1 and payment_status = 2 then 4 " +
          "when status = 3 and ledger_status = 3 and payment_status = 2 then 4 " +
          "when status = 5 then 5 " +
          "when status = 6 or status = 7 then 6 " +
          "else 8 end as status",
        "big_policy",//是否是大保单
        "proposal_time_policy",//投保时间
        "policy_start_date",//保单起期
        "policy_end_date",//投保止期
        "insured_count",
        "insured_company",//被保人企业
        "sku_charge_type",//方案类别
        "insurance_name",
        "'' as inc_dec_order_no",
        "product_code",
        "sales_name",
        "biz_operator"
      )

    /**
      * 近5d内的数据在核心数据不存在的
      */
    val policyStreamingRes = policyStreaming.join(odsPolicyDetail,policyStreaming("policy_code")===odsPolicyDetail("policy_code_slave"),"leftouter")
      .where("policy_code_slave is null")
      .selectExpr(
        "proposal_no",
        "policy_code",
        "policy_no",
        "holder_name",
        "channel_id",
        "channel_name",
        "status",
        "big_policy",//是否是大保单
        "cast('' as timestamp) as proposal_time_preserve",//批单投保时间
        "proposal_time_policy",//保单投保时间
        "policy_start_date",//保单起期
        "policy_end_date",//投保止期
        "cast('' as timestamp) as preserve_start_date",
        "cast('' as timestamp) as preserve_end_date",
        "insured_count",
        "insured_company",//被保人企业
        "insurance_name",
        "sku_charge_type",//方案类别
        "inc_dec_order_no",
        "sales_name",
        "biz_operator"
      )

    /**
      * 读取近5d新增的批单数据
      */
    val odsPreserveStreamingDetail = sqlContext.sql("select * from odsdb.ods_preserve_streaming_detail")
      .selectExpr(
        "proposal_no",
        "policy_code",
        "policy_no",
        "holder_name",
        "channel_id",
        "channel_name",
        "case when status = 1 then 1 " +
          "when status = 3 then 2 " +
          "when status = 7 then 5 " +
          "else 8 end as status",
        "big_policy",
        "proposal_time_preserve",//批单投保时间
        "proposal_time_policy",//保单投保时间
        "policy_start_date",//保单起期
        "policy_end_date",//投保止期
        "preserve_start_date",
        "preserve_end_date",
        "insured_count",
        "insured_company",//被保人企业
        "insurance_name",
        "sku_charge_type",
        "inc_dec_order_no",
        "sales_name",
        "biz_operator"
      )

    /**
      * 近5d内存批单的数据在核心中不存在的
      */
    val odsPreserveStreamingDetailRes = odsPreserveStreamingDetail.join(odsPreseveDetail,odsPreserveStreamingDetail("inc_dec_order_no")===odsPreseveDetail("inc_dec_order_no_slave"),"leftouter")
      .where("inc_dec_order_no_slave is null")
      .selectExpr(
        "proposal_no",
        "policy_code",
        "policy_no",
        "holder_name",
        "channel_id",
        "channel_name",
        "status",
        "big_policy",
        "proposal_time_preserve",//批单投保时间
        "proposal_time_policy",//保单投保时间
        "policy_start_date",//保单起期
        "policy_end_date",//投保止期
        "preserve_start_date",
        "preserve_end_date",
        "insured_count",
        "insured_company",//被保人企业
        "insurance_name",
        "sku_charge_type",
        "inc_dec_order_no",
        "sales_name",
        "biz_operator"
      )

    /**
      * 5天之前的保单和批单的数据
      */
    val data5DBefore = policyStreamingRes.unionAll(odsPreserveStreamingDetailRes)

    /**
      * 保险公司简称
      */
    val odsInsuranceCompanyTempDimension = sqlContext.sql("select insurance_company,short_name from odsdb.ods_insurance_company_temp_dimension")

    /**
      * 关联得到保险公司简称
      */
    val data5DBeforeRes = data5DBefore.join(odsInsuranceCompanyTempDimension,data5DBefore("insurance_name")===odsInsuranceCompanyTempDimension("insurance_company"),"leftouter")
      .drop("insurance_company").withColumnRenamed("short_name","insurance_company_short_name")

    /**
      * 读取雇主销售表
      */
    val odsEntGuzhuSalesmanDetail = sqlContext.sql("select  ent_id,ent_name,salesman as salesman_slave,biz_operator as biz_operator_slave,channel_id as channel_id_slave," +
      "case when channel_name = '直客' then ent_name else channel_name end as channel_name_slave from odsdb.ods_ent_guzhu_salesman_detail")

    val res = data5DBeforeRes.join(odsEntGuzhuSalesmanDetail,data5DBeforeRes("holder_name")===odsEntGuzhuSalesmanDetail("ent_name"),"leftouter")
      .selectExpr(
        "getUUID() as id",
        "proposal_no",
        "policy_code",
        "policy_no",
        "case when channel_id_slave is null then null else ent_id end as ent_id",
        "case when channel_id_slave is null then holder_name else ent_name end as ent_name",
        "case when channel_id_slave is not null then channel_id_slave else channel_id end as channel_id",
        "case when channel_name_slave is not null then channel_name_slave else channel_name end as channel_name",
        "status",
        "big_policy",
        "proposal_time_preserve",//批单投保时间
        "proposal_time_policy",//保单投保时间
        "policy_start_date",//保单起期
        "policy_end_date",//投保止期
        "preserve_start_date",
        "preserve_end_date",
        "insured_count",
        "insured_company",//被保人企业
        "insurance_name",
        "insurance_company_short_name",
        "sku_charge_type",
        "clean(inc_dec_order_no) as inc_dec_order_no",
        "clean(case when salesman_slave is not null then salesman_slave else sales_name end) as  sales_name",
        "clean(case when biz_operator_slave is not null then biz_operator_slave else biz_operator end) as biz_operator",
        "date_format(now(),'yyyy-MM-dd HH:mm:ss') as create_time",
        "date_format(now(),'yyyy-MM-dd HH:mm:ss') as update_time"
      )

    res
  }
}
