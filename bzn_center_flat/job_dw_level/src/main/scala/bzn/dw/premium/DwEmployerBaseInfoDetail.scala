package bzn.dw.premium

import java.text.SimpleDateFormat
import java.util.Date

import bzn.dw.util.SparkUtil
import bzn.job.common.Until
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext

/*
* @Author:liuxiang
* @Date：2019/9/23
* @Describe: 雇主基础信息表
*/
object DwEmployerBaseInfoDetail extends SparkUtil with Until {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    val res = DwEmployerBaseInfoDetail(hiveContext)
    hiveContext.sql("truncate table dwdb.dw_employer_baseinfo_detail")
    res.repartition(1).write.mode(SaveMode.Append).saveAsTable("dwdb.dw_employer_baseinfo_detail")
   // res.repartition(1).write.mode(SaveMode.Overwrite).parquet("/dw_data/dw_data/dw_employer_baseinfo_detail")
    sc.stop()
  }

  /**
    *
    * @param sqlContext 获取相关的信息
    * @return
    */

  def DwEmployerBaseInfoDetail(sqlContext: HiveContext) = {
    sqlContext.udf.register("clean", (str: String) => clean(str))
    sqlContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      //设置日期格式
      val date = df.format(new Date()) // new Date()为获取当前系统时间
      (date + "")
    })

    //读取保单明细表
    val odsPolicyDetail: DataFrame = sqlContext.sql("select policy_id,policy_code,holder_name,insured_subject,product_code " +
      ",policy_status from odsdb.ods_policy_detail")
      .where("policy_status in (1,0,-1)")

    //读取企业联系人
    val odsEnterpriseDetail = sqlContext.sql("select ent_id,ent_name from odsdb.ods_enterprise_detail")

    //读取客户归属销售表
    val odsEntGuzhuDetail: DataFrame =
      sqlContext.sql("select ent_id as entid,ent_name as entname,channel_id,(case when channel_name='直客' then ent_name else channel_name end) as channel_name from odsdb.ods_ent_guzhu_salesman_detail")

    // 将企业联系人 与 客户归属销售表关联  拿到 渠道id和name
    val enterperiseAndEntGuzhu = odsEnterpriseDetail.join(odsEntGuzhuDetail, odsEnterpriseDetail("ent_id") === odsEntGuzhuDetail("entid"), "leftouter")
      .selectExpr("ent_id", "ent_name", "channel_id", "channel_name")

    // 将关联结果与保单明细表关联
    val resDetail = odsPolicyDetail.join(enterperiseAndEntGuzhu, odsPolicyDetail("holder_name") === enterperiseAndEntGuzhu("ent_name"), "leftouter")
      .selectExpr("policy_id", "policy_code", "holder_name", "insured_subject", "product_code")

    //读取产品表
    val odsProductDetail = sqlContext.sql("select product_code as product_code_temp,product_name,one_level_pdt_cate from odsdb.ods_product_detail")

    //将关联结果与产品表关联 拿到产品类别
    val resProductDetail = resDetail.join(odsProductDetail, resDetail("product_code") === odsProductDetail("product_code_temp"), "leftouter")
      .selectExpr("policy_id", "policy_code", "holder_name", "insured_subject", "product_code", "one_level_pdt_cate")

    /**
      * 读取理赔表
      */
    val dwPolicyClaimDetail = sqlContext.sql("SELECT policy_id as id, sum(pre_com) as pre_com,sum(final_payment) as final_payment,sum(res_pay) as res_pay from dwdb.dw_policy_claim_detail GROUP BY policy_id")

    /**
      * 将上述结果与理赔表关联
      */
    val insuredAndClaimRes = resProductDetail.join(dwPolicyClaimDetail, resProductDetail("policy_id") === dwPolicyClaimDetail("id"), "leftouter")
      .selectExpr("policy_id", "policy_code", "holder_name", "insured_subject", "product_code", "one_level_pdt_cate", "pre_com", "final_payment", "res_pay")

    //读取方案信息表
    val odsPolicyProductPlanDetail: DataFrame = sqlContext.sql("select policy_code as policy_code_temp,product_code as product_code_temp,sku_coverage,sku_append," +
      "sku_ratio,sku_price,sku_charge_type,tech_service_rate,economic_rate," +
      "commission_discount_rate,commission_rate from odsdb.ods_policy_product_plan_detail")


    //将上述结果与方案信息表关联
    val res = insuredAndClaimRes.join(odsPolicyProductPlanDetail, insuredAndClaimRes("policy_code") === odsPolicyProductPlanDetail("policy_code_temp"), "leftouter")
      .selectExpr("clean(policy_id) as policy_id", "clean(policy_code) as policy_code", " clean(holder_name) as holder_name", "clean(insured_subject) as insured_subject", "clean(product_code) as product_code",
        "clean(one_level_pdt_cate) as one_level_pdt_cate", "sku_coverage", "clean(sku_append) as sku_append", "clean(sku_ratio) as sku_ratio", "sku_price",
        "clean(sku_charge_type) as sku_charge_type ",
        "tech_service_rate", "economic_rate", "commission_discount_rate", "commission_rate","pre_com", "final_payment", "res_pay",
        "getNow() as dw_create_time")

    res

  }


}
