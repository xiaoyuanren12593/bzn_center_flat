package bzn.dw.premium

import java.text.SimpleDateFormat
import java.util.Date

import bzn.dw.util.SparkUtil
import bzn.job.common.Until
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext

/**
  * author:xiaoYuanRen
  * Date:2019/12/25
  * Time:14:19
  * describe: 处理ods层的投保单与批单数据
  **/
object DwProposalDetailStreamingDetail extends SparkUtil with Until {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName,"")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    val res = getProposalData(hiveContext)
    hiveContext.sql("truncate table dwdb.dw_proposal_operator_daily_detail")
    res.repartition(10).write.mode(SaveMode.Append).saveAsTable("dwdb.dw_proposal_operator_daily_detail")

    sc.stop()
  }

  def getProposalData(sqlContext:HiveContext): DataFrame = {
    import sqlContext.implicits._
    sqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    sqlContext.udf.register("clean", (str: String) => clean(str))
    sqlContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")//设置日期格式
      val date = df.format(new Date())// new Date()为获取当前系统时间
      date + ""
    })

    /**
      * 读取运营日报投保单和批单的明细数据
      */
    val odsProposalOperatorDailyDetail = sqlContext.sql("select * from odsdb.ods_proposal_operator_daily_detail")
      .selectExpr(
        "id",
        "policy_code",
        "add_person_count",
        "del_person_count",
        "insurance_name",
        "effective_date",
        "status",
        "preserve_type",
        "sku_price",
        "premium",
        "add_premium",
        "del_premium",
        "sku_coverage",
        "sku_ratio",
        "sku_charge_type",
        "dw_create_time"
      )

    /**
      * 读取保险公司简称表
      */
    val odsInsuranceCompanyTempDimension = sqlContext.sql("select insurance_company,short_name from odsdb.ods_insurance_company_temp_dimension")

    /**
      * 得到保险公司简称
      */
    val allData = odsProposalOperatorDailyDetail.join(odsInsuranceCompanyTempDimension,'insurance_name==='insurance_company,"leftouter")
      .drop("insurance_company").withColumnRenamed("short_name","insurance_company_short_name")

    /**
      * 读取工种方案表
      */
    val odsWorkGradeDetail = sqlContext.sql("select policy_code as policy_code_slave,profession_type from odsdb.ods_work_grade_detail")

    val policyData = allData.join(odsWorkGradeDetail,allData("policy_code")===odsWorkGradeDetail("policy_code_slave"),"leftouter")
      .selectExpr(
        "id",
        "policy_code",
        "add_person_count",
        "del_person_count",
        "insurance_company_short_name",
        "effective_date",
        "status",
        "preserve_type",
        "profession_type",
        "sku_price",
        "premium",
        "add_premium",
        "del_premium",
        "sku_coverage",
        "sku_ratio",
        "sku_charge_type",
        "dw_create_time"
      )

    /**
      * 读取方案类别表
      */
    val odsWorkGradeDimension: DataFrame = sqlContext.sql("select policy_code as policy_code_temp,profession_type as profession_type_salve from odsdb.ods_work_grade_dimension")

    /**
      * 读取方案数据
      */
    val odsPolicyProductPlanDetail =
      sqlContext.sql("select policy_code as policy_code_slave,sku_ratio as sku_ratio_slave,sku_coverage as sku_coverage_slave,sku_price as sku_price_slave,sku_charge_type as sku_charge_type_slave from odsdb.ods_policy_product_plan_detail")

    /**
      * 得到方案类别数据
      */
    val preserveProfessionData = policyData.join(odsWorkGradeDimension,'policy_code==='policy_code_temp,"leftouter")
      .selectExpr(
        "id",
        "policy_code",
        "add_person_count",
        "del_person_count",
        "insurance_company_short_name",
        "effective_date",
        "status",
        "preserve_type",
        "case when profession_type_salve is not null then profession_type_salve else profession_type end as profession_type",
        "sku_price",
        "premium",
        "add_premium",
        "del_premium",
        "sku_coverage",
        "sku_ratio",
        "sku_charge_type",
        "dw_create_time"
      )

    val preserveProfessionPlanData = preserveProfessionData.join(odsPolicyProductPlanDetail,'policy_code==='policy_code_slave,"leftouter")
      .selectExpr(
        "id",
        "policy_code",
        "add_person_count",
        "del_person_count",
        "insurance_company_short_name",
        "effective_date",
        "status",
        "preserve_type",
        "profession_type",
        "case when policy_code_slave is not null then sku_price_slave else sku_price end as sku_price",
        "premium",
        "add_premium",
        "del_premium",
        "case when policy_code_slave is not null then sku_coverage_slave else sku_coverage end sku_coverage",
        "case when policy_code_slave is not null then sku_ratio_slave else sku_ratio end as sku_ratio",
        "case when policy_code_slave is not null then sku_charge_type_slave else sku_charge_type end as sku_charge_type",
        "dw_create_time"
      )

    val res = preserveProfessionPlanData
    res
  }
}
