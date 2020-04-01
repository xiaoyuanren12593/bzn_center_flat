package bzn.ods.accounts

import java.text.SimpleDateFormat
import java.util.Date

import bzn.job.common.{MysqlUntil, Until}
import bzn.ods.util.SparkUtil
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/*
* @Author:liuxiang
* @Date：2019/12/11
* @Describe:
*/ object OdsEmpTAccountsIntermediateDetailToHive extends SparkUtil with Until with MysqlUntil {
  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "")

    val sc = sparkConf._2
    val sqlContext = sparkConf._3
    val hqlContext: HiveContext = sparkConf._4
    val res = readMysqlTable(hqlContext)
    res.printSchema()
    hqlContext.sql("truncate table odsdb.ods_t_accounts_employer_intermediate")

    res.write.mode(SaveMode.Append).saveAsTable("odsdb.ods_t_accounts_employer_intermediate")

    sc.stop()
  }

  def readMysqlTable(hiveContext: HiveContext): DataFrame = {
    hiveContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")//设置日期格式
      val date = df.format(new Date())// new Date()为获取当前系统时间
      date + ""
    })
    hiveContext.udf.register("clean", (str: String) => clean(str))
    val tablename = "ods_t_accounts_employer_detail"
    val username = "mysql.username.106"
    val password = "mysql.password.106"
    val driver = "mysql.driver"
    val url = "mysql.url.106.odsdb"

    val res = readMysqlTable(hiveContext, tablename, username, password, driver, url)
      .selectExpr(
        "id",
        "clean(batch_no) as batch_no",
        "clean(policy_no) as policy_no",
        "clean(preserve_id) as preserve_id",
        "clean(add_batch_code) as add_batch_code",
        "clean(del_batch_code) as del_batch_code",
        "clean(preserve_status) as preserve_status",
        "clean(data_source) as data_source",
        "clean(project_name) as project_name",
        "clean(product_code) as product_code",
        "clean(product_name) as product_name",
        "clean(channel_name) as channel_name",
        "clean(business_owner)  as business_owner",
        "clean(business_region) as business_region",
        "clean(business_source) as business_source",
        "clean(business_type) as business_type",
        "clean(policy_source_code) as policy_source_code",
        "clean(policy_source_name) as policy_source_name",
        "order_date",
        "performance_accounting_day",
        "clean(operational_name) as operational_name",
        "clean(holder_name) as holder_name",
        "clean(insurer_name) as insurer_name",
        "plan_price",
        "plan_coverage",
        "clean(plan_append) as plan_append",
        "plan_disability_rate",
        "clean(plan_pay_type) as plan_pay_type",
        "clean(underwriting_company) as underwriting_company",
        "policy_effect_date",
        "policy_start_time",
        "policy_effective_time",
        "policy_expire_time",
        "clean(cur_policy_status) as cur_policy_status",
        "clean(policy_status) as policy_status",
        "premium_total",
        "clean(premium_pay_status) as premium_pay_status",
        "clean(has_behalf) as has_behalf",
        "clean(behalf_status) as behalf_status",
        "clean(premium_invoice_type) as premium_invoice_type",
        "clean(economy_company) as economy_company",
        "economy_rates",
        "economy_fee",
        "technical_service_rates",
        "technical_service_fee",
        "consulting_service_rates",
        "consulting_service_fee",
        "service_fee_check_time",
        "clean(service_fee_check_status) as service_fee_check_status",
        "clean(has_brokerage) as has_brokerage",
        "brokerage_ratio",
        "brokerage_fee",
        "clean(brokerage_pay_status) as brokerage_pay_status",
        "clean(remake) as remake",
        "create_time",
        "update_time",
        "operator",
        "getNow() as dw_create_time")

    res

  }




}
