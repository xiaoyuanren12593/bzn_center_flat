package bzn.dw.premium

import java.text.SimpleDateFormat
import java.util.Date

import bzn.dw.util.SparkUtil
import bzn.job.common.{DataBaseUtil, MysqlUntil, Until}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

object DwEmployerCurrPremiumDetail extends SparkUtil with Until with DataBaseUtil {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "")

    val sc = sparkConf._2
    val hqlContext = sparkConf._4
    val sqlContext = sparkConf._3

    val res = EveryDayPremiumAndCounts(hqlContext)
    saveASMysqlTable(res,"dm_employer_curr_premium_detail",SaveMode.Append,
      "mysql.username.106","mysql.password.106","mysql.driver","mysql.url.106.dmdb")

    sc.stop()

  }


  def EveryDayPremiumAndCounts(hiveContext: HiveContext): DataFrame = {
    hiveContext.udf.register("clean", (str: String) => clean(str))
    hiveContext.udf.register ("getNow", () => {
      val df = new SimpleDateFormat ("yyyy-MM-dd HH:mm:ss")
      //设置日期格式
      val date = df.format (new Date ()) // new Date()为获取当前系统时间
      date + ""
    })

    //读取雇主基础信息表
    val dwEmployerBaseInfo_salve_one = hiveContext.sql("select insure_company_short_name,sku_charge_type,consumer_new_old,policy_code,two_level_pdt_cate from dwdb.dw_employer_baseinfo_detail")

    val dwEmployerBaseInfo = dwEmployerBaseInfo_salve_one.selectExpr("insure_company_short_name",
      "case when sku_charge_type is null then '1' else sku_charge_type end as sku_charge_type",
      "case when consumer_new_old is null then 'old' else consumer_new_old end as consumer_new_old",
      "policy_code as policy_code_salve",
      "two_level_pdt_cate")

    //读取在保人表
    val dwPolicyCurrInsuredDetail = hiveContext.sql("select policy_code,sum(count) as counts from dwdb.dw_policy_curr_insured_detail " + "WHERE day_id = regexp_replace(substring(cast(now() as STRING),1,10),'-','') " +
      "group by policy_code")

    //基础表关联在保人表
    val empInsured = dwEmployerBaseInfo.join(dwPolicyCurrInsuredDetail, dwEmployerBaseInfo("policy_code_salve") === dwPolicyCurrInsuredDetail("policy_code"), "leftouter")
      .selectExpr("insure_company_short_name",
        "sku_charge_type",
        "consumer_new_old",
        "policy_code_salve",
        "case when counts is null or two_level_pdt_cate ='零工保' then 0 else counts end as counts")

    //读取雇主电子台账
    val odsTaccounts = hiveContext.sql("select policy_no,sum(premium_total) as premium_total from odsdb.ods_t_accounts_employer_intermediate " +
      "where to_date(performance_accounting_day) <= to_date(now()) and substr(cast(to_date(performance_accounting_day) as string),1,7) = substr(cast(to_date(now()) as string),1,7) " +
      "GROUP BY policy_no")

    //将上述结果关联读取雇主电子台账
    val res_temp = empInsured.join(odsTaccounts, empInsured("policy_code_salve") === odsTaccounts("policy_no"), "leftouter")
      .selectExpr("insure_company_short_name",
        "consumer_new_old",
        "policy_code_salve",
        "case sku_charge_type when  '1' then '月单' when '2' then '年单' when '4' then '年单' end as sku_charge_type",
        "counts",
        "cast(case when premium_total is null then 0 else premium_total end as decimal(14,4)) as premium_total")

    res_temp.registerTempTable("TempTable")
    val res = hiveContext.sql("select insure_company_short_name,sku_charge_type,consumer_new_old,sum(counts) as counts,sum(premium_total) as premium_total,substring(cast(now() as STRING),1,10) as create_time from TempTable group by insure_company_short_name,consumer_new_old,sku_charge_type order by insure_company_short_name,create_time")

    //如果有当天的数 先删除掉
    val urlFormatOfficial = "mysql.url.106.dmdb"
    val userFormatOfficial = "mysql.username.106"
    val possWordFormatOfficial = "mysql.password.106"
    val driverFormat = "mysql.driver"
    val tableName = "dm_employer_curr_premium_detail"

    val now = getNowTime().substring(0,10)
    val sql = "delete from "+tableName+" where create_time  = '"+now+"'"
    exeSql(sql:String,urlFormatOfficial:String,userFormatOfficial:String,possWordFormatOfficial:String,driverFormat:String)


    /*res.printSchema()
    res.selectExpr("sum(premium_total)").show(1)
    res.selectExpr("sum(counts)").show(1)
    res.show(100)*/

    res

  }

}
