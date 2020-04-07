package bzn.dm.saleeasy

import bzn.dm.util.SparkUtil
import bzn.job.common.{DataBaseUtil, Until}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * author:xiaoYuanRen
  * Date:2020/3/24
  * Time:15:47
  * describe: 销售e 业绩核算统计
  **/
object DmSaleseasyPerformanceStatisticalDetailTest extends SparkUtil with DataBaseUtil with Until{
  def main (args: Array[String]): Unit = {
    System.setProperty ("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo (appName, "local[*]")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4

    val res = getPerformanceStatisticalData (hiveContext)
    res.printSchema()
    val tableName = "dm_saleseasy_performance_statistical_detail"
    val user103 = "mysql.username.103"
    val pass103 = "mysql.password.103"
    val url103 = "mysql_url.103.dmdb"
    val driver = "mysql.driver"
    val user106 = "mysql.username.106"
    val pass106 = "mysql.password.106"
    val url106 = "mysql.url.106.dmdb"
    if(res.count() > 0){
//      saveASMysqlTable(res.limit(5000): DataFrame, tableName: String, SaveMode.Overwrite,user103:String,pass103:String,driver:String,url103:String)
//      saveASMysqlTable(res: DataFrame, tableName: String, SaveMode.Overwrite,user106:String,pass106:String,driver:String,url106:String)
    }
    sc.stop ()
  }

  /**
    * 业绩核算数据统计
    * @param sqlContext 上下文
    */
  def getPerformanceStatisticalData(sqlContext:HiveContext): DataFrame = {
    sqlContext.udf.register("clean", (str: String) => clean(str))
    sqlContext.udf.register("changeColumnData", (str: String) => changeColumnData(str))
    sqlContext.udf.register("randowPremium", (cln: java.math.BigDecimal) => randomPremium(cln))
    sqlContext.udf.register("randowPersonCount", (cln: Long) => randomPersonCount(cln))
    val res = sqlContext.sql(
      """
        |select  case when x.channel_name is null then '未知' else x.channel_name end as channel_name,
        |x.performance_accounting_day,cast(x.premium_total as decimal(14,4)) as premium_total,x.product_category,if(x.salesman is null,'公司',x.salesman) as salesman,
        |'boss' as top_level,y.department as business_line,y.team_name_ps as sales_team,if(length(y.group_name) = 0,x.salesman,y.group_name) as sales_group
        |,
        |date_format(now(),'yyyy-MM-dd HH:mm:ss') as create_time,
        |date_format(now(),'yyyy-MM-dd HH:mm:ss') as update_time
        |from
        |(
        |    select if(a.channel_name is null , a.holder_name,a.channel_name) as channel_name,
        |    to_date(a.performance_accounting_day) as performance_accounting_day,
        |    cast(sum(a.premium_total) as decimal(14,4)) as premium_total,
        |    a.business_line as product_category,
        |    (case when a.business_line <> '雇主' then a.sale_name when a.business_line = '雇主' and b.consumer_new_old = 'new' then a.sale_name else b.biz_operator end) as salesman
        |    from dwdb.dw_accounts_and_tmt_detail a
        |    left join odsdb.ods_ent_guzhu_salesman_detail b
        |    on a.holder_name = b.ent_name and a.business_line = '雇主'
        |    where a.source != 'inter' and to_date(a.performance_accounting_day) >= to_date('2020-01-01')
        |    group by if(a.channel_name is null , a.holder_name,a.channel_name),to_date(a.performance_accounting_day),
        |    a.business_line,
        |    (case when a.business_line <> '雇主' then a.sale_name when a.business_line = '雇主' and b.consumer_new_old = 'new' then a.sale_name else b.biz_operator end)
        |) x
        |left join
        |(
        |    select sale_name,team_name_ps,department,group_name from odsdb.ods_ent_sales_team_dimension
        |) y
        |on if(x.salesman is null,'公司',x.salesman) = y.sale_name
      """.stripMargin)
      .selectExpr(
        "channel_name",
        "performance_accounting_day",
        "premium_total",
        "product_category",
        "salesman",
        "top_level",
        "business_line",
        "sales_team",
        "sales_group",
        "create_time",
        "update_time"
      )
    res
  }
}
