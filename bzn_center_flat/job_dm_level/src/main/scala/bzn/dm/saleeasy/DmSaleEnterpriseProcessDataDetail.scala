package bzn.dm.saleeasy

import bzn.dm.util.SparkUtil
import bzn.job.common.DataBaseUtil
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * author:xiaoYuanRen
  * Date:2020/3/25
  * Time:16:21
  * describe: 销售e 雇主周报销售过程数据
  **/
object DmSaleEnterpriseProcessDataDetail extends SparkUtil with DataBaseUtil {
  def main (args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "")

    val sc = sparkConf._2
    val hqlContext = sparkConf._4
    getSaleEnterpriseProcessData(hqlContext)
    sc.stop()
  }

  /**
    * 获取数据
    * @param sqlContext 上下文
    */
  def getSaleEnterpriseProcessData(sqlContext:HiveContext) = {

    import sqlContext.implicits._

    /**
      * 雇主销售对应的人数和保费数据
      */
    val empBaseData = sqlContext.sql(
      """
        |select a.sale_name,a.team_name,a.group_name,sum(case when b.preserve_id is null then 1 else 0 end) as underwrite_policy_count,
        |sum(case when b.preserve_id is not null then 1 else 0 end) as underwrite_preserve_count,sum(b.add_premium) as add_premium,sum(b.del_premium) as del_premium,sum(b.add_person_count) as add_person_count,
        |sum(b.del_person_count) as del_person_count,sum(b.sum_person) as sum_person,sum(b.sum_premium) as sum_premium,b.day_id from dwdb.dw_employer_baseinfo_detail a
        |join dwdb.dw_policy_premium_detail b
        |on a.policy_id = b.policy_id
        |where a.consumer_new_old = 'new'
        |group by a.sale_name,a.team_name,b.day_id,a.group_name
      """.stripMargin)

    val odsEntSalesTeamDimension = sqlContext.sql(
      """
        |select distinct sale_name as sale_name_slave,group_name as group_name_slave from odsdb.ods_ent_sales_team_dimension
      """.stripMargin)

    val driver = "mysql.driver"
    val user106 = "mysql.username.106"
    val pass106 = "mysql.password.106"
    val url106 = "mysql.url.106.dmdb"
    val user103 = "mysql.username.103"
    val pass103 = "mysql.password.103"
    val url103 = "mysql_url.103.dmdb"
    val tableName = "dm_saleseasy_employer_daily"


    readMysqlTable(sqlContext: SQLContext, tableName: String,user106:String,pass106:String,driver:String,url106:String)
      .selectExpr("sales_name","contacts_num","visitors_num","intended_num","intended_trace_num","strong_intended_num","sales_team",
        "regexp_replace(to_date(daily_time),'-','') as daily_time")
      .registerTempTable("dm_saleseasy_employer_daily_temp")

    val dmSaleseasyEmployerDailyData = sqlContext.sql(
      """
        |select sales_name,sales_team,daily_time,sum(contacts_num) as contacts_num,sum(intended_num) as intended_num,sum(intended_trace_num) as intended_trace_num,
        |sum(strong_intended_num) as strong_intended_num,sum(visitors_num) as visitors_num
        |from dm_saleseasy_employer_daily_temp
        |group by sales_name,daily_time,sales_team
      """.stripMargin)

    val resOne = empBaseData.join(dmSaleseasyEmployerDailyData,'sale_name==='sales_name and 'day_id==='daily_time,"fullouter")
      .selectExpr(
        "if(sale_name is null,sales_name,sale_name) as sale_name",
        "group_name",
        "if(team_name is null,sales_team,team_name) as team_name",
        "case when underwrite_policy_count is null then 0 else underwrite_policy_count end as underwrite_policy_count",
        "case when underwrite_preserve_count is null then 0 else underwrite_preserve_count end as underwrite_preserve_count",
        "case when add_premium is null then 0 else add_premium end as add_premium",
        "case when del_premium is null then 0 else del_premium end as del_premium",
        "case when add_person_count is null then 0 else add_person_count end as add_person_count",
        "case when del_person_count is null then 0 else del_person_count end as del_person_count",
        "case when sum_person is null then 0 else sum_person end as sum_person",
        "case when sum_premium is null then 0 else sum_premium end as sum_premium",
        "if(day_id is null,daily_time,day_id) as day_id",
        "case when contacts_num  is null then 0 else contacts_num end as contacts_num",
        "case when visitors_num  is null then 0 else visitors_num end as visitors_num",
        "case when intended_num  is null then 0 else intended_num end as intended_num",
        "case when intended_trace_num  is null then 0 else intended_trace_num end as intended_trace_num",
        "case when strong_intended_num is null then 0 else strong_intended_num end as strong_intended_num",
        "0 as apply_link_count"
      )

    val res = resOne.join(odsEntSalesTeamDimension,'sale_name==='sale_name_slave,"leftouter")
      .selectExpr(
        "sale_name",
        "if(group_name is null,group_name_slave,group_name) as group_name",
        "team_name",
        "cast(underwrite_policy_count as int) as underwrite_policy_count",
        "cast (underwrite_preserve_count as int) as underwrite_preserve_count",
        "cast (add_premium as decimal(14,4)) as add_premium",
        "cast (del_premium as decimal(14,4)) as del_premium",
        "cast (add_person_count as int) as add_person_count",
        "cast (del_person_count as int) as del_person_count",
        "cast (sum_premium as decimal(14,4)) as sum_premium",
        "cast (sum_person as int) as sum_person",
        "day_id",
        "cast (contacts_num as int) as contacts_num",
        "cast (visitors_num as int) as visitors_num",
        "cast (intended_num as int) as intended_num",
        "cast (intended_trace_num as int) as intended_trace_num",
        "cast (strong_intended_num as int) as strong_intended_num",
        "case when apply_link_count = 0 then null else apply_link_count end as apply_link_count",
        "date_format(now(),'yyyy-MM-dd HH:mm:ss') as dw_create_time"
      )

    val tableTarget = "dm_saleseasy_ent_process_data_detail"
    if(res.count() > 0){
      saveASMysqlTable(res: DataFrame, tableTarget: String, SaveMode.Overwrite,user106:String,pass106:String,driver:String,url106:String)
//      saveASMysqlTable(res: DataFrame, tableTarget: String, SaveMode.Overwrite,user103:String,pass103:String,driver:String,url103:String)
    }
  }
}
