package bzn.dm.bclickthrough

import bzn.dm.bclickthrough.DmPolicyStreamingDetail.saveASMysqlTable
import bzn.dm.util.SparkUtil
import bzn.job.common.{MysqlUntil, Until}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext

/**
  * author:xiaoYuanRen
  * Date:2019/10/23
  * Time:16:51
  * describe: 每天新增的数据
  **/
object DmPolicyStreamingDetailTest extends SparkUtil with Until with MysqlUntil{
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName,"local[*]")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    val res = getHolderInfo(hiveContext)
   // hiveContext.sql("truncate table dwdb.dw_policy_streaming_detail")
   // res.repartition(1).write.mode(SaveMode.Append).saveAsTable("dwdb.dw_policy_streaming_detail")

    sc.stop()
  }

  def getHolderInfo(sqlContext:HiveContext) = {
    sqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    import sqlContext.implicits._
    /**
      * 读取每天新增的数据
      */
    val dwPolicyStreamingDetail =
      sqlContext.sql("select channel_id,channel_name,ent_id,ent_name,sale_name,biz_operator,0 as now_insured_count,insured_count as next_month_insured_count, " +
        "regexp_replace(substr(cast(now() as string),1,10),'-','') as now_day_id," +
        " regexp_replace(date_add(last_day(now()),1),'-','') as next_month_day_id from dwdb.dw_policy_streaming_detail")

    /**
      * 读取雇主基础数据
      */
    val dwEmployerBaseinfoDetail = sqlContext.sql("select policy_id,holder_name,ent_id,channel_id,channel_name,sale_name,biz_operator, " +
      "regexp_replace(substr(cast(now() as string),1,10),'-','') as now_day_id," +
      " regexp_replace(date_add(last_day(now()),1),'-','') as next_month_day_id  from dwdb.dw_employer_baseinfo_detail")

    /**
      * 读取当前在保人表
      */
    val dwPolicyCurrInsuredDetail = sqlContext.sql("select policy_id as policy_id_insued,day_id,count as insured_count from dwdb.dw_policy_curr_insured_detail")
      .where("regexp_replace(substr(cast(now() as string),1,10),'-','') = day_id or regexp_replace(date_add(last_day(now()),1),'-','') = day_id")

    val nowDataRes = dwEmployerBaseinfoDetail.join(dwPolicyCurrInsuredDetail,'policy_id === 'policy_id_insued and 'now_day_id === 'day_id,"leftouter")
      .selectExpr(
        "policy_id",
        "ent_id",
        "holder_name",
        "channel_id",
        "channel_name",
        "sale_name",
        "biz_operator",
        "insured_count as now_insured_count",
        "now_day_id",
        "next_month_day_id"
      )

    val nextMonthData = nowDataRes.join(dwPolicyCurrInsuredDetail,'policy_id === 'policy_id_insued and 'next_month_day_id === 'day_id,"leftouter")
      .selectExpr(
        "channel_id",
        "channel_name",
        "ent_id",
        "holder_name as ent_name",
        "sale_name",
        "biz_operator",
        "now_insured_count",
        "insured_count as next_month_insured_count",
        "now_day_id",
        "next_month_day_id"
      )

    val resTemp = dwPolicyStreamingDetail.unionAll(nextMonthData)
        .registerTempTable("resTemp")

    val res = sqlContext.sql("select channel_id,channel_name,ent_id,ent_name,sale_name,biz_operator," +
      "sum(case when now_insured_count is null then 0 else now_insured_count end) as now_insured_count," +
      "sum(case when next_month_insured_count is null then 0 else next_month_insured_count end) as next_month_insured_count" +
      " from resTemp group by channel_id,channel_name,ent_id,ent_name,sale_name,biz_operator")
      .selectExpr(
        "getUUID() as id",
        "channel_id",
        "channel_name",
        "ent_id",
        "ent_name",
        "sale_name",
        "biz_operator",
        "cast(now_insured_count as int) as curr_insured",
        "cast(next_month_insured_count as int) as pre_continue_person_count",
        "date_format(now(), 'yyyy-MM-dd HH:mm:ss') as create_time",
        "date_format(now(), 'yyyy-MM-dd HH:mm:ss') as update_time"
      )

    val tableName  = "dm_b_clickthrouth_emp_continue_policy_detail"

    //    val user103 = "mysql.username.103"
    //    val pass103 = "mysql.password.103"
    //    val url103 = "mysql_url.103.dmdb"
    val driver = "mysql.driver"
    val user106 = "mysql.username.106"
    val pass106 = "mysql.password.106"
    val url106 = "mysql_url.106.dmdb"

    // saveASMysqlTable(res: DataFrame, tableName, SaveMode.Overwrite,user103,pass103,driver,url103)
    saveASMysqlTable(res: DataFrame, tableName, SaveMode.Overwrite,user106,pass106,driver,url106)
    res.printSchema()

  }
}