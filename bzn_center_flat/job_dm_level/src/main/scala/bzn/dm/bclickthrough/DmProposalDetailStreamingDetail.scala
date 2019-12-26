package bzn.dm.bclickthrough

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import bzn.dm.util.SparkUtil
import bzn.job.common.{DataBaseUtil, Until}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * author:xiaoYuanRen
  * Date:2019/12/25
  * Time:17:17
  * describe: this is new class
  **/
object DmProposalDetailStreamingDetail extends SparkUtil with Until with DataBaseUtil{
  case class DmbBatchingMonitoringDetail(id: String,source:String,project_name:String,warehouse_level:String,house_name:String,table_name:String,
                                         status:Int,remark:String,create_time:Timestamp,update_time:Timestamp)

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName,"")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    getProposalData(hiveContext)

    sc.stop()
  }

  def getProposalData(sqlContext:HiveContext) = {
    import sqlContext.implicits._
    sqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    sqlContext.udf.register("clean", (str: String) => clean(str))
    sqlContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")//设置日期格式
      val date = df.format(new Date())// new Date()为获取当前系统时间
      date + ""
    })

    /**
      * 读取投保单与批单的数据
      */
    val dwProposalOperatorDailyDetail = sqlContext.sql("select *,regexp_replace(subString(getNow(),1,10),'-','') as day_id from dwdb.dw_proposal_operator_daily_detail")
      .selectExpr(
        "policy_code",
        "add_person_count",
        "del_person_count",
        "insurance_company_short_name",
        "preserve_type",
        "profession_type",
        "case when sku_charge_type = 2 then sku_price/12 else sku_price end as sku_price",
        "premium",
        "sku_coverage",
        "sku_ratio",
        "case when sku_charge_type = 1 then '月单' when sku_charge_type = 2 then '年单' else null end as sku_charge_type",
        "day_id",
        "date_format(now(),'yyyy-MM-dd HH:dd:ss') as create_time",
        "date_format(now(),'yyyy-MM-dd HH:dd:ss') as update_time"
      )


    /**
      * 得到雇主的每日在保人数和上月同期在保人数
      */
    val empCurrInsuredAndLastMonthData  = sqlContext.sql(
      """
        |select m.insure_company_short_name as insurance_company_short_name,m.day_id,m.last_month_homochronous_time,m.curr_insured,n.last_month_homochronous_curr_insured,
        | date_format(now(),'yyyy-MM-dd HH:dd:ss') as create_time,date_format(now(),'yyyy-MM-dd HH:dd:ss') as update_time from (
        |select t.insure_company_short_name,t.day_id,t.last_month_homochronous_time,sum(curr_insured) as curr_insured
        |from
        |(
        |    select a.policy_code,a.product_code,a.insure_company_short_name,b.day_id,b.last_month_homochronous_time,if(b.count is null,0,b.count) as curr_insured
        |    from dwdb.dw_employer_baseinfo_detail  a
        |    left join
        |    (
        |        -- 得到每个day_id对应的上月同期时间
        |        select x.policy_id,x.policy_code,x.count,regexp_replace(substring(y.c_lm_day_id,1,10),'-','') as last_month_homochronous_time,x.day_id from dwdb.dw_policy_curr_insured_detail x
        |        left join odsdb.ods_t_day_dimension y
        |        on x.day_id = y.c_day_id
        |    ) b
        |    on a.policy_code = b.policy_code
        |    where a.product_code not in ('17000001','LGB000001')
        |) t
        |group by t.insure_company_short_name ,t.day_id,t.last_month_homochronous_time
        |) m
        |join
        |(
        |select t.insure_company_short_name,t.day_id,sum(curr_insured) as last_month_homochronous_curr_insured
        |from
        |(
        |    select a.policy_code,a.product_code,a.insure_company_short_name,b.day_id,if(b.count is null,0,b.count) as curr_insured
        |    from dwdb.dw_employer_baseinfo_detail  a
        |    left join
        |    (
        |        -- 得到每个day_id对应的上月同期时间
        |        select x.policy_id,x.policy_code,x.count,x.day_id
        |        from dwdb.dw_policy_curr_insured_detail x
        |    ) b
        |    on a.policy_code = b.policy_code
        |    where a.product_code not in ('17000001','LGB000001')
        |) t
        |group by t.insure_company_short_name ,t.day_id
        |) n
        |on m.insure_company_short_name = n.insure_company_short_name and m.last_month_homochronous_time = n.day_id
      """.stripMargin)

    val updateColumns: Array[String] = Array("status","remark","update_time")
    val urlFormatOfficial = "mysql.url.106.dmdb"
    val userFormatOfficial = "mysql.username.106"
    val possWordFormatOfficial = "mysql.password.106"

    val urlFormat = "mysql.url.103.dmdb"
    val userFormat = "mysql.username.103"
    val possWordFormat = "mysql.password.103"
    val driverFormat = "mysql.driver"
    val tableMysqlName = "dm_batching_monitoring_detail"

    val nowTime = getNowTime().substring(0,10)
    val tableNameOneRes = "dm_saleeasy_operation_daily_policy_detail"
    val tableNameTwoRes = "dm_saleeasy_operation_daily_pro_and_pre_detail"

    /**
      * 运营日报-在保人详情页 每天18点更新
      */
    saveASMysqlTable(empCurrInsuredAndLastMonthData: DataFrame, tableNameOneRes: String, SaveMode.Overwrite,userFormat:String,possWordFormat:String,driverFormat:String,urlFormat:String)
//    saveASMysqlTable(empCurrInsuredAndLastMonthData: DataFrame, tableNameOneRes: String, SaveMode.Overwrite,userFormatOfficial:String,possWordFormatOfficial:String,driverFormat:String,urlFormatOfficial:String)

    val oneDataTest = readMysqlTable(sqlContext: SQLContext, tableNameOneRes: String,userFormat:String,possWordFormat:String,driverFormat:String,urlFormat:String).count()
//    val oneDataOfficial = readMysqlTable(sqlContext: SQLContext, tableNameOneRes: String,userFormatOfficial:String,possWordFormatOfficial:String,driverFormat:String,urlFormatOfficial:String).count()

    if(oneDataTest > 0){
      val dataMonitor =
        Seq(
          DmbBatchingMonitoringDetail(nowTime+"`dm_saleeasy_operation_daily_policy_detail`"+"mysql","mysql","运营日报-在保人详情页","dm","dmdb","dm_saleeasy_operation_daily_policy_detail",1,"运营日报-在保人详情页-成功",new Timestamp(System.currentTimeMillis()),new Timestamp(System.currentTimeMillis()))
        ).toDF()

      insertOrUpdateDFtoDBUsePoolNew(tableMysqlName: String, dataMonitor: DataFrame, updateColumns: Array[String],
        urlFormat:String,userFormat:String,possWordFormat:String,driverFormat:String)

      insertOrUpdateDFtoDBUsePoolNew(tableMysqlName: String, dataMonitor: DataFrame, updateColumns: Array[String],
        urlFormatOfficial:String,userFormatOfficial:String,possWordFormatOfficial:String,driverFormat:String)
    }

    /**
      * 运营日报-新投保单和批单详情数据 每天18点更新
      */
    saveASMysqlTable(dwProposalOperatorDailyDetail: DataFrame, tableNameTwoRes: String, SaveMode.Overwrite,userFormat:String,possWordFormat:String,driverFormat:String,urlFormat:String)
//    saveASMysqlTable(empCurrInsuredAndLastMonthData: DataFrame, tableNameTwoRes: String, SaveMode.Overwrite,userFormatOfficial:String,possWordFormatOfficial:String,driverFormat:String,urlFormatOfficial:String)

    val twoDataTest = readMysqlTable(sqlContext: SQLContext, tableNameTwoRes: String,userFormat:String,possWordFormat:String,driverFormat:String,urlFormat:String).count()
//    val twoDataOfficial = readMysqlTable(sqlContext: SQLContext, tableNameTwoRes: String,userFormatOfficial:String,possWordFormatOfficial:String,driverFormat:String,urlFormatOfficial:String).count()

    if(twoDataTest > 0){
      val dataMonitor =
        Seq(
          DmbBatchingMonitoringDetail(nowTime+"`dm_saleeasy_operation_daily_pro_and_pre_detail`"+"mysql","mysql","运营日报-新投保单和批单详情数据","dm","dmdb","dm_saleeasy_operation_daily_pro_and_pre_detail",1,"运营日报-新投保单和批单详情数据-成功",new Timestamp(System.currentTimeMillis()),new Timestamp(System.currentTimeMillis()))
        ).toDF()

      insertOrUpdateDFtoDBUsePoolNew(tableMysqlName: String, dataMonitor: DataFrame, updateColumns: Array[String],
        urlFormat:String,userFormat:String,possWordFormat:String,driverFormat:String)

      insertOrUpdateDFtoDBUsePoolNew(tableMysqlName: String, dataMonitor: DataFrame, updateColumns: Array[String],
        urlFormatOfficial:String,userFormatOfficial:String,possWordFormatOfficial:String,driverFormat:String)
    }

  }
}
