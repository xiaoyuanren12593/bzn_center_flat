package bzn.dm.aegis_test

import bzn.dm.util.SparkUtil
import bzn.job.common.{DataBaseUtil, Until}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * author:xiaoYuanRen
  * Date:2020/3/9
  * Time:15:49
  * describe: this is new class
  **/
object DmChangeWaistcoatInsuredAndBaseInfoDetail extends SparkUtil with DataBaseUtil with Until{
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    getChangeWaistcoatInsuredData(hiveContext)
    getChangeWaistcoatBaseInfoData(hiveContext)
    getOdsEntGuzhuSalesmanDetailData(hiveContext)

    sc.stop()
  }

  /**
    * 得到渠道以及被保人明细数据
    * @param sqlContext 上下文
    */
  def getChangeWaistcoatInsuredData(sqlContext:HiveContext): Unit = {
    sqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    sqlContext.udf.register("getMD5", (ent_name: String) => MD5(ent_name))

//    val urlCK = "clickhouse.url"
    val urlCKTest = "clickhouse.url.odsdb.test"
    val userCK = "clickhouse.username"
    val possWordCK = "clickhouse.password"
    val driverCK = "clickhouse.driver"

    val odsChangeWaistcoatChannelInsuredDetailTtable = "ods_change_waistcoat_channel_insured_detail"

    /**
      *  gsc的人员清单
      */
    val insuredData = sqlContext.sql(
      """
        |select getMD5(concat(insured_cert_no,channel_name)) as id,getMD5(insured_cert_no) as insured_cert_no_md5,channel_name,current_date() as day_id,date_format(now(),'yyyy-MM-dd HH:mm:ss') as dw_create_time
        |from dwdb.dw_employer_baseinfo_person_detail
        |where insured_cert_no is not null and channel_name is not null
        |group by channel_name,insured_cert_no
      """.stripMargin)

    if(insuredData.count() > 0){
      writeClickHouseTable(insuredData:DataFrame,odsChangeWaistcoatChannelInsuredDetailTtable: String,
        SaveMode.Overwrite,urlCKTest:String,userCK:String,possWordCK:String,driverCK:String)
    }
  }

  def getChangeWaistcoatBaseInfoData(sqlContext:HiveContext): Unit = {
    import sqlContext.implicits._
    sqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    sqlContext.udf.register("getMD5", (ent_name: String) => MD5(ent_name))
    import org.apache.spark.sql.functions.monotonically_increasing_id

//    val user103 = "mysql.username.103"
//    val pass103 = "mysql.password.103"
//    val url103 = "mysql_url.103.dmdb"
    val driver = "mysql.driver"
    val user106 = "mysql.username.106"
    val pass106 = "mysql.password.106"
    val url106 = "mysql.url.106.dmdb"
    //黑名单表
    val blackListTableName = "dm_blacklist_ent"
    //灰名单
    val grayListTableName = "dm_graylist_ent"

    val blackListData = readMysqlTable(sqlContext: SQLContext, blackListTableName: String,user106:String,pass106:String,driver:String,url106:String)
      .where("is_effect = 1")
    val grayListData = readMysqlTable(sqlContext: SQLContext, grayListTableName: String,user106:String,pass106:String,driver:String,url106:String)
      .where("is_open = 1")

    /**
      * 客户，销售，运营，重复率
      */
    val repetitionRateData = sqlContext.sql(
      """
        |select c.channel_name as channel_name_master,c.biz_operator,c.sale_name,
        |sum(case when m.insured_cert_no_a = m.insured_cert_no_b then 1 else 0 end)/c.channel_person_count as repetition_person_rate
        |from (
        |    select a.channel_name,a.insured_cert_no as insured_cert_no_a,b.insured_cert_no as insured_cert_no_b
        |    from
        |    (
        |        select distinct insured_cert_no,channel_name
        |        from dwdb.dw_employer_baseinfo_person_detail
        |        where insured_cert_no is not null and channel_name is not null
        |    ) a
        |    left join
        |    (
        |        select insured_cert_no,channel_name
        |        from dwdb.dw_employer_baseinfo_person_detail
        |        where insured_cert_no is not null and channel_name is not null
        |        GROUP BY insured_cert_no,channel_name
        |    ) b
        |    on a.insured_cert_no = b.insured_cert_no
        |    where a.channel_name <> b.channel_name
        |    GROUP BY a.channel_name,a.insured_cert_no,b.insured_cert_no
        |) m
        |right join
        |(
        |    select channel_name,biz_operator,sale_name,count(DISTINCT insured_cert_no) as channel_person_count
        |    from dwdb.dw_employer_baseinfo_person_detail
        |    where insured_cert_no is not null and channel_name is not null
        |    GROUP BY channel_name,biz_operator,sale_name
        |) c
        |on m.channel_name = c.channel_name
        |GROUP BY m.channel_name,c.channel_person_count,c.biz_operator,c.sale_name,c.channel_name
      """.stripMargin)

    val EmpRiskMonitorKriTableName = "emp_risk_monitor_kri_detail"
    val urlCK = "clickhouse.url"
    val urlCKTest = "clickhouse.url.odsdb.test"
    val userCK = "clickhouse.username"
    val possWordCK = "clickhouse.password"
    val driverCK = "clickhouse.driver"

    /**
      * 读取神盾监控kri关键指标表
      */
    readClickHouseTable(sqlContext,EmpRiskMonitorKriTableName: String,urlCK:String,userCK:String,possWordCK:String)
      .registerTempTable("emp_risk_monitor_kri_detail_Temp")

    /**
      * 基础数据渠道、在保人数、赔付率、出险率
      */
    val baseInfoData = sqlContext.sql(
      """
        |select channel_name,sum(curr_insured) as curr_insured,
        |case when sum(acc_charge_premium) = 0 then 0 else sum(acc_prepare_claim_premium)/sum(acc_charge_premium) end as prepare_claim_rate,
        |sum(acc_case_num)*365/sum(acc_curr_insured) as risk_rate
        |from emp_risk_monitor_kri_detail_temp
        |where day_id = to_date(now())
        |GROUP BY channel_name
      """.stripMargin)

    /**
      * 黑名单标签
      */
    val blackBaseInfoData = repetitionRateData.join(blackListData,'channel_name_master==='ent_name,"leftouter")
      .selectExpr("channel_name_master","repetition_person_rate","sale_name","biz_operator","case when ent_name is not null then 1 else null end as is_black_gray_list")

    /**
      * 灰名单标签
      */
    val grayAndBlackAndBaseInfoData = blackBaseInfoData.join(grayListData,'channel_name_master==='ent_name,"leftouter")
      .selectExpr("channel_name_master","repetition_person_rate","sale_name","biz_operator","case when is_black_gray_list is null and ent_name is not null then 2 else 0 end as is_black_gray_list")

    val odsChangeWaistcoatChannelBaseInfo = grayAndBlackAndBaseInfoData.join (baseInfoData,'channel_name_master==='channel_name,"leftouter")
      .selectExpr("channel_name_master as channel_name","cast(curr_insured as int) as curr_insured","cast(prepare_claim_rate as decimal(14,4)) as prepare_claim_rate",
        "cast(risk_rate as decimal(14,4)) as risk_rate","is_black_gray_list","sale_name","biz_operator",
        "cast(repetition_person_rate as decimal(14,10)) as channel_repetition_rate",
        "to_date(now()) as day_id",
        "date_format(now(),'yyyy-MM-dd HH:mm:ss') as dw_create_time"
      ).withColumn("id",monotonically_increasing_id+1)
      .where("channel_name is not null")

    val odsChangeWaistcoaChannelBaseInfoDetailTable = "ods_change_waistcoat_channel_base_info_detail"
    if(odsChangeWaistcoatChannelBaseInfo.count() > 0){
      writeClickHouseTable(odsChangeWaistcoatChannelBaseInfo:DataFrame,odsChangeWaistcoaChannelBaseInfoDetailTable: String,
        SaveMode.Overwrite,urlCKTest:String,userCK:String,possWordCK:String,driverCK:String)
    }
  }

  /**
    * 渠道销售表
    * @param sqlContext  上下文
    */
  def getOdsEntGuzhuSalesmanDetailData(sqlContext:HiveContext) = {

    val res = sqlContext.sql(
      """
        |select *,to_date(now()) as day_id from odsdb.ods_ent_guzhu_salesman_detail
        |where channel_name is not null
      """.stripMargin)

    val empChannelSalemanTableName = "ods_ent_guzhu_salesman_detail"
//    val urlCK = "clickhouse.url"
    val urlCKTest = "clickhouse.url.odsdb.test"
    val userCK = "clickhouse.username"
    val possWordCK = "clickhouse.password"
    val driverCK = "clickhouse.driver"

    if(res.count() > 0){
      writeClickHouseTable(res:DataFrame,empChannelSalemanTableName: String,
        SaveMode.Overwrite,urlCKTest:String,userCK:String,possWordCK:String,driverCK:String)
    }
  }
}
