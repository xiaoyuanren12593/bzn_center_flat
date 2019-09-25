package bzn.dm.bclickthrough

import java.sql.{Connection, DriverManager}
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import bzn.dm.util.SparkUtil
import bzn.job.common.Until
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * author:xiaoYuanRen
  * Date:2019/9/24
  * Time:16:52
  * describe: 续投结果数据
  **/
object DmEmployerProposalContinueDetail extends SparkUtil with Until {
  def main (args: Array[String]): Unit = {
    System.setProperty ("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo (appName, "")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    val res = continueProposalDetail(hiveContext)
      .selectExpr(
        "id",
        "policy_id",
        "policy_code",
        "policy_start_date",
        "policy_end_date",
        "insure_company_name",//保险公司
        "product_code",
        "product_name",
        "continue_policy_id",
        "preserve_policy_no",
        "sku_coverage",
        "sku_charge_type",
        "sku_price",
        "now_date",
        "curr_insured_count as current_insured",
        "should_continue_policy_date  as continue_date",
        "realy_continue_policy_date",
        "realy_insured_count  as persons_continued",
        "should_continue_policy_date_is",
        "should_insured_count as persons_expired",
        "effect_month as month",
        "month as continue_month",
        "ent_id",
        "ent_name",
        "salesman",
        "team_name",
        "biz_operator",
        "consumer_category",
        "channel_id",
        "channel_name",
        "dw_create_time"
      )

    val mysqlRes = res.selectExpr(
      "id",
      "policy_code",
      "insure_company_name",//保险公司
      "ent_id",
      "ent_name",
      "channel_id",
      "channel_name",
      "salesman",
      "biz_operator",
      "sku_coverage",
      "sku_price",
      "sku_charge_type",
      "current_insured",
      "policy_start_date",
      "policy_end_date",
      "continue_date",
      "continue_month",
      "persons_expired",
      "persons_continued",
      "month",
      "dw_create_time"
    ).cache()
    saveASMysqlTable(mysqlRes,"dm_employer_policy_continue_detail",SaveMode.Overwrite)

    sc.stop ()
  }

  /**
    * @param sqlContext
    */
  def continueProposalDetail(sqlContext:HiveContext) ={
    sqlContext.udf.register ("getUUID", () => (java.util.UUID.randomUUID () + "").replace ("-", ""))
    sqlContext.udf.register ("getNow", () => {
      val df = new SimpleDateFormat ("yyyy-MM-dd HH:mm:ss")
      //设置日期格式
      val date = df.format (new Date ()) // new Date()为获取当前系统时间
      date + ""
    })
    /**
      * 读取续投基础数据
      */
    val dwBClickthrouthEmpProposalContinueDetail = sqlContext.sql("select * from dwdb.dw_b_clickthrouth_emp_proposal_continue_Detail")
      .selectExpr(
        "policy_id",
        "policy_code",
        "policy_start_date",
        "policy_end_date",
        "insure_company_name",//保险公司
        "product_code",
        "product_name",
        "continue_policy_id",
        "preserve_policy_no",
        "sku_coverage",
        "sku_charge_type",
        "sku_price",
        "now_date as day_id",
        "should_continue_policy_date",
        "realy_continue_policy_date",
        "should_continue_policy_date_is",
        "effect_month",
        "month",
        "ent_id",
        "ent_name",
        "salesman",
        "team_name",
        "biz_operator",
        "consumer_category",
        "channel_id",
        "channel_name"
      )

    /**
      * 读取在保人表
      */
    val dwPolicyCurrInsuredDetail = sqlContext.sql("select policy_id,policy_code as policy_code_insured ,day_id,count" +
      " from dwdb.dw_policy_curr_insured_detail")
      .cache()

    /**
      * 统计当前在保人数
      */
    val nowInsuredCountRes = dwBClickthrouthEmpProposalContinueDetail.join(dwPolicyCurrInsuredDetail,Seq("policy_id","day_id"),"leftouter")
      .selectExpr(
        "policy_id",
        "policy_code",
        "policy_start_date",
        "policy_end_date",
        "insure_company_name",//保险公司
        "product_code",
        "product_name",
        "continue_policy_id",
        "preserve_policy_no",
        "sku_coverage",
        "sku_charge_type",
        "sku_price",
        "day_id as now_date",
        "case when count is null then 0 else count end as curr_insured_count",
        "should_continue_policy_date",
        "realy_continue_policy_date",
        "should_continue_policy_date_is as day_id",
        "effect_month",
        "month",
        "ent_id",
        "ent_name",
        "salesman",
        "team_name",
        "biz_operator",
        "consumer_category",
        "channel_id",
        "channel_name"
      )

    /**
      * 统计应续人数
      */
    val shouldInsuredCountRes = nowInsuredCountRes.join(dwPolicyCurrInsuredDetail,Seq("policy_id","day_id"),"leftouter")
      .selectExpr(
        "policy_id as policy_id_master",
        "policy_code",
        "policy_start_date",
        "policy_end_date",
        "insure_company_name",//保险公司
        "product_code",
        "product_name",
        "continue_policy_id  as policy_id",
        "preserve_policy_no",
        "sku_coverage",
        "sku_charge_type",
        "sku_price",
        "now_date",
        "curr_insured_count",
        "should_continue_policy_date",
        "realy_continue_policy_date as day_id",
        "day_id as should_continue_policy_date_is",
        "case when count is null then 0 else count end as should_insured_count",
        "effect_month",
        "month",
        "ent_id",
        "ent_name",
        "salesman",
        "team_name",
        "biz_operator",
        "consumer_category",
        "channel_id",
        "channel_name"
      )

    /**
      * 统计实续在保人数
      */
    val res = shouldInsuredCountRes.join(dwPolicyCurrInsuredDetail,Seq("policy_id","day_id"),"leftouter")
      .selectExpr(
        "getUUID() as id",
        "policy_id_master as policy_id",
        "policy_code",
        "policy_start_date",
        "policy_end_date",
        "insure_company_name",//保险公司
        "product_code",
        "product_name",
        "policy_id as continue_policy_id",
        "preserve_policy_no",
        "sku_coverage",
        "sku_charge_type",
        "sku_price",
        "now_date",
        "curr_insured_count",
        "should_continue_policy_date",
        "day_id as realy_continue_policy_date",
        "case when count is null then 0 else count end as realy_insured_count",
        "should_continue_policy_date_is",
        "should_insured_count",
        "effect_month",
        "month",
        "ent_id",
        "ent_name",
        "salesman",
        "team_name",
        "biz_operator",
        "consumer_category",
        "channel_id",
        "channel_name",
        "getNow() as dw_create_time"
      )
    res
  }

  /**
    * 将DataFrame保存为Mysql表
    *
    * @param dataFrame 需要保存的dataFrame
    * @param tableName 保存的mysql 表名
    * @param saveMode  保存的模式 ：Append、Overwrite、ErrorIfExists、Ignore
    */
  def saveASMysqlTable(dataFrame: DataFrame, tableName: String, saveMode: SaveMode) = {
    var table = tableName
    val properties: Properties = getProPerties()
    val prop = new Properties //配置文件中的key 与 spark 中的 key 不同 所以 创建prop 按照spark 的格式 进行配置数据库
    prop.setProperty("user", properties.getProperty("mysql.username.106"))
    prop.setProperty("password", properties.getProperty("mysql.password.106"))
    prop.setProperty("driver", properties.getProperty("mysql.driver"))
    prop.setProperty("url", properties.getProperty("mysql_url.106.dmdb"))
    if (saveMode == SaveMode.Overwrite) {
      var conn: Connection = null
      try {
        conn = DriverManager.getConnection(
          prop.getProperty("url"),
          prop.getProperty("user"),
          prop.getProperty("password")
        )
        val stmt = conn.createStatement
        table = table.toLowerCase
        stmt.execute(s"truncate table $table") //为了不删除表结构，先truncate 再Append
        conn.close()
      }
      catch {
        case e: Exception =>
          println("MySQL Error:")
          e.printStackTrace()
      }
    }
    dataFrame.write.mode(SaveMode.Append).jdbc(prop.getProperty("url"), table, prop)
  }

  /**
    * 获取配置文件
    * @return
    */
  def getProPerties() = {
    val lines_source = Source.fromURL(getClass.getResource("/config_scala.properties")).getLines.toSeq
    var properties: Properties = new Properties()
    for (elem <- lines_source) {
      val split = elem.split("==")
      val key = split(0)
      val value = split(1)
      properties.setProperty(key,value)
    }
    properties
  }
}