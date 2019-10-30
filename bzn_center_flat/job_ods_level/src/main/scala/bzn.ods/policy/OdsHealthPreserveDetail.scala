package bzn.ods.policy

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import bzn.job.common.Until
import bzn.ods.util.SparkUtil
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * author:xiaoYuanRen
  * Date:2019/5/21
  * Time:9:47
  * describe: 1.0 系统保全表
  **/
object OdsHealthPreserveDetail extends SparkUtil with Until{

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName,"")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4

    val res = twoOdsPolicyDetail(hiveContext)
    hiveContext.sql("truncate table odsdb.ods_health_preserce_detail")
    res.repartition(10).write.mode(SaveMode.Append).saveAsTable("odsdb.ods_health_preserce_detail")
    sc.stop()

  }

  /**
    * 2.0系统保单明细表
    * @param sqlContext 上下文
    */
  def twoOdsPolicyDetail(sqlContext:HiveContext) :DataFrame = {
    sqlContext.udf.register("clean", (str: String) => clean(str))
    sqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    sqlContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")//设置日期格式
      val date = df.format(new Date())// new Date()为获取当前系统时间
      date + ""
    })
    sqlContext.udf.register("getNull", (line:String) =>  {
      if (line == "" || line == null || line == "NULL") 9 else line.toInt
    })


    /**
      * 读取健康保单表
      */
    val healthPolicyBznrobot = readMysqlTable(sqlContext,"health_policy_bznrobot")
      .selectExpr("policy_no","product_code","premium","create_time")

    /**
      * 读取健康投保单表
      */
    val tProposalBznrobot = readMysqlTable(sqlContext,"t_proposal_bznrobot")
      .selectExpr("proposal_no","policy_no as policy_no_slave","insurance_policy_no","holder_name","sell_channel_name")

    /**
      * 读取
      */
    val tProposalSubjectPersonMasterBznrobot = readMysqlTable(sqlContext,"t_proposal_subject_person_master_bznrobot")
      .selectExpr("proposal_no as proposal_no_slave","name")

    val resTemp = healthPolicyBznrobot.join(tProposalBznrobot,healthPolicyBznrobot("policy_no")===tProposalBznrobot("policy_no_slave"),"leftouter")
      .selectExpr("policy_no","product_code","premium","create_time","proposal_no","insurance_policy_no","holder_name","sell_channel_name")

    val res = resTemp.join(tProposalSubjectPersonMasterBznrobot,resTemp("proposal_no")===tProposalSubjectPersonMasterBznrobot("proposal_no_slave"),"leftouter")
      .where("product_code='P00001619' and insurance_policy_no != '' and premium>1")
      .selectExpr(
        "getUUID() as id",
        "insurance_policy_no",
        "concat(insurance_policy_no,'_',date_format(create_time,'yyyy-MM-dd HH:mm:ss')) as preserve_id",
        "premium as premium_total","holder_name","name as insurer_name","sell_channel_name as channel_name","create_time as policy_effective_time",
        "date_format(now(), 'yyyy-MM-dd HH:mm:ss') as create_time",
        "date_format(now(), 'yyyy-MM-dd HH:mm:ss') as update_time")
    res.printSchema()
    res.take(100).foreach(println)
    res
  }

  /**
    * 获取 Mysql 表的数据
    *
    * @param sqlContext 上下文
    * @param tableName 读取Mysql表的名字
    * @return 返回 Mysql 表的 DataFrame
    */
  def readMysqlTable(sqlContext: SQLContext, tableName: String): DataFrame = {
    val properties: Properties = getProPerties()
    sqlContext
      .read
      .format("jdbc")
      .option("url", properties.getProperty("mysql.url.106"))
      .option("driver", properties.getProperty("mysql.driver"))
      .option("user", properties.getProperty("mysql.username.106"))
      .option("password", properties.getProperty("mysql.password.106"))
      .option("numPartitions","10")
      .option("partitionColumn","id")
      .option("lowerBound", "0")
      .option("upperBound","200")
      .option("dbtable", tableName)
      .load()
  }

  /**
    * 获取配置文件
    * @return
    */
  def getProPerties() : Properties= {
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