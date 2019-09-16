package bzn.other

import java.util.Properties

import bzn.job.common.Until
import bzn.ods.util.SparkUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.hive.HiveContext

import scala.io.Source

object OfoToHiveTest extends SparkUtil with Until {

  def main(args: Array[String]): Unit = {

    //    初始化设置
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName: String = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "local[*]")

    val sc: SparkContext = sparkConf._2
    val hiveContext: HiveContext = sparkConf._4

    //    合并去重
    val oldOfoInfo: DataFrame = oldOfoDetail(hiveContext)
    val newOfoInfo: DataFrame = newOfoDetail(hiveContext)
    val result: DataFrame = oldOfoInfo
      .unionAll(newOfoInfo)
      .dropDuplicates(Array("policy_id"))

    result.printSchema()

    sc.stop()

  }

  /**
    * 2016年开始的ofo表
    * @param hiveContext
    * @return
    */
  def oldOfoDetail(hiveContext: HiveContext): DataFrame = {
    import hiveContext.implicits._
    hiveContext.udf.register("dropEmptys", (line: String) => dropEmpty(line))
    hiveContext.udf.register("dropSpecial", (line: String) => dropSpecial(line))

    /**
      * 读取ofo接口的hive表
      */
    val oldOfoInfo: DataFrame = hiveContext.sql("select policy_id, proposal_no, user_id, product_code, start_date, end_date, " +
      "insured_name, insured_mobile, insured_cert_type, insured_cert_no, create_time, month_id as month from odsdb_prd.open_ofo_policy_parquet")
      .selectExpr("clean(policy_id) as policy_id", "clean(proposal_no) as proposal_no", "clean(user_id) as user_id",
        "clean(product_code) as product_code", "clean(start_date) as start_date", "clean(end_date) as end_date",
        "clean(insured_name) as insured_name", "clean(insured_mobile) as insured_mobile", "clean(insured_cert_type) as insured_cert_type",
        "clean(insured_cert_no) as insured_cert_no", "clean(create_time) as create_time", "clean(month) as month")

    //    返回表
    oldOfoInfo

  }

  /**
    * 旧的ofo数据
    * @param hiveContext
    * @return
    */
  def newOfoDetail(hiveContext: HiveContext): DataFrame = {
    import hiveContext.implicits._
    hiveContext.udf.register("dropEmptys", (line: String) => dropEmpty(line))
    hiveContext.udf.register("dropSpecial", (line: String) => dropSpecial(line))

    val newOfoInfo: DataFrame = hiveContext.sql("select policy_id, proposal_no, user_id, product_code, start_date, end_date, " +
      "insured_name, insured_mobile, insured_cert_type, insured_cert_no, create_time, month from odsdb.ods_open_ofo_policy_detail")
      .selectExpr("clean(policy_id) as policy_id", "clean(proposal_no) as proposal_no", "clean(user_id) as user_id",
        "clean(product_code) as product_code", "clean(cast(start_date as string)) as start_date", "clean(cast(end_date as string)) as end_date",
        "clean(insured_name) as insured_name", "clean(insured_mobile) as insured_mobile", "clean(cast(insured_cert_type as int)) as insured_cert_type",
        "clean(insured_cert_no) as insured_cert_no", "clean(cast(create_time as string)) as create_time", "clean(cast(month as string)) as month")

    //    返回表
    newOfoInfo

  }

}
