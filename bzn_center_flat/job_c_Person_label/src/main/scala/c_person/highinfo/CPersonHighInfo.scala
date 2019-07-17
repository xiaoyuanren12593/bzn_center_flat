package c_person.highinfo

import bzn.job.common.{HbaseUtil, Until}
import c_person.util.SparkUtil
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * author:xiaoYuanRen
  * Date:2019/7/17
  * Time:16:53
  * describe: 高级标签
  **/
object CPersonHighInfo extends SparkUtil with Until with HbaseUtil  {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4

//    highInfoDetail(sc,hiveContext)
    sc.stop()
  }

  /**
    * 高级标签清洗
    * @param sc 上下文
    * @param sqlContext sql上下文
    */
  def highInfoDetail(sc:SparkContext,sqlContext:HiveContext) ={
    import sqlContext.implicits._
    /**
      * 读取产品表
      */
    val odsProductDetail = sqlContext.sql("select product_code as product_code_slave,product_name as product_name_slave,one_level_pdt_cate from odsdb.ods_product_detail")

    /**
      * 读取保单数据
      */
    val odsPolicyDetailTemp = sqlContext.sql("select holder_name as holder_name_slave,policy_id as policy_id_slave,policy_code,policy_status,product_code,product_name," +
      "first_premium,sum_premium,sku_coverage,policy_start_date,policy_end_date,channel_id,channel_name,policy_create_time,pay_way,belongs_regional," +
      "insure_company_name from odsdb.ods_policy_detail")

    val odsPolicyDetail =
      odsPolicyDetailTemp.join(odsProductDetail, odsPolicyDetailTemp("product_code") === odsProductDetail("product_code_slave"), "leftouter")
        .selectExpr("policy_id_slave", "policy_code", "holder_name_slave", "policy_status", "product_code", "product_name", "first_premium", "sum_premium",
          "sku_coverage", "policy_start_date", "policy_end_date", "channel_id", "channel_name", "policy_create_time", "pay_way", "belongs_regional",
          "insure_company_name")

    /**
      * 读取投保人
      */
    val odsHolderDetail =
      sqlContext.sql("select policy_id,holder_name,holder_cert_type,holder_cert_no from odsdb.ods_holder_detail")
        .where("holder_cert_type = 1 and length(holder_cert_no) = 18")
        .filter("notXing(holder_cert_no) = 1")
        .distinct()

    /**
      * 读取hbase上的数据
      */
    val hbaseData = getHbaseBussValue(sc,"label_person")
      .toDF("asda","result")
    hbaseData.show()
  }
}
