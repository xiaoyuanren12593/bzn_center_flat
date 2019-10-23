package bzn.dw.premium

import bzn.dw.util.SparkUtil
import bzn.job.common.{MysqlUntil, Until}
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * author:xiaoYuanRen
  * Date:2019/10/23
  * Time:16:10
  * describe: 每天新增的数据
  **/
object DwPolicyStreamingDetail extends SparkUtil with Until with MysqlUntil{
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName,"")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    val res = getHolderInfo(hiveContext)
    hiveContext.sql("truncate table dwdb.dw_policy_streaming_detail")
    res.repartition(1).write.mode(SaveMode.Append).saveAsTable("dwdb.dw_policy_streaming_detail")

    sc.stop()
  }


  def getHolderInfo(sqlContext:HiveContext) = {
    sqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))

    /**
      * 读取每天新增的数据
      */
    val odsPolicyStreamingDetail = sqlContext.sql("select * from odsdb.ods_policy_streaming_detail")
      .selectExpr("holder_name","insured_count")

    /**
      * 读取雇主销售表
      */
    val odsEntGuzhuSalesmanDetail = sqlContext.sql("select  ent_id,ent_name,salesman,biz_operator,channel_id," +
      "case when channel_name = '直客' then ent_name else channel_name end as channel_name from odsdb.ods_ent_guzhu_salesman_detail")

    val res = odsPolicyStreamingDetail.join(odsEntGuzhuSalesmanDetail,odsPolicyStreamingDetail("holder_name")===odsEntGuzhuSalesmanDetail("ent_name"),"leftouter")
      .selectExpr("getUUID() as id","channel_id","channel_name","ent_id","holder_name as ent_name","salesman as sale_name","biz_operator","insured_count",
        "date_format(now(), 'yyyy-MM-dd HH:mm:ss') as create_time",
        "date_format(now(), 'yyyy-MM-dd HH:mm:ss') as update_time")

    res
  }
}
