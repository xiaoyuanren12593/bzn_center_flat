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
    sqlContext.udf.register("clean", (str: String) => clean(str))
    /**
      * 读取近5d新增的保单数据
      */
    val odsPolicyStreamingDetail = sqlContext.sql("select * from odsdb.ods_policy_streaming_detail")
      .selectExpr(
        "id",
        "holder_name",
        "policy_code",
        "channel_id",
        "channel_name",
        "status",
        "insured_count",
        "product_code",
        "create_time",
        "update_time"
      )

    /**
      * 产品表
      */
    val odsProductDetail = sqlContext.sql("select * from odsdb.ods_product_detail")
      .selectExpr("product_code as product_code_slave","one_level_pdt_cate")

    /**
      * 得到全部的雇主信息
      */
    val policystreaming = odsPolicyStreamingDetail.join(odsProductDetail,odsPolicyStreamingDetail("product_code")===odsProductDetail("product_code_slave"),"leftouter")
      .where("one_level_pdt_cate not in ('17000001','LGB000001') and one_level_pdt_cate = '蓝领外包'")
      .selectExpr(
        "holder_name",
        "policy_code",
        "'' as preserve_id",
        "channel_id",
        "channel_name",
        "status",
        "insured_count",
        "create_time",
        "update_time"
      )

    /**
      * 读取近5d新增的批单数据
      */
    val odsPreserveStreamingDetail = sqlContext.sql("select * from odsdb.ods_preserve_streaming_detail")
      .selectExpr(
        "holder_name",
        "policy_code",
        "preserve_id",
        "channel_id",
        "channel_name",
        "status",
        "insured_count",
        "create_time",
        "update_time"
      )

    /**
      * 5天之前的保单和批单的数据
      */
    val data5DBefore = policystreaming.unionAll(odsPreserveStreamingDetail)

    /**
      * 读取雇主销售表
      */
    val odsEntGuzhuSalesmanDetail = sqlContext.sql("select  ent_id,ent_name,salesman,biz_operator,channel_id as channel_id_slave," +
      "case when channel_name = '直客' then ent_name else channel_name end as channel_name_slave from odsdb.ods_ent_guzhu_salesman_detail")

    val res = data5DBefore.join(odsEntGuzhuSalesmanDetail,odsPolicyStreamingDetail("holder_name")===odsEntGuzhuSalesmanDetail("ent_name"),"leftouter")
      .selectExpr(
        "getUUID() as id",
        "policy_code",
        "clean(preserve_id) as preserve_id",
        "ent_id",
        "ent_name",
        "case when channel_id_slave is not null then channel_id_slave else channel_id end as channel_id",
        "case when channel_name_slave is not null then channel_name_slave else channel_name end as channel_name",
        "status",
        "insured_count",
        "salesman as sale_name",
        "biz_operator",
        "create_time",
        "update_time"
      )

    res
  }
}
