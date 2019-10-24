package bzn.other

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import bzn.dw.premium.DwPolicyInsuredDayIdDetailTest.getBeg_End_one_two
import bzn.job.common.Until
import bzn.ods.util.SparkUtil
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * author:xiaoYuanRen
  * Date:2019/10/24
  * Time:20:50
  * describe: this is new class
  **/
object EveryMonthEmpDataTest extends  SparkUtil with Until{
    def main(args: Array[String]): Unit = {
    System.setProperty ("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo (appName, "")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    dwPolicyInsuredDayIdDetail(hiveContext)
    sc.stop ()
  }
  def dwPolicyInsuredDayIdDetail(sqlContext:HiveContext) = {
    import sqlContext.implicits._
    sqlContext.udf.register ("getUUID", () => (java.util.UUID.randomUUID () + "").replace ("-", ""))
    sqlContext.udf.register ("getNow", () => {
      val df = new SimpleDateFormat ("yyyy-MM-dd HH:mm:ss")
      //设置日期格式
      val date = df.format (new Date ()) // new Date()为获取当前系统时间
      (date + "")
    })
    /**
      * 读取保单表
      */
    val odsPolicyDetail =
      sqlContext.sql ("select id,policy_id, policy_code, product_code, policy_start_date, policy_end_date, policy_status,holder_name from odsdb.ods_policy_detail")
        .where ("length(policy_code) > 0 and policy_status in (0,1,-1)" +
          " and policy_start_date >= '2018-01-01 00:00:00' and " +
          "policy_end_date is not null")
        .repartition (200)
        .cache ()

    /**
      * 读取产品表
      */
    val odsProductDetail =
      sqlContext.sql ("select product_code as product_code_slave, one_level_pdt_cate from odsdb.ods_product_detail")
        .where ("one_level_pdt_cate = '蓝领外包'")
        .cache ()

    /**
      * 保单和产品进行关联 得到产品为蓝领外包（雇主）的所有保单,并算出每日保单信息
      */
    val policyAndProductTemp = odsPolicyDetail.join (odsProductDetail, odsPolicyDetail ("product_code") === odsProductDetail ("product_code_slave")).cache ()
      .selectExpr("policy_id","policy_start_date","policy_end_date","holder_name")


    val policyAndProductOne = policyAndProductTemp
      .selectExpr("policy_id","policy_start_date","policy_end_date","holder_name")
      .mapPartitions(rdd => {
        rdd.flatMap(x => {
          val holderName = x.getAs[String]("holder_name")
          val policyStartDate = x.getAs[Timestamp]("policy_start_date").toString
          val policyEndDate = x.getAs[Timestamp]("policy_end_date").toString
          val res = getBeg_End_one_two_month(policyStartDate, policyEndDate)
            .map(day_id => {
            (holderName,day_id)
          })
          res
        })
      })
      .distinct()
      .toDF("holder_name","day_id")
    policyAndProductOne

    policyAndProductOne.rdd.repartition(1).saveAsTextFile("C:\\Users\\xingyuan\\Desktop\\未完成 2\\11.数据仓库项目搭建\\提数\\雇主续投数据1")
    policyAndProductOne.show()
  }
}
