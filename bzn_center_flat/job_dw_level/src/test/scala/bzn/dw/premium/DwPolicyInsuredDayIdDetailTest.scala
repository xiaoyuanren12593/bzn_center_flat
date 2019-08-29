package bzn.dw.premium

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date
import bzn.dw.util.SparkUtil
import bzn.job.common.Until
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * author:xiaoYuanRen
  * Date:2019/6/12
  * Time:19:24
  * describe: this is new class
  **/
object DwPolicyInsuredDayIdDetailTest extends SparkUtil with Until{
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName,"local[*]")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    val res = dwPolicyInsuredDayIdDetail(hiveContext)
    //    res.write.mode(SaveMode.Overwrite).saveAsTable("dwdb.dw_policy_curr_insured_detail")
//    val outputTmpDir = "/dwdb/dw_policy_curr_insured_detail"
//    val output = "dwdb.dw_policy_curr_insured_detail"
//    res.rdd.map(x => x.mkString("\001")).saveAsTextFile(outputTmpDir)
//    hiveContext.sql(s"""load data  inpath '$outputTmpDir' overwrite into table $output""")
    sc.stop()
  }

  def dwPolicyInsuredDayIdDetail(sqlContext:HiveContext) ={
    import sqlContext.implicits._
    sqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    sqlContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")//设置日期格式
      val date = df.format(new Date())// new Date()为获取当前系统时间
      (date + "")
    })
    /**
      * 读取保单表
      */
    val odsPolicyDetail =
      sqlContext.sql("select policy_id, policy_code, product_code, policy_start_date, policy_end_date, policy_status from odsdb.ods_policy_detail")
        .where("length(policy_code) > 0 and policy_status in (0,1,-1)")
        .cache()

    /**
      * 读取产品表
      */
    val odsProductDetail =
      sqlContext.sql("select product_code as product_code_slave, one_level_pdt_cate from odsdb.ods_product_detail")
        .where("one_level_pdt_cate = '蓝领外包'")
        .cache()

    /**
      * 保单和产品进行关联 得到产品为蓝领外包（雇主）的所有保单,并算出每日保单信息
      */
    val policyAndProductTemp = odsPolicyDetail.join(odsProductDetail,odsPolicyDetail("product_code") === odsProductDetail("product_code_slave")).cache()

    val cachePolicy = policyAndProductTemp.selectExpr("policy_id as policy_id_cache","policy_code").cache()

    /**
      * 将保单和day_id进行拉平操作 即每个保单对应一个day_id
      */
    val policyAndProductOne = policyAndProductTemp
      .selectExpr("policy_id","policy_start_date","policy_end_date")
      .mapPartitions(rdd => {
        rdd.flatMap(x => {
          val policyId = x.getAs[String]("policy_id")
          val policyStartDate = x.getAs[Timestamp]("policy_start_date").toString.split(" ")(0).replaceAll("-", "").replaceAll("/", "")
          val policyEndDate = x.getAs[Timestamp]("policy_end_date").toString.split(" ")(0).replaceAll("-", "").replaceAll("/", "")
          val res = getBeg_End_one_two(policyStartDate, policyEndDate).map(day_id => {
            (policyId,day_id)
          })
          res
        })
      })
      .toDF("policy_id","day_id")

    /**
      * 得到保单和保单号
      */
    val policyAndProduct = policyAndProductOne.join(cachePolicy,policyAndProductOne("policy_id")===cachePolicy("policy_id_cache"))
      .selectExpr("policy_id","policy_code","day_id")
    /**
      * 读取在保人明细表
      */
    val odsPolicyInsuredDetail =
      sqlContext.sql("select policy_id as policy_id_insured,start_date,end_date,insured_cert_no from odsdb.ods_policy_insured_detail")
        .where("length(policy_id_insured) > 0 and ( start_date is not null or end_date is not null)")
        .cache()

    /**
      * 每个保单每天在保人明细
      */
    val InsuredDay = policyAndProductTemp.join(odsPolicyInsuredDetail,policyAndProductTemp("policy_id")===odsPolicyInsuredDetail("policy_id_insured"))

    val InsuredDayRes = InsuredDay
      .selectExpr("policy_id","start_date","end_date","insured_cert_no")
      .mapPartitions(rdd => {
        rdd.flatMap(x => {
          val policyId = x.getAs[String]("policy_id")

          var insuredStartDate = x.getAs[Timestamp]("start_date")
          var insuredEndDate = x.getAs[Timestamp]("end_date")

          if(insuredStartDate == null) {
            insuredStartDate = insuredEndDate
          }

          if(insuredEndDate == null){
            insuredEndDate = insuredStartDate
          }

          val insuredStartDateRes = x.getAs[Timestamp]("start_date").toString.split(" ")(0).replaceAll("-", "").replaceAll("/", "")
          val insuredEndDateRes = x.getAs[Timestamp]("end_date").toString.split(" ")(0).replaceAll("-", "").replaceAll("/", "")
          val insuredCertNo = x.getAs[String]("insured_cert_no")
          val res: ArrayBuffer[(String, String, String)] = getBeg_End_one_two(insuredStartDateRes, insuredEndDateRes).map(day_id => {
            (policyId,day_id,insuredCertNo)
          })

          res
        })
      })
      .toDF("policy_id","day_id","insured_cert_no")

    /**
      * 保单每日信息与在保人每日信息关联得到
      */
    val res = policyAndProduct.join(InsuredDayRes,Seq("policy_id","day_id"),"leftouter")
      .selectExpr("policy_id","policy_code","day_id","insured_cert_no")
      .map(x => {
        val policyId = x.getAs[String]("policy_id")
        val policyCode = x.getAs[String]("policy_code")
        val dayId = x.getAs[String]("day_id")
        var insuredCertNo = x.getAs[String]("insured_cert_no")
        if(insuredCertNo == null){
          insuredCertNo = "`"
        }
        ((policyId,policyCode,dayId),insuredCertNo)
      })
      .groupByKey()
      .map(x => {
        var res: Int = 0
        val ls = x._2.toSet
        if(!ls.isEmpty){
          res  = ls.size
        }
        if(ls.contains("`")){
          if(res != 0){
            res = res-1
          }
        }
        (x._1._1,x._1._2,x._1._3,res)
      })
      .toDF("policy_id","policy_code","day_id","count")
      .selectExpr("getUUID() as id","policy_id","policy_code","day_id","count","getNow() as dw_create_time")
    res
    res.printSchema()
  }
}
