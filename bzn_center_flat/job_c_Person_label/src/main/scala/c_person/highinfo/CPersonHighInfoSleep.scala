package c_person.highinfo

import java.sql.Timestamp

import bzn.job.common.{HbaseUtil, Until}
import c_person.util.SparkUtil
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * author:xiaoYuanRen
  * Date:2019/7/17
  * Time:16:53
  * describe: 高级标签
  **/
object CPersonHighInfoSleep extends SparkUtil with Until with HbaseUtil  {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4

     highInfoDetail(sc,hiveContext)
    sc.stop()
  }

  /**
    * 高级标签清洗
    * @param sc 上下文
    * @param sqlContext sql上下文
    */
  def highInfoDetail(sc:SparkContext,sqlContext:HiveContext): DataFrame ={
    import sqlContext.implicits._
    sqlContext.udf.register("notXing", (str: String) => {
      if (str != null && str.contains("*")) {
        0
      } else {
        1
      }
    })

    /**
      * 读取保单数据
      */
    val odsPolicyDetail = sqlContext.sql("select policy_id as policy_id_slave,policy_start_date,policy_end_date,policy_create_time" +
      " from odsdb.ods_policy_detail")

    /**
      * 读取投保人
      */
    val odsHolderDetail =
      sqlContext.sql("select policy_id,holder_cert_type,holder_cert_no from odsdb.ods_holder_detail")
        .where("holder_cert_type = 1 and length(holder_cert_no) = 18")
        .filter("notXing(holder_cert_no) = 1")
        .distinct()

    /**
      * 得到止期最大的保单
      */
    val holderMaxEnd = odsHolderDetail.join(odsPolicyDetail,odsHolderDetail("policy_id")===odsPolicyDetail("policy_id_slave"))
      .selectExpr("holder_cert_no","case when policy_start_date is null then policy_create_time else policy_start_date end as policy_start_date","policy_end_date")
      .where("policy_end_date is not null")
      .map(x => {
      val holderCert_no = x.getAs[String]("holder_cert_no")
      val policyStartDate = x.getAs[java.sql.Timestamp]("policy_start_date")
      val policyEndDate = x.getAs[java.sql.Timestamp]("policy_end_date")
      (holderCert_no,(policyStartDate,policyEndDate))
    })
      .reduceByKey((x1, x2) => {
        val policyEndDate = if (x1._2.compareTo(x2._2) >= 0) x1 else x2
        policyEndDate
      })
      .map(x => (x._1, x._2._1,x._2._2))
      .toDF("cert_no", "policy_start_date","policy_end_date")

    /**
      * 最近的保单
      */
    val holderNewPolicy = odsHolderDetail.join(odsPolicyDetail,odsHolderDetail("policy_id")===odsPolicyDetail("policy_id_slave"))
      .selectExpr("holder_cert_no","case when policy_start_date is null then policy_create_time else policy_start_date end as policy_start_date")
      .where("policy_start_date is not null")
      .map(x => {
        val holderCert_no = x.getAs[String]("holder_cert_no")
        val policyStartDate = x.getAs[java.sql.Timestamp]("policy_start_date")
        (holderCert_no,policyStartDate)
      })
      .reduceByKey((x1, x2) => {
        val policyEndDate = if (x1.compareTo(x2) >= 0) x1 else x2
        policyEndDate
      })
      .map(x => (x._1, x._2))
      .toDF("cert_no_slave", "policy_new_start_date")

    /**
      * 保单止期最大保单和最近保单信息关联
      * 如果保单止期最大的保单的开始时间小于最近保单的开始时间用最近保单的开始时间
      */
    val holdInfo = holderMaxEnd.join(holderNewPolicy,holderMaxEnd("cert_no")===holderNewPolicy("cert_no_slave"))
      .selectExpr("cert_no as holder_cert_no",
        "case when policy_new_start_date > policy_start_date then policy_new_start_date else policy_start_date end policy_new_start_date",
        "policy_start_date","policy_end_date")

    holdInfo.printSchema()


    /**
      * 读取hbase上的数据
      */
    val hbaseData = getHbaseBussValue(sc,"label_person")
        .map(x => {
          val key = Bytes.toString(x._2.getRow)
          val cusType = Bytes.toString(x._2.getValue("cent_info".getBytes, "cus_type".getBytes))
          val lastCusType = Bytes.toString(x._2.getValue("high_info".getBytes, "last_cus_type".getBytes)) //前一次投保类型
          val becomeCurrCusTime = Bytes.toString(x._2.getValue("high_info".getBytes, "become_curr_cus_time".getBytes))
          (key,cusType,lastCusType,becomeCurrCusTime)
        })
      .toDF("cert_no","cus_type","last_cus_type","become_curr_cus_time")
      .where("cus_type in ('1','2','3','4','5')")

    /**
      * 标签数据和投保人数据进行关联
      */
    val currTime = getNowTime()
    val res = hbaseData.join(holdInfo,hbaseData("cert_no")===holdInfo("holder_cert_no"),"leftouter")
      .selectExpr("cert_no","cus_type","policy_new_start_date","policy_start_date","policy_end_date","last_cus_type","become_curr_cus_time")
      .map(x => {
        val certNo = x.getAs[String]("cert_no")
        var cusType = x.getAs[String]("cus_type")
        var lastCusType = x.getAs[String]("last_cus_type")
        val policyNewStartDate = x.getAs[java.sql.Timestamp]("policy_new_start_date")
        val policyStartDate = x.getAs[java.sql.Timestamp]("policy_start_date")
        val policyEndDate = x.getAs[java.sql.Timestamp]("policy_end_date")
        var becomeCurrCusTime = x.getAs[String]("become_curr_cus_time")
        val cuurTimeNew = Timestamp.valueOf(currTime)
        /**
          * 上一个客户类型
          */
        val lastCusTypeRes = JSON.parseArray(lastCusType)
//        val lastType = lastCusTypeRes.getJSONArray(JSON.parseArray(lastCusType).size()-1).get(0)
        /**
          * 沉睡
          * 客户标签为流失，且近180天未在保&未投保的客户
          */
        if(cusType == "4"){
          if(policyEndDate != null){
            val days2 = getBeg_End_one_two_new(policyEndDate.toString,cuurTimeNew.toString) //终止天数--未投保天数 复购天数
            if(days2 > 180){//在保
              cusType = "5"
              lastCusTypeRes.add(("4",timeSubstring(becomeCurrCusTime)))
              becomeCurrCusTime = currTimeFuction(policyEndDate.toString,180)
            }
          }
        }
        val jsonString = JSON.toJSONString(lastCusTypeRes, SerializerFeature.BeanToArray)
        (certNo,cusType,becomeCurrCusTime,jsonString)
      })
      .toDF("cert_no","cus_type","become_curr_cus_time","last_cus_type")

    val res1 = res.selectExpr("cert_no","cus_type")
    val  rowKeyName = "cert_no"
    val  tableName = "label_person"
    val  columnFamily1 = "cent_info"
    val  columnFamily2 = "high_info"
    toHBase(res1,tableName,columnFamily1,rowKeyName)
    val res2 = res.selectExpr("cert_no","become_curr_cus_time","last_cus_type")
    toHBase(res2,tableName,columnFamily2,rowKeyName)
    res2
  }
}
