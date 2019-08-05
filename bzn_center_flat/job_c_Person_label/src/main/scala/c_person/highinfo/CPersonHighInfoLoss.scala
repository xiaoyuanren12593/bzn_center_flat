package c_person.highinfo

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
object CPersonHighInfoLoss extends SparkUtil with Until with HbaseUtil  {
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
  def highInfoDetail(sc:SparkContext,sqlContext:HiveContext) ={
    import sqlContext.implicits._
    sqlContext.udf.register("notXing", (str: String) => {
      if (str != null && str.contains("*")) {
        0
      } else {
        1
      }
    })

    /**
      * 读取hbase上的数据
      */
    val hbaseData = getHbaseBussValue(sc,"label_person")
      .map(x => {
        val key = Bytes.toString(x._2.getRow)
        val cusType = Bytes.toString(x._2.getValue("cent_info".getBytes, "cus_type".getBytes))
        val lastCusType = Bytes.toString(x._2.getValue("high_info".getBytes, "last_cus_type".getBytes)) //前一次投保类型
        val becomeCurrCusTime = Bytes.toString(x._2.getValue("high_info".getBytes, "become_curr_cus_time".getBytes))
        val lastPolicyEndDate = Bytes.toString(x._2.getValue("cent_info".getBytes, "last_policy_end_date".getBytes))
        (key,cusType,lastCusType,becomeCurrCusTime,lastPolicyEndDate)
      })
      .toDF("cert_no","cus_type","last_cus_type","become_curr_cus_time","last_policy_end_date")
      .where("cus_type in ('1','3')")

    /**
      *  客户标签为新客或易流失&保单到期&60内无复购
      */
    val currTime = getNowTime()
    val res = hbaseData.rdd.map(x => {
      val certNo = x.getAs[String]("cert_no")
      var cusType = x.getAs[String]("cus_type")
      val lastCusType = x.getAs[String]("last_cus_type")
      var becomeCurrCusTime = x.getAs[String]("become_curr_cus_time")
      val lastPolicyEndDate = x.getAs[String]("last_policy_end_date")
      val lastCusTypeRes = JSON.parseArray(lastCusType)
      var days = Long.MinValue
      var lossTime = ""
      if(lastPolicyEndDate != null){
        days = getBeg_End_one_two_new(lastPolicyEndDate,currTime)//保险止期和当前日期所差天数
        lossTime = currTimeFuction(lastPolicyEndDate.toString,60)
      }
      if(cusType == "1" && days > 60){
        cusType = "4"
        becomeCurrCusTime = timeSubstring(lossTime)
      }
      if(cusType == "3" && days > 60){
        lastCusTypeRes.add(("3",timeSubstring(becomeCurrCusTime)))
        cusType = "4"
        becomeCurrCusTime = timeSubstring(lossTime)
      }
      val jsonString = JSON.toJSONString(lastCusTypeRes, SerializerFeature.BeanToArray)
      (certNo,cusType,becomeCurrCusTime,jsonString)
    })
    .toDF("cert_no","cus_type","become_curr_cus_time","last_cus_type")
    .cache()

    val res1 = res.selectExpr("cert_no","cus_type")
    val  rowKeyName = "cert_no"
    val  tableName = "label_person"
    val  columnFamily1 = "cent_info"
    val  columnFamily2 = "high_info"
    putByList(sc,res1,tableName,columnFamily1,rowKeyName)
    val res2 = res.selectExpr("cert_no","last_cus_type","become_curr_cus_time")
    putByList(sc,res2,tableName,columnFamily2,rowKeyName)
  }
}
