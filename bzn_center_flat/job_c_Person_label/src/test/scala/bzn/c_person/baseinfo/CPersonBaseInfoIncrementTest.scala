package bzn.c_person.baseinfo

import java.text.SimpleDateFormat
import java.util.Date

import bzn.c_person.util.SparkUtil
import bzn.job.common.{HbaseUtil, Until}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * author:sangJiaQI
  * Date:2019/7/30
  * describe: 基础标签增量
  */
object CPersonBaseInfoIncrementTest extends SparkUtil with Until with HbaseUtil{

  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName: String = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "local[*]")

    val sc: SparkContext = sparkConf._2
    val hiveContext: HiveContext = sparkConf._4


    sc.stop()

  }





  def newPolicyId(hiveContext: HiveContext): DataFrame = {
    import hiveContext.implicits._
    hiveContext.udf.register("getEmptyString", () => "")
    hiveContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")//设置日期格式
      val data = df.format(new Date())// new Date()为获取当前系统时间
      (data + "")
    })

    /**
      * 读取hive中新增保单表
      */
    val PolicyId: DataFrame = hiveContext.sql("select policy_id, inc_type from dwdb.dw_policy_detail_inc")
      .where("inc_type = 0")
      .selectExpr("policy_id")

//    新增保单号
    PolicyId

  }


}
