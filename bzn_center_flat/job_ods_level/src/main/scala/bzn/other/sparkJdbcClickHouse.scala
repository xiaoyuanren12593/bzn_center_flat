package bzn.other

import bzn.job.common.Until
import bzn.util.SparkUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}


import scala.io.Source

/*
* @Author:liuxiang
* @Date：2019/10/10
* @Describe:
*/
object sparkJdbcClickHouse extends  SparkUtil with Until{

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName,"")

    val sc = sparkConf._2
    val hiveContext: HiveContext = sparkConf._4
    val res: DataFrame = hiveContext.sql("select policy_id,policy_code,dw_create_time from odsdb.ods_policy_detail")

    writeTable(res,"odsdb.ods_policy_test")
    sc.stop()


  }


  /**
    *
    */
  def  writeTable(res:DataFrame,tableName: String): Unit ={
    res.write
        .mode(SaveMode.Append)
      .format("jdbc")
      .option("url","jdbc:clickhouse://172.16.11.100:8123/odsdb")
      .option("driver","cc.blynk.clickhouse.ClickHouseDriver")
      .option("numPartitions","1")//设置并发
      .option("isolationLevel","NONE")
      .option("dbtable",s"{$tableName}")
      .option("user","default")
      .option("password","iaisYuX4")
      .option("batchsize","50000")
      .save()

  }





}
