package bzn.other

import bzn.job.common.HbaseUtil
import bzn.ods.util.{SparkUtil, Until}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

/**
  * author:xiaoYuanRen
  * Date:2019/8/20
  * Time:17:50
  * describe: this is new class
  **/
object VictoryLogs extends SparkUtil with HbaseUtil with Until{
  def main (args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName,"local[*]")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    val logDatas = sc.textFile("hdfs://namenode1.cdh:8020/xing/data/spring.log.2019-02-01.0")
    var strBuffer= new java.lang.StringBuffer()

    val res = logDatas.map(x => {
      if(x.length > 19){
        val str = x.substring(0,19)
        val res = verifyData(str)
        val result = if(res == true){
          strBuffer
          strBuffer.append(x)
        }else{
          strBuffer.append(x)
        }
        result
      }else{
        null
      }
    })

    res.foreach(println)
    sc.stop()
  }
}
