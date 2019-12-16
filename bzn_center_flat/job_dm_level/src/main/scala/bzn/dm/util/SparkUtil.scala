package bzn.dm.util

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * author:xiaoYuanRen
  * Date:2019/5/21
  * Time:9:54
  * describe: this is new class
  **/
trait SparkUtil {
  /**
    * spark配置信息以及上下文
    * @param appName 名称
    * @param exceType 执行类型，本地/集群
    */
  def sparkConfInfo(appName:String,exceType:String): (SparkConf, SparkContext, SQLContext, HiveContext) ={
    val conf = new SparkConf()
      .setAppName(appName)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable]))
      .set("spark.sql.broadcastTimeout", "36000")
      .set("spark.network.timeout", "36000")
    if(exceType != ""){
      conf.setMaster(exceType)
    }

    val sc = new SparkContext(conf)

    val sqlContest = new SQLContext(sc)

    val hiveContext = new HiveContext(sc)
    hiveContext.setConf("hive.exec.dynamic.partition","true")
    hiveContext.setConf("hive.exec.dynamic.partition.mode","nonstrict")

    (conf,sc,sqlContest,hiveContext)
  }

}