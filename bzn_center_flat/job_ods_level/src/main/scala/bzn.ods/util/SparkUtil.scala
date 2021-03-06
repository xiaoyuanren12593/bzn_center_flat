package bzn.ods.util

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{ SQLContext}
import org.apache.spark.sql.hive.HiveContext

import scala.io.Source

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
    if(exceType != ""){
      conf.setMaster(exceType)
    }

    val sc = new SparkContext(conf)

    //SET hive.exec.dynamic.partition = true

    val sqlContext = new SQLContext(sc)
    //sqlContext.setConf("hive.exec.dynamic.partition.mode","nonstrict")

    val hiveContext = new HiveContext(sc)
    hiveContext.setConf("hive.exec.dynamic.partition.mode","nonstrict")
    hiveContext.setConf("hive.exec.dynamic.partition","true")

    (conf,sc,sqlContext,hiveContext)
  }
}