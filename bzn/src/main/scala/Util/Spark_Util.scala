package Util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

class Spark_Util {

  /**
    * 获取config_scala.properties文件的值
    * @param source 源文件
    * @param parameter 需要获取值得key
    * @return
    */
  def getValueByKey(source : String , parameter: String) :String ={

    val lines = Source.fromURL(getClass.getResource(source)).getLines

    val res = lines.filter(_.contains(parameter)).map(_.split("==")(1)).mkString("")

    res
  }

  /**
    * sparkConf配置信息获取上下文
    * @param name
    * @param local
    * @return
    */
  def sparkConf(name:String,local:String) : SparkContext ={
    //spark配置文件信息
    val conf_s = new SparkConf()
      .setAppName(name)
      .set("spark.sql.broadcastTimeout", "36000")
      .set("spark.network.timeout", "36000")
      .set("spark.executor.heartbeatInterval","20000")
    if(local != null && !"".equals(local)){
      println(local+"12313213")
      conf_s.setMaster(local)
    }
    //配置信息上下文
    val sc: SparkContext = new SparkContext(conf_s)

    sc
  }

  //HBaseConf 配置
  def HbaseConf(tableName: String): (Configuration, Configuration)
  = {
    /**
      * Hbase
      **/
    //定义HBase的配置
    val conf: Configuration = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "172.16.11.106")
    conf.set("mapreduce.task.timeout", "1200000")
    conf.set("hbase.client.scanner.timeout.period", "600000")
    conf.set("hbase.rpc.timeout", "600000")
    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    conf.setInt("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily", 3000)
    //设置配置文件，为了操作hdfs文件
    val conf_fs: Configuration = new Configuration()
    conf_fs.set("fs.default.name", "hdfs://namenode1.cdh:8020")
    (conf, conf_fs)
  }
}
