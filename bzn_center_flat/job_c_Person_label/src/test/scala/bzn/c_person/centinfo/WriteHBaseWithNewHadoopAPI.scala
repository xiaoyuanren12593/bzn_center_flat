package bzn.c_person.centinfo


import java.util

import bzn.c_person.util.SparkUtil
import bzn.job.common.{HbaseUtil, Until}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.filter.{Filter, FilterList}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * author:xiaoYuanRen
  * Date:2019/7/17
  * Time:10:21
  * describe: this is new class
  **/
object WriteHBaseWithNewHadoopAPI extends SparkUtil with Until with HbaseUtil{
  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "local[*]")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
//    val res = getHbaseQueryRDD(("","110000198608059433","110101195107290010",util.List[Filter]),sc)
//    res.map(_._2).foreach(println)
  }

  def getHbaseQueryRDD(taskParam: (String, String, String, util.List[Filter]), sc: SparkContext): RDD[(ImmutableBytesWritable, Result)] = {

    val hbaseConf: Configuration = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseConf.set("hbase.zookeeper.quorum", "172.16.11.106,172.16.11.105,172.16.11.103")
    hbaseConf.set("mapreduce.task.timeout", "1200000")
    hbaseConf.set("hbase.client.scanner.timeout.period", "600000")
    hbaseConf.set("hbase.rpc.timeout", "600000")
    hbaseConf.setInt("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily", 3000)

    //设置查询的表名
    hbaseConf.set(TableInputFormat.INPUT_TABLE, "label_person")
    val scan = new Scan()
    scan.setStartRow(Bytes.toBytes(taskParam._2))
    scan.setStopRow(Bytes.toBytes(taskParam._3))
    val filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, taskParam._4)
    scan.setFilter(filterList)
    hbaseConf.set(TableInputFormat.SCAN, convertScanToString(scan))
    val rs = sc.newAPIHadoopRDD(
      hbaseConf,
      classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    //todo 解析
    rs

  }
  private def convertScanToString(scan: Scan) = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }

}
