package com.bzn.test


import com.alibaba.fastjson.JSONObject
import org.apache.hadoop.hbase
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue}
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
  * 链接hbase
  */
object Read_Hbase_Test {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Read_Hbase_Test").setMaster("local[4]")

    val sc = new SparkContext(conf)

    val hbase_conf = HBaseConfiguration.create()

    hbase_conf.set("hbase.zookeeper.property.clientPort", "2181")
    hbase_conf.set("hbase.zookeeper.quorum", "172.16.11.106")
    hbase_conf.set("mapreduce.task.timeout", "1200000")
    hbase_conf.set("hbase.client.scanner.timeout.period", "600000")
    hbase_conf.set("hbase.rpc.timeout", "600000")
    hbase_conf.setInt("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily", 3000)
    //设置hbase要查询的表名
    hbase_conf.set(TableInputFormat.INPUT_TABLE, "labels:label_user_enterprise_vT")

    var res_rdd: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
      hbase_conf,
      classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result]
    )

    var res_row: RDD[(String, Array[hbase.KeyValue])] = res_rdd.map(res => {
      res._2
    }).map( f = x => {
      var ent_name: String = Bytes.toString(x.getValue("baseinfo".getBytes(), "ent_name".getBytes()))
      (ent_name, x.raw())
    })
    var res_rows: RDD[Array[KeyValue]] = res_rdd.map(res => {
      res._2.raw()
    })

    res_rows.foreach(println)
//    res_rdd.map(res => {
//      Bytes.toString(res._2.getRow)
//    }).foreach(println)

    var res: RDD[String] = res_row.mapPartitions(row => {
      val json: JSONObject = new JSONObject
      row.map(x => {
        var buffer = new StringBuffer
        val itr = x._2.iterator
        while(itr.hasNext){
          var end = itr.next()
          val row = Bytes.toString(end.getRow)
          val family = Bytes.toString(end.getFamily) //列族
          val qual = Bytes.toString(end.getQualifier) //字段
          val value = Bytes.toString(end.getValue) //字段值

          json.put("row", row)
          json.put("family", family)
          json.put("qual", qual)
          json.put("value", value)
          buffer.append(json.toString + ";")
        }
//        val strings: String = buffer.toString.split(";").filter(_.contains("ent_type")).take(1).mkString("")
//        println(strings)
//        println("_______________________")
//        var s: String = buffer.toString.split(";").filter(_.contains("ent_type")).mkString("")
//        println(s)
//        println("_______________________")
        s"${x._1}^^${buffer.substring(0, buffer.length - 1)}"
      })
    })

    res.foreach(x => {
      println(" ------  ")
    })

  }
}
