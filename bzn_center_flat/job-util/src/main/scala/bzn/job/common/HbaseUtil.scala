package bzn.job.common

import java.util.Date

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FsShell, Path}
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, HTable, Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles, TableInputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

/**
  * author:xiaoYuanRen
  * Date:2019/7/11
  * Time:15:55
  * describe: hbase工具类
  **/

trait HbaseUtil {

  /**
    * hhbase api方式写入
    * @param sc 上下文
    * @param rdd 数据集
    * @param tableName 表名
    * @param columnFamily 列簇
    * @param rowKeyName rowkey名称
    */
  def putByList(sc:SparkContext,rdd:DataFrame,tableName:String,columnFamily:String,rowKeyName:String): Unit = {
    try {
      val columns: Array[String] = rdd.columns.filter(x => x != rowKeyName)
      val broadcastColimns: Broadcast[Array[String]] = sc.broadcast(columns)

      rdd.foreachPartition(list => {

        //定义HBase的配置
        val conf: Configuration = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.property.clientPort", "2181")
        conf.set("hbase.zookeeper.quorum", "172.16.11.106,172.16.11.104,172.16.11.105,172.16.11.103,172.16.11.102")
        conf.set("mapreduce.task.timeout", "1200000")
        conf.set("hbase.client.scanner.timeout.period", "600000")
        conf.set("hbase.rpc.timeout", "600000")
        conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
        conf.setInt("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily",6000)

        val conn = ConnectionFactory.createConnection(conf)

        val table = conn.getTable(TableName.valueOf(tableName))

        val putList = new java.util.LinkedList[Put]()

        list.foreach(value => {
          val rowKey = value.getAs[String](rowKeyName)
          val put = new Put(Bytes.toBytes(rowKey))

          val columns = broadcastColimns.value
          columns.foreach(println)
          columns.foreach(x => {
            val columnsValue = value.getAs[Any](x)
            if(columnsValue != null && columnsValue != ""){
              put.addImmutable(Bytes.toBytes(columnFamily), Bytes.toBytes(x), Bytes.toBytes(columnsValue.toString))
            }
          })
          putList.add(put)
        })

        table.put(putList)
        table.close()
      })
    }
  }

  //得到个人标签数据
  def getHbaseBussValue(sc: SparkContext,tableName: String): RDD[(ImmutableBytesWritable, Result)] = {
    //定义HBase的配置
    val conf: Configuration = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "172.16.11.106,172.16.11.105,172.16.11.103")
    conf.set("mapreduce.task.timeout", "1200000")
    conf.set("hbase.client.scanner.timeout.period", "600000")
    conf.set("hbase.rpc.timeout", "600000")
    conf.setInt("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily", 3000)

    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, tableName)

    val usersRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
      conf,
      classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result]
    )
    usersRDD
  }

  //HBaseConf 配置
  def HbaseConf(tableName: String): (Configuration, Configuration) = {
    //定义HBase的配置
    val conf: Configuration = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "172.16.11.106,172.16.11.104,172.16.11.105,172.16.11.103,172.16.11.102")
    conf.set("mapreduce.task.timeout", "1200000")
    conf.set("hbase.client.scanner.timeout.period", "600000")
    conf.set("hbase.rpc.timeout", "600000")
    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    conf.setInt("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily",6000)
    //设置配置文件，为了操作hdfs文件
    val conf_fs: Configuration = new Configuration()
    conf_fs.set("fs.default.name", "hdfs://namenode1.cdh:8020")
    (conf, conf_fs)
  }

  //对文件进行权限的设置
  def proessFile(conf_fs: Configuration, stagingFolder: String): Unit = {
    val shell = new FsShell(conf_fs)
    shell.run(Array[String]("-chmod", "-R", "777", stagingFolder))

  }

  //删除HFile文件
  def deleteFile(conf_fs: Configuration, stagingFolder: String): Unit = {
    val hdfs = FileSystem.get(conf_fs)
    val path = new Path(stagingFolder)
    hdfs.delete(path,true)
  }

  //将hfile存到Hbase中
  def saveToHbase(result: RDD[(String, String, String)], columnFamily1: String,
                  conf_fs: Configuration, tableName: String, conf: Configuration): Unit = {
    val stagingFolder = s"/hbasehfile/$columnFamily1"
    //创建hbase的链接,利用默认的配置文件,实际上读取的hbase的master地址
    val hdfs = FileSystem.get(conf_fs)
    val path = new Path(stagingFolder)

    //检查是否存在
    if (!hdfs.exists(path)) {
      //不存在就执行存储
      deToHbase(result,columnFamily1,conf_fs,conf,tableName,stagingFolder)
    } else if (hdfs.exists(path)) {
      //存在即删除后执行存储
      deleteFile(conf_fs, stagingFolder)
      deToHbase(result,columnFamily1,conf_fs,conf,tableName,stagingFolder)
    }
  }

  /**
    * 写入hbase
    * @param result //rdd
    * @param columnFamily1 列簇
    * @param conf_fs hdfs
    * @param conf hbase
    * @param stagingFolder  文件存储路径
    */
  def deToHbase(result: RDD[(String, String, String)],columnFamily1: String, conf_fs: Configuration,conf: Configuration,tableName:String,stagingFolder:String): Unit = {
    val sourceRDD: RDD[(ImmutableBytesWritable, KeyValue)] = result
      .sortBy(_._1)
      .map(x => {
        //rowkey
        val rowKey = Bytes.toBytes(x._1)
        //列族
        val family = Bytes.toBytes(columnFamily1)
        //字段
        val colum = Bytes.toBytes(x._3)
        //当前时间
        val date = new Date().getTime
        //数据
        val value = Bytes.toBytes(x._2)

        //将RDD转换成HFile需要的格式，并且我们要以ImmutableBytesWritable的实例为key
        (new ImmutableBytesWritable(rowKey), new KeyValue(rowKey, family, colum, date, value))
      })

    //hbase名称

    //创建hbase连接
    val conn = ConnectionFactory.createConnection(conf)

    //得到表数据
    val table = conn.getTable(TableName.valueOf(tableName))

    try {
      //创建一个job
      lazy val job = Job.getInstance(conf)
      job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
      job.setMapOutputValueClass(classOf[KeyValue])

      HFileOutputFormat2.configureIncrementalLoadMap(job,table)

      val load = new LoadIncrementalHFiles(conf)

      //保存一个文件
      sourceRDD.saveAsNewAPIHadoopFile(stagingFolder,
        classOf[ImmutableBytesWritable],
        classOf[KeyValue],
        classOf[HFileOutputFormat2],
        job.getConfiguration()
      )

      //权限设置
      proessFile(conf_fs, stagingFolder + "/*")

      //开始导入
      load.doBulkLoad(new Path(stagingFolder), table.asInstanceOf[HTable])

    }finally {
      //关闭连接
      table.close()
      conn.close()
    }
  }

  /**
    * 将DataFrame写入HBase
    * @param dataFrame
    * @param tableName
    * @param columnFamily
    */
  def toHBase(dataFrame: DataFrame, tableName: String, columnFamily: String,rowKeyName:String)  = {
    //    获取conf
    val con: (Configuration, Configuration) = HbaseConf(tableName)
    val conf_fs: Configuration = con._2
    val conf: Configuration = con._1
    //    获取列
    val cols: Array[String] = dataFrame.columns
    //    取不等于key的列循环
    cols.filter(x => x != rowKeyName).map(x => {
      val hbaseRDD: RDD[(String, String, String)] = dataFrame.map(rdd => {
        val certNo = rdd.getAs[String](rowKeyName)
        val clo: Any = rdd.getAs[Any](x)
        //证件号，列值 列名
        (certNo,clo,x)
      })
      .filter(x => x._2 != null && x._2 != "")
      .map(x => (x._1,x._2.toString,x._3))

      saveToHbase(hbaseRDD, columnFamily, conf_fs, tableName, conf)
    })
  }
}
