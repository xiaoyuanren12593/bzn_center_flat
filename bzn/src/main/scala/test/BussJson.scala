package test

import Util.Spark_Util
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FsShell
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object BussJson {
  def main(args: Array[String]): Unit = {

    //程序用户名
    System.setProperty("HADOOP_USER_NAME", "root")

    //spark 自定义工具类
    val util = new Spark_Util

    val location_mysql_url = util.getValueByKey("/config_scala.properties","location_mysql_url")

    val hdfs_url = util.getValueByKey("/config_scala.properties","hdfs_url")

    val sc = util.sparkConf("ParseJson","")

    val file: Array[String] = sc.textFile("D:\\idea-poject\\bzn\\src\\main\\scala\\test_file\\buss.json").collect()

    var json = file.mkString("")

    //获取三级目录
    var res = JSON.parseObject(JSON.parseObject(json).get("data").toString).get("list").toString

    val nObject: JSONArray = JSON.parseArray(res)

    val value: Array[AnyRef] = nObject.toArray()

    //获取基础字段信息baseInfo
    val baseInfo: Array[String]= getBaseInfo_Buss(value)

    //获取可变字段函数
    val customField: Array[String] = getCustomField(value)

    //合并baseInfo和可变字段数据
    var baseInfo_CustomField =  getMerge(baseInfo,customField)

    //将合并的数组转换成RDD
    var rdd1: RDD[(String, String)] = sc.parallelize(baseInfo_CustomField.toList)

    rdd1.foreach(println)
    //HBaseConf
    val conf = HbaseConf("crm_niche")._1
    val tableName = "crm_niche"
    val columnFamily1 = "baseInfo"
    val columnFamily2 = "customField"

    //存储到hbase中
    saveHbase(rdd1,sc,tableName,conf,columnFamily1,columnFamily2)

    sc.stop()
  }

  /**
    * 存储到hbase
    * @param indataRDD
    * @param sc
    * @param tablename
    * @param conf
    * @param columnFamily1
    */
  def saveHbase(indataRDD:RDD[(String, String)] ,sc : SparkContext,tablename:String,conf: Configuration,columnFamily1:String,columnFamily2:String): Unit ={

    //初始化jobconf，TableOutputFormat必须是org.apache.hadoop.hbase.mapred包下的！
    val jobConf = new JobConf(conf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tablename)

    var rdd = indataRDD.map(x => {
      /*一个Put对象就是一行记录，在构造方法中指定主键
      * 所有插入的数据必须用org.apache.hadoop.hbase.util.Bytes.toBytes方法转换
      * Put.add方法接收三个参数：列族，列名，数据
      */
      val put = new Put(Bytes.toBytes(x._1.toString))
      //(id=1210807,name=音曼（北京）科技有限公司|officeId=501|telephone=|mobile=|email=null|customerHighSeaId=196|address=null|classification=null|province=0|city=0|district=0|remark=null|businessCategoryId=229|highSeaStatus=4|customerHighSeaReceiveTime=null|customerHighSeaReturnCount=0|notFollowDays=0|lastMasterUserId=null|transferToHighSeaReason=null|recentActivityRecordTime=2019-01-18 14:31:44|expireTime=null|locked=0|createUser=刘超|updateUse=刘超|master=刘超|masterOffice=员福三部|createTime=2019-01-18 14:26:56|updateTime=2019-01-18 14:31:44|CustomField_4807=15135241941^CustomField_4806=15135241941)

      //获得全部数据
      var mergeData: Array[String] = x._2.split("\\|")

      //获得数组长度
      var len = mergeData.length
      for (x <- 0 to len-2){
        var keyValue = mergeData(x).split("=")
        if(keyValue.length == 2) {
          if(keyValue(1) != null && !"null".equals(keyValue(1))){
            put.add(Bytes.toBytes(columnFamily1),Bytes.toBytes(keyValue(0)),Bytes.toBytes(keyValue(1)))
          }
        }
      }

      //用户自定义数据
      val field = mergeData(len-1).split("\\^")

      val fieldLen = field.length

      println(fieldLen)

      for(z <- 0 to fieldLen-1){
        val keyValue = field(z).split("=")
        println(keyValue(1))
        put.add(Bytes.toBytes(columnFamily2),Bytes.toBytes(keyValue(0)),Bytes.toBytes(keyValue(1)))
      }

      //转化成RDD[(ImmutableBytesWritable,Put)]类型才能调用saveAsHadoopDataset
      (new ImmutableBytesWritable, put)
    })

//    rdd.foreach(println)
    //使用saveAsHadoopDataSet算子将数据存储hbase
    rdd.saveAsHadoopDataset(jobConf)
  }

  /**
    * 获取baseInfo和可变字段的数据
    * @param baseInfo
    * @param customField
    * @return
    */
  def getMerge(baseInfo:Array[String],customField:Array[String]): Map[String, String] ={
    //定义可变map存储json结果
    var emptyMap: Map[String, String] = new scala.collection.immutable.HashMap[String, String]

    var i = 0
    var baseInfo_CustomField =  baseInfo.map(x => {
      var builder = new StringBuilder
      val spr = x.split("&")
      var key: String = spr(0)
      var value: String = spr(1)+customField(i)
      i += 1
      emptyMap += (key -> value)
    })

    emptyMap
  }
  /**
    * 获取可变字段函数
    * @param value
    * @return
    */
  def getCustomField(value : Array[AnyRef]): Array[String] ={
    var res1: Array[String] = value.map(x => {
      //解析json  list下的key-value
      var res = JSON.parseObject(JSON.parseObject(x.toString).toString).toString
      //存储可变字段key和value
      val builder = new StringBuilder
      //key
      var fieldCode = ""
      //value
      var context = ""
      //判断key中是否有新增字段，并进行解析，然后拼接成字符串
      if( (JSON.parseObject(res.toString).get("fieldMap").toString).length != 2){
        res = JSON.parseObject(res.toString).get("fieldMap").toString
        var re: JSONObject = JSON.parseObject(res.toString)
        var keys = re.keySet()
        val itr = keys.iterator()
        while(itr.hasNext()){
          var key = itr.next()
          context = JSON.parseObject(re.get(key).toString).get("content").toString
          builder.append(key+"="+context+"^")
        }
        builder.toString().substring(0,builder.toString().length-1)
      }else{
        builder.toString()
      }
    })
//    res1.foreach(println)
    res1
  }

  /**
    *  获取baseInfo数据
    * @param value
    */
  def getBaseInfo_Buss(value : Array[AnyRef]): Array[String] ={
    var baseInfo: Array[String] = value.map(x => {
      val id = JSON.parseObject(x.toString).get("id")//客户id
      val name = JSON.parseObject(x.toString).get("name")//客户名称
      val remark = JSON.parseObject(x.toString).get("remark")//备注
      val customerId = JSON.parseObject(x.toString).get("customerId")//客户组
      val saleProcess = JSON.parseObject(x.toString).get("saleProcess")//销售阶段
      val preSignDate = JSON.parseObject(x.toString).get("preSignDate")//结束时间
      val loseReason = JSON.parseObject(x.toString).get("loseReason")//输单原因
      val winRate = JSON.parseObject(x.toString).get("winRate")//赢率
      val createTime = JSON.parseObject(x.toString).get("createTime")//创建时间
      val updateTime = JSON.parseObject(x.toString).get("updateTime")//修改时间
      val changeTimestamp = JSON.parseObject(x.toString).get("changeTimestamp")//最近修改时间
      val recentActivityRecordTime = JSON.parseObject(x.toString).get("recentActivityRecordTime")//最新活动记录时间
      val customer_name = JSON.parseObject(JSON.parseObject(x.toString).get("customer").toString).get("name")
      val businessCategory_name = JSON.parseObject(JSON.parseObject(x.toString).get("businessCategory").toString).get("name")//商机类型
      val master_realname = JSON.parseObject(JSON.parseObject(x.toString).get("master").toString).get("realname")//所有人
      val createUser_realname = JSON.parseObject(JSON.parseObject(x.toString).get("createUser").toString).get("realname")//创建人
      val updateUser_realname = JSON.parseObject(JSON.parseObject(x.toString).get("updateUser").toString).get("realname")//最近修改人

      var test  = "id="+id+
        "&name=" +name+
        "|remark=" +remark+
        "|customerId=" +customerId+
        "|customer_name=" +customer_name+
        "|saleProcess=" +saleProcess+
        "|preSignDate=" +preSignDate+
        "|loseReason=" +loseReason+
        "|businessCategory_name=" +businessCategory_name+
        "|winRate=" +winRate+
        "|master_realname=" +master_realname+
        "|createUser_realname=" +createUser_realname+
        "|createTime=" +createTime+
        "|updateTime=" +updateTime+
        "|updateUser_realname=" +updateUser_realname+
        "|changeTimestamp=" +changeTimestamp+
        "|recentActivityRecordTime=" +recentActivityRecordTime+"|"

      test
    })
    baseInfo
  }

  /**
    * 对文件进行权限的设置
    * @param conf_fs hdfs文件配信息
    * @param stagingFolder 文件存储的目录
    */
  def proessFile(conf_fs: Configuration, stagingFolder: String): Unit
  = {
    val shell = new FsShell(conf_fs)
    shell.run(Array[String]("-chmod", "-R", "777", stagingFolder))

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



