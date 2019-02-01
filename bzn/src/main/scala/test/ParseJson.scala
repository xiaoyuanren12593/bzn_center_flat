package test

import java.util

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

object ParseJson {
  def main(args: Array[String]): Unit = {

    //程序用户名
    System.setProperty("HADOOP_USER_NAME", "root")

    //spark 自定义工具类
    val util = new Spark_Util

    val location_mysql_url = util.getValueByKey("/config_scala.properties","location_mysql_url")

    val hdfs_url = util.getValueByKey("/config_scala.properties","hdfs_url")

    val sc = util.sparkConf("ParseJson","local[2]")

    val file: Array[String] = sc.textFile("D:\\idea-poject\\bzn\\src\\main\\scala\\test_file\\cumsmer.json").collect()

    var json = file.mkString("")

    //获取三级目录
    var res = JSON.parseObject(JSON.parseObject(json).get("data").toString).get("list").toString

    val nObject: JSONArray = JSON.parseArray(res)

    val value: Array[AnyRef] = nObject.toArray()

    //获取基础字段信息baseInfo
    val baseInfo: Array[String]= getBaseInfo(value)

    //获取可变字段函数
    val customField: Array[String] = getCustomField(value)

    //合并baseInfo和可变字段数据
    var baseInfo_CustomField: Map[String, String] =  getMerge(baseInfo,customField)

    //将合并的数组转换成RDD
    var rdd1: RDD[(String, String)] = sc.parallelize(baseInfo_CustomField.toList)

    rdd1.foreach(println)
    //HBaseConf
    val conf = HbaseConf("crm_customer")._1
    val tableName = "crm_customer"
    val columnFamily1 = "baseInfo"
    val columnFamily2 = "customField"

    //存储到hbase中
    saveHbase(rdd1,sc,tableName,conf,columnFamily1,columnFamily2)

    sc.stop()
  }

  /**
    * 存储到hbase
    * @param sc
    * @param tablename
    * @param conf
    * @param columnFamily1
    */
  def saveHbaseStreaming(x:(String,String),sc : SparkContext,tablename:String,conf: Configuration,columnFamily1:String,columnFamily2:String) ={

    //初始化jobconf，TableOutputFormat必须是org.apache.hadoop.hbase.mapred包下的！
    val jobConf = new JobConf(conf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tablename)

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

    var rdd: RDD[(ImmutableBytesWritable, Put)] = indataRDD.map(x => {
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
      val spr = x.split("#")
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

          val sets: util.Set[String] = JSON.parseObject(re.get(key).toString).keySet()

          if(sets.size()==4){
            context = JSON.parseObject(re.get(key).toString).get("content").toString
            builder.append(key+"="+context+"^")
          }else{
            println(key+"  "+sets.size())
          }
        }

        if(builder.toString().length > 0){
          builder.toString().substring(0,builder.toString().length-1)
        }else{
          builder.toString()
        }
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
  def getBaseInfo(value : Array[AnyRef]): Array[String] ={
    var baseInfo: Array[String] = value.map(x => {
//      println(x)
      val id = JSON.parseObject(x.toString).get("id")//客户id
      val name = JSON.parseObject(x.toString).get("name")//客户名称
      val officeId = JSON.parseObject(x.toString).get("officeId")//部门id
      val telephone = JSON.parseObject(x.toString).get("telephone")//电话
      val mobile = JSON.parseObject(x.toString).get("mobile")//手机号
      val email = JSON.parseObject(x.toString).get("email")//邮箱
      val customerHighSeaId = JSON.parseObject(x.toString).get("customerHighSeaId")//所属公海id
      val address = JSON.parseObject(x.toString).get("address")//详细地址
      val classification = JSON.parseObject(x.toString).get("classification")//客户级别
      val province = JSON.parseObject(x.toString).get("province")//省份
      val city = JSON.parseObject(x.toString).get("city")//市
      val district = JSON.parseObject(x.toString).get("district")//地区
      val remark = JSON.parseObject(x.toString).get("remark")//备注
      //    val officeId = JSON.parseObject(res).get("officeId")//负责组
      val businessCategoryId = JSON.parseObject(x.toString).get("businessCategoryId")//客户类型
      val highSeaStatus = JSON.parseObject(x.toString).get("highSeaStatus")//公海状态
      val customerHighSeaReceiveTime = JSON.parseObject(x.toString).get("customerHighSeaReceiveTime")//领取时间
      val customerHighSeaReturnCount = JSON.parseObject(x.toString).get("customerHighSeaReturnCount")//退回次数
      val notFollowDays = JSON.parseObject(x.toString).get("notFollowDays")//未跟进天数
      val lastMasterUserId = JSON.parseObject(x.toString).get("lastMasterUserId")//最后所有人
      val transferToHighSeaReason = JSON.parseObject(x.toString).get("transferToHighSeaReason")//退回公海原因
      val recentActivityRecordTime = JSON.parseObject(x.toString).get("recentActivityRecordTime")//最新活动记录时间
      val expireTime = JSON.parseObject(x.toString).get("expireTime")//到期时间
      val locked = JSON.parseObject(x.toString).get("locked")//锁定状态
      val createUser = JSON.parseObject(JSON.parseObject(x.toString).get("createUser").toString).get("realname")//创建人
      var updateUser  = ""
      if(x.toString.contains("updateUser")){
        updateUser = JSON.parseObject(JSON.parseObject(x.toString).get("updateUser").toString).get("realname").toString//最近修改人
      }
      val master = JSON.parseObject(JSON.parseObject(x.toString).get("master").toString).get("realname")//客户所有人
      val masterOffice: AnyRef = JSON.parseObject(JSON.parseObject(x.toString).get("masterOffice").toString).get("name")//客户所属部门
      val createTime = JSON.parseObject(x.toString).get("createTime")//创建时间
      var updateTime = ""
      if(x.toString.contains("updateTime")){
        updateTime = JSON.parseObject(x.toString).get("updateTime").toString//最近修改时间
      }

      var test  = "id="+id+
        "#name=" +name+
        "|officeId=" +officeId+
        "|telephone=" +telephone+
        "|mobile=" +mobile+
        "|email=" +email+
        "|customerHighSeaId=" +customerHighSeaId+
        "|address=" +address+
        "|classification=" +classification+
        "|province=" +province+
        "|city=" +city+
        "|district=" +district+
        "|remark=" +remark+
        "|businessCategoryId=" +businessCategoryId+
        "|highSeaStatus=" +highSeaStatus+
        "|customerHighSeaReceiveTime=" +customerHighSeaReceiveTime+
        "|customerHighSeaReturnCount=" +customerHighSeaReturnCount+
        "|notFollowDays=" +notFollowDays+
        "|lastMasterUserId=" +lastMasterUserId+
        "|transferToHighSeaReason=" +transferToHighSeaReason+
        "|recentActivityRecordTime=" +recentActivityRecordTime+
        "|expireTime=" +expireTime+
        "|locked="+locked+
        "|createUser=" +createUser+
        "|updateUser=" +updateUser+
        "|master=" +master+
        "|masterOffice=" +masterOffice+
        "|createTime=" +createTime+
        "|updateTime=" +updateTime+"|"

//      println(test)
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




