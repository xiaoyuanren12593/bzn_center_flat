package bzn.c_person.centinfo

import java.util.Date

import bzn.c_person.util.SparkUtil
import bzn.job.common.Until
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FsShell, Path}
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, HTable, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles, TableInputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

/**
  * author:xiaoYuanRen
  * Date:2019/7/10
  * Time:13:39
  * describe: c端核心标签清洗
  **/
object CPersonCenInfoTest extends SparkUtil with Until{
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "local[*]")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    getCPersonCenInfoDetail(hiveContext)

//    val confinfo: (Configuration, Configuration) = HbaseConf("label_person")
//    saveToHbase(test, "", "", confinfo._2, "", confinfo._1)
    sc.stop()
  }

  /**
    * 计算个人核心标签的数据
    * @param sqlContext
    */
  def getCPersonCenInfoDetail(sqlContext:HiveContext) = {
    import sqlContext.implicits._
    /**
      * 读取投保人
      */
    val odsHolderDetail =
      sqlContext.sql("select holder_name,holder_cert_type,holder_cert_no from odsdb.ods_holder_detail")
      .where("holder_cert_type = 1 and holder_cert_no is not null")
      .distinct()
      .limit(10)

    /**
      * 读取被保人表
      */
    val odsPolicyInsuredDetail =
      sqlContext.sql("select insured_id,insured_name,policy_id,insured_cert_type,insured_cert_no,start_date,end_date " +
        "from odsdb.ods_policy_insured_detail")
      .where("insured_cert_type = '1' and insured_cert_no is not null")
      .distinct()
      .limit(10)

    /**
      * 读取从属被保人
      */
    val odsPolicyInsuredSlaveDetail =
      sqlContext.sql("select slave_name, master_id,slave_cert_type,slave_cert_no from odsdb.ods_policy_insured_slave_detail")
      .where("slave_cert_type = '1' and slave_cert_no is not null")
      .distinct()
      .limit(10)

    /**
      * 所有人员证件号union
      */
    val certNos =odsHolderDetail.selectExpr("holder_cert_no as cert_no")
      .unionAll(odsPolicyInsuredDetail.selectExpr("insured_cert_no as cert_no"))
      .unionAll(odsPolicyInsuredSlaveDetail.selectExpr("slave_cert_no as cert_no"))
      .distinct()

//    odsHolderDetail.join(odsPolicyInsuredDetail)

    /**
      * 读取产品表
      */
    val odsProductDetail = sqlContext.sql("select product_code as product_code_slave,product_name,one_level_pdt_cate from odsdb.ods_product_detail")

    /**
      * 读取保单数据
      */
    val odsPolicyDetail = sqlContext.sql("select holder_name as holder_name_slave,policy_id,policy_code,policy_status,product_code,product_name," +
      "first_premium,sum_premium,sku_coverage,policy_start_date,policy_end_date,channel_id,channel_name,policy_create_time,payment_type,belongs_regional," +
      "insure_company_name from odsdb.ods_policy_detail")
      .where("policy_status in (0,1,-1)")

    val holderPolicy = odsHolderDetail.join(odsPolicyDetail,odsHolderDetail("holder_name")===odsPolicyDetail("holder_name_slave"))
      .selectExpr("policy_id","policy_code","holder_name","holder_cert_no","policy_status","product_code","product_name","first_premium","sum_premium",
        "sku_coverage","policy_start_date","policy_end_date","channel_id","channel_name","policy_create_time","payment_type","belongs_regional",
        "insure_company_name")
    /*
    policy_id: string (nullable = true)
    policy_code: string (nullable = true)
    holder_name: string (nullable = true)
    holder_cert_no: string (nullable = true)
    policy_status: integer (nullable = true)
    product_code: string (nullable = true)
    product_name: string (nullable = true)
    first_premium: double (nullable = true)
    sum_premium: double (nullable = true)
    sku_coverage: string (nullable = true)
    policy_start_date: timestamp (nullable = true
    policy_end_date: timestamp (nullable = true)
    channel_id: string (nullable = true)
    channel_name: string (nullable = true)
     */

    /**
      * 首次投保时间，首次投保产品，首次投保产品code，首次投保方案，首次投保保障期间，首次投保支付方式，首次投保年龄，首次投保城市，首次投保省份，首次投保保险公司
      */
    val newResOne = holderPolicy.map(x => {
      val policyId = x.getAs[String]("policy_id")
      val policyCode = x.getAs[String]("policy_code")
      val holderName = x.getAs[String]("holder_name")
      val holderCert_no = x.getAs[String]("holder_cert_no")
      val policyStatus = x.getAs[Int]("policy_status")
      val productCode = x.getAs[String]("product_code")
      val productName = x.getAs[String]("product_name")
      val firstPremium = x.getAs[Double]("first_premium")
      val sumPremium = x.getAs[Double]("sum_premium")
      val skuCoverage = x.getAs[String]("sku_coverage")
      val policyStartDate = x.getAs[java.sql.Timestamp]("policy_start_date")
      val policyEndDate = x.getAs[java.sql.Timestamp]("policy_end_date")
      val channelId = x.getAs[String]("channel_id")
      val channelName = x.getAs[String]("channel_name")
      val policyCreateTime = x.getAs[java.sql.Timestamp]("policy_create_time")
      val paymentType = x.getAs[Int]("payment_type")
      val belongsRegional  = x.getAs[String]("belongs_regional ")
      val insureCompanyName  = x.getAs[String]("insure_company_name ")
      var province = ""
      var city = ""
      if(belongsRegional != null){
        if(belongsRegional.toString.length == 6){
          province = belongsRegional.substring(0,2)+"0000"
          city = belongsRegional.substring(0,4)+"00"
        }else{
          province = null
          city = null
        }
      }else{
        province = null
        city = null
      }
      (holderCert_no,(policyId,policyCode,holderName,policyStatus,productCode,productName,firstPremium,sumPremium,skuCoverage,
        policyStartDate,policyEndDate,channelId,channelName,policyCreateTime,paymentType,belongsRegional,insureCompanyName))
    })
      .reduceByKey((x1,x2)=> {
        var res = if(x1._11.compareTo(x2._11) >= 0) x1 else x2
        res
      })
      .map(x => {
        val cert_no = x._1
        val policy_start_date = x._2._9
        val age: Int = getAgeFromBirthTime(cert_no,policy_start_date)
        (x._1,x._2._1,x._2._2,x._2._3,x._2._4,x._2._5,x._2._6,x._2._7,x._2._8,x._2._9,x._2._10,x._2._11,x._2._12,x._2._13,x._2._14,x._2._15,
          x._2._16,x._2._17,age)
      })
      .toDF("holder_cert_no","policy_id","policy_code","holder_name","policy_status","product_code","product_name","first_premium","sum_premium",
        "sku_coverage","policy_start_date","policy_end_date","channel_id","channel_name","policy_create_time","payment_type","belongs_regional",
        "insure_company_name","age")

//    newResOne.join()
    newResOne.printSchema()
    holderPolicy.printSchema()

  }

  //得到个人标签数据
  def getHbaseBussValue(sc: SparkContext): RDD[(ImmutableBytesWritable, Result)] = {
    //定义HBase的配置
    val conf: Configuration = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "172.16.11.106,172.16.11.105,172.16.11.103")
    conf.set("mapreduce.task.timeout", "1200000")
    conf.set("hbase.client.scanner.timeout.period", "600000")
    conf.set("hbase.rpc.timeout", "600000")
    conf.setInt("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily", 3000)

    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, "label_person")

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
    conf.setInt("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily", 3000)
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
  def saveToHbase(result: RDD[(String, String, String)], columnFamily1: String, column: String,
                  conf_fs: Configuration, tableName: String, conf: Configuration): Unit = {
    val stagingFolder = s"/hbasehfile/$columnFamily1/$column"
    //创建hbase的链接,利用默认的配置文件,实际上读取的hbase的master地址
    val hdfs = FileSystem.get(conf_fs)
    val path = new Path(stagingFolder)

    //检查是否存在
    if (!hdfs.exists(path)) {
      //不存在就执行存储
      deToHbase(result,columnFamily1,conf_fs,conf,stagingFolder)
    } else if (hdfs.exists(path)) {
      //存在即删除后执行存储
      deleteFile(conf_fs, stagingFolder)
      deToHbase(result,columnFamily1,conf_fs,conf,stagingFolder)
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
  def deToHbase(result: RDD[(String, String, String)],columnFamily1: String, conf_fs: Configuration,conf: Configuration,stagingFolder:String) {
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
    val tableName = "label_person"

    //创建hbase连接
    val conn = ConnectionFactory.createConnection(conf)

    //得到表数据
    val table = conn.getTable(TableName.valueOf(tableName))

    println(table.getName)
    println(table.getTableDescriptor)

    try {
      //创建一个job
      lazy val job = Job.getInstance(conf)
      job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
      job.setMapOutputValueClass(classOf[KeyValue])

      HFileOutputFormat2.configureIncrementalLoadMap(job,table)

      val load = new LoadIncrementalHFiles(conf)
      println(load)

//      //保存一个文件
//      sourceRDD.saveAsNewAPIHadoopFile(stagingFolder,
//        classOf[ImmutableBytesWritable],
//        classOf[KeyValue],
//        classOf[HFileOutputFormat2],
//        job.getConfiguration()
//      )
//
//      //权限设置
//      proessFile(conf_fs, stagingFolder + "/*")
//
//      //开始导入
//      load.doBulkLoad(new Path(stagingFolder), table.asInstanceOf[HTable])

    }finally {
      //关闭连接
      table.close()
      conn.close()
    }
  }
}
