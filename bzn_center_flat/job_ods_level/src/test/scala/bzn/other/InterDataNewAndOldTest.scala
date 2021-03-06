package bzn.other

import java.sql.{Connection, DriverManager}
import java.util.Properties

import bzn.job.common.Until
import bzn.util.SparkUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * author:xiaoYuanRen
  * Date:2019/6/18
  * Time:16:01
  * describe: 对接口数据进行处理
  **/
object InterDataNewAndOldTest extends SparkUtil with Until{
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName,"local[*]")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    val res = interDataNewAndOld(hiveContext)
//    res.write.mode(SaveMode.Overwrite).saveAsTable("odsdb_prd.inter_every_month_new_old_detail")
    sc.stop()
  }

  /**
    * 处理接口数据，得到新客 老客
    * @param sqlContext
    */
  def interDataNewAndOld(sqlContext:HiveContext) ={
   import sqlContext.implicits._
   val openOtherPolicy = readMysqlTable(sqlContext,"open_other_policy")
    .selectExpr("insured_cert_no","product_code","month")
    .where("month >= '2019-01-01' and month is not null")
    .map(x => {
      val insuredCertNo = x.getAs[String]("insured_cert_no")
      val productCode = x.getAs[String]("product_code")
      var month = x.getAs[java.sql.Date]("month").toString.substring(0,7).replaceAll("-","").toInt
      (insuredCertNo,productCode,month)
    })
    .distinct()
    .toDF("insured_cert_no","product_code","month")
    .cache()
    //每个月新客 老客
    val one = openOtherPolicy.where("month < 201902").cache
    val two = openOtherPolicy.where("month < 201903").cache
    val three = openOtherPolicy.where("month < 201904").cache
    val four = openOtherPolicy.where("month < 201905").cache
    val five = openOtherPolicy.where("month < 201906").cache
    val six = openOtherPolicy.where("month < 201907").cache

    /**
      * 一月新客
      */
    val oneNew = one.map(x => {
      val insuredCertNo = x.getAs[String]("insured_cert_no")
      val productCode = x.getAs[String]("product_code")
      val month = x.getAs[Int]("month")
      (month,insuredCertNo,productCode,1)
    })
    .distinct()
    .toDF("month","insured_cert_no","product_code","cumm_type")

    /**
      * 二月新老客
      */
    val twoTemp = two.where("month = 201902")
    val oneTemp = one.selectExpr("insured_cert_no as insured_cert_no_temp").distinct
    val twoRes = twoTemp.join(oneTemp,twoTemp("insured_cert_no")===oneTemp("insured_cert_no_temp"),"leftouter")
      .map(x => {
        val insuredCertNo = x.getAs[String]("insured_cert_no")
        val productCode = x.getAs[String]("product_code")
        val insuredCertNoTemp = x.getAs[String]("insured_cert_no_temp")
        val month = x.getAs[Int]("month")
        var cumm_type = 1
        if(insuredCertNoTemp != null){
          cumm_type = 2
        }
        (month,insuredCertNo,productCode,cumm_type)
      })
      .toDF("month","insured_cert_no","product_code","cumm_type")

    /**
      * 三月新老客
      */
    val threeTemp = three.where("month = 201903")
    val twoTempTemp = two.selectExpr("insured_cert_no as insured_cert_no_temp").distinct
    val threeRes = threeTemp.join(twoTempTemp,threeTemp("insured_cert_no")===twoTempTemp("insured_cert_no_temp"),"leftouter")
      .map(x => {
        val insuredCertNo = x.getAs[String]("insured_cert_no")
        val productCode = x.getAs[String]("product_code")
        val insuredCertNoTemp = x.getAs[String]("insured_cert_no_temp")
        val month = x.getAs[Int]("month")
        var cumm_type = 1
        if(insuredCertNoTemp != null){
          cumm_type = 2
        }
        (month,insuredCertNo,productCode,cumm_type)
      })
      .toDF("month","insured_cert_no","product_code","cumm_type")

    /**
      * 四月新老客
      */
    val fourTemp = four.where("month = 201904")
    val threeTempTemp = three.selectExpr("insured_cert_no as insured_cert_no_temp").distinct
    val fourRes = fourTemp.join(threeTempTemp,fourTemp("insured_cert_no")===threeTempTemp("insured_cert_no_temp"),"leftouter")
      .map(x => {
        val insuredCertNo = x.getAs[String]("insured_cert_no")
        val productCode = x.getAs[String]("product_code")
        val insuredCertNoTemp = x.getAs[String]("insured_cert_no_temp")
        val month = x.getAs[Int]("month")
        var cumm_type = 1
        if(insuredCertNoTemp != null){
          cumm_type = 2
        }
        (month,insuredCertNo,productCode,cumm_type)
      })
      .toDF("month","insured_cert_no","product_code","cumm_type")

    /**
      * 五月新老客
      */
    val fiveTemp = five.where("month = 201905")
    val fourTempTemp = four.selectExpr("insured_cert_no as insured_cert_no_temp").distinct
    val fiveRes = fiveTemp.join(fourTempTemp,fiveTemp("insured_cert_no")===fourTempTemp("insured_cert_no_temp"),"leftouter")
      .map(x => {
        val insuredCertNo = x.getAs[String]("insured_cert_no")
        val productCode = x.getAs[String]("product_code")
        val insuredCertNoTemp = x.getAs[String]("insured_cert_no_temp")
        val month = x.getAs[Int]("month")
        var cumm_type = 1
        if(insuredCertNoTemp != null){
          cumm_type = 2
        }
        (month,insuredCertNo,productCode,cumm_type)
      })
      .toDF("month","insured_cert_no","product_code","cumm_type")

    /**
      * 六月新老客
      */
    val sixTemp =six.where("month = 201906")
    val fiveTempTemp = five.selectExpr("insured_cert_no as insured_cert_no_temp").distinct
    val sixRes = sixTemp.join(fiveTempTemp,sixTemp("insured_cert_no")===fiveTempTemp("insured_cert_no_temp"),"leftouter")
      .map(x => {
        val insuredCertNo = x.getAs[String]("insured_cert_no")
        val productCode = x.getAs[String]("product_code")
        val insuredCertNoTemp = x.getAs[String]("insured_cert_no_temp")
        val month = x.getAs[Int]("month")
        var cumm_type = 1
        if(insuredCertNoTemp != null){
          cumm_type = 2
        }
        (month,insuredCertNo,productCode,cumm_type)
      })
      .toDF("month","insured_cert_no","product_code","cumm_type")

    val res = oneNew.unionAll(twoRes).unionAll(threeRes).unionAll(fourRes).unionAll(fiveRes).unionAll(sixRes)

    res.printSchema()
  }


  def readUserHbase(sqlContext:HiveContext,sc:SparkContext) = {
    import sqlContext.implicits._
    sqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    sqlContext.udf.register("lengthStr",(str:String) => {
      var strRes = ""
      if(str!=null){
        if(str.length > 11){
          strRes = str.substring(str.length-11)
        }else{
          strRes = str
        }
      }else{
        strRes = null
      }
      strRes
    })
//    val conf = HbaseConf("labels:label_user_personal")._1
//    val conf_fs = HbaseConf("labels:label_user_personal")._2
//    val tableName = "labels:label_user_personal"
//    val columnFamily1 = "baseinfo"
    val bussValue = getHbaseBussValue(sc)
    val bussValueTemp: DataFrame = bussValue.map(x => x._2).map(x => {
      //rowkey
      val key = Bytes.toString(x.getRow)
      var strRes = ""
      if(key!=null){
        if(key.length > 11){
          strRes = key.substring(key.length-11)
        }else{
          strRes = key
        }
      }else{
        strRes = null
      }
      strRes
      val user_name=Bytes.toString(x.getValue("baseinfo".getBytes,"user_name".getBytes))
      val user_sex=Bytes.toString(x.getValue("baseinfo".getBytes,"user_sex".getBytes))
      val user_age=Bytes.toString(x.getValue("baseinfo".getBytes,"user_age".getBytes))
      val user_native=Bytes.toString(x.getValue("baseinfo".getBytes,"user_native".getBytes))
      val user_constellation=Bytes.toString(x.getValue("baseinfo".getBytes,"user_constellation".getBytes))
      val user_identity=Bytes.toString(x.getValue("baseinfo".getBytes,"user_identity".getBytes))
      val user_induster=Bytes.toString(x.getValue("baseinfo".getBytes,"user_induster".getBytes))
      val user_company=Bytes.toString(x.getValue("baseinfo".getBytes,"user_company".getBytes))
      val user_income=Bytes.toString(x.getValue("baseinfo".getBytes,"user_income".getBytes))
      val user_province=Bytes.toString(x.getValue("baseinfo".getBytes,"user_province".getBytes))
      val user_city=Bytes.toString(x.getValue("baseinfo".getBytes,"user_city".getBytes))
      val user_city_grade=Bytes.toString(x.getValue("baseinfo".getBytes,"user_city_grade".getBytes))
      val user_child=Bytes.toString(x.getValue("baseinfo".getBytes,"user_child".getBytes))
      val user_marry=Bytes.toString(x.getValue("baseinfo".getBytes,"user_marry".getBytes))
      val user_accu_insure_singular=Bytes.toString(x.getValue("insureinfo".getBytes,"user_accu_insure_singular".getBytes))
      val user_now_efficient_singula=Bytes.toString(x.getValue("insureinfo".getBytes,"user_now_efficient_singular".getBytes))
      val user_accu_insure_money=Bytes.toString(x.getValue("insureinfo".getBytes,"user_accu_insure_money".getBytes))
      val user_mean_insure_money=Bytes.toString(x.getValue("insureinfo".getBytes,"user_mean_insure_money".getBytes))
      val user_year_insure_money=Bytes.toString(x.getValue("insureinfo".getBytes,"user_year_insure_money".getBytes))
      val user_fist_now_month=Bytes.toString(x.getValue("insureinfo".getBytes,"user_fist_now_month".getBytes))
      val user_fist_insure_product=Bytes.toString(x.getValue("insureinfo".getBytes,"user_fist_insure_product".getBytes))
      val user_insure_product_num=Bytes.toString(x.getValue("insureinfo".getBytes,"user_insure_product_num".getBytes))
      val user_insure_coverage=Bytes.toString(x.getValue("insureinfo".getBytes,"user_insure_coverage".getBytes))
      val user_source=Bytes.toString(x.getValue("insureinfo".getBytes,"user_source".getBytes))
      val user_last_insure_date=Bytes.toString(x.getValue("insureinfo".getBytes,"user_last_insure_date".getBytes))
      val user_last_date=Bytes.toString(x.getValue("insureinfo".getBytes,"user_last_date".getBytes))
      val user_craft_level=Bytes.toString(x.getValue("baseinfo".getBytes,"user_craft_level".getBytes))
      val user_night_bird=Bytes.toString(x.getValue("goout".getBytes,"user_night_bird".getBytes))
      val user_early_bird=Bytes.toString(x.getValue("goout".getBytes,"user_early_bird".getBytes))
      val user_most_riding_date=Bytes.toString(x.getValue("goout".getBytes,"user_most_riding_date".getBytes))
      val user_riding_brand=Bytes.toString(x.getValue("goout".getBytes,"user_riding_brand".getBytes))
      val user_riding_type=Bytes.toString(x.getValue("goout".getBytes,"user_riding_type".getBytes))
      val user_count_cycling_days=Bytes.toString(x.getValue("goout".getBytes,"user_count_cycling_days".getBytes))
      val user_cycling_frequency=Bytes.toString(x.getValue("goout".getBytes,"user_cycling_frequency".getBytes))
      val user_2_hours_quantum=Bytes.toString(x.getValue("goout".getBytes,"user_2_hours_quantum".getBytes))
      val user_on_worker=Bytes.toString(x.getValue("goout".getBytes,"user_on_worker".getBytes))
      val user_badminton=Bytes.toString(x.getValue("goout".getBytes,"user_badminton".getBytes))
      val user_swimming=Bytes.toString(x.getValue("goout".getBytes,"user_swimming".getBytes))
      val user_skiing=Bytes.toString(x.getValue("goout".getBytes,"user_skiing".getBytes))
      val user_triathlon=Bytes.toString(x.getValue("goout".getBytes,"user_triathlon".getBytes))
      val user_water_entertainment=Bytes.toString(x.getValue("goout".getBytes,"user_water_entertainment".getBytes))
      val user_competition=Bytes.toString(x.getValue("goout".getBytes,"user_competition".getBytes))
      val user_marathon=Bytes.toString(x.getValue("goout".getBytes,"user_marathon".getBytes))
      val user_skating=Bytes.toString(x.getValue("goout".getBytes,"user_skating".getBytes))
      val user_footwear=Bytes.toString(x.getValue("goout".getBytes,"user_footwear".getBytes))
      val user_riding=Bytes.toString(x.getValue("goout".getBytes,"user_riding".getBytes))
      val user_insure_code=Bytes.toString(x.getValue("baseinfo".getBytes,"user_insure_code".getBytes))
      val report_count=Bytes.toString(x.getValue("claiminfo".getBytes,"report_count".getBytes))
      val claim_count=Bytes.toString(x.getValue("claiminfo".getBytes,"claim_count".getBytes))
      val death_count=Bytes.toString(x.getValue("claiminfo".getBytes,"death_count".getBytes))
      val disability_count=Bytes.toString(x.getValue("claiminfo".getBytes,"disability_count".getBytes))
      val case_work_count=Bytes.toString(x.getValue("claiminfo".getBytes,"case_work_count".getBytes))
      val case_notwork_count=Bytes.toString(x.getValue("claiminfo".getBytes,"case_notwork_count".getBytes))
      val prepay_total=Bytes.toString(x.getValue("claiminfo".getBytes,"prepay_total".getBytes))
      val prepay_death=Bytes.toString(x.getValue("claiminfo".getBytes,"prepay_death".getBytes))
      val prepay_disability=Bytes.toString(x.getValue("claiminfo".getBytes,"prepay_disability".getBytes))
      val prepay_work=Bytes.toString(x.getValue("claiminfo".getBytes,"prepay_work".getBytes))
      val prepay_notwork=Bytes.toString(x.getValue("claiminfo".getBytes,"prepay_notwork".getBytes))
      val finalpay_total=Bytes.toString(x.getValue("claiminfo".getBytes,"finalpay_total".getBytes))
      val avg_prepay=Bytes.toString(x.getValue("claiminfo".getBytes,"avg_prepay".getBytes))
      val avg_finalpay=Bytes.toString(x.getValue("claiminfo".getBytes,"avg_finalpay".getBytes))
      val case_overtime_count=Bytes.toString(x.getValue("claiminfo".getBytes,"case_overtime_count".getBytes))
      val avg_effecttime=Bytes.toString(x.getValue("claiminfo".getBytes,"avg_effecttime".getBytes))
      (strRes,user_name+","+user_sex+","+user_age+","+user_native+","+user_constellation+","+user_identity+","+user_induster+","+user_company+","+user_income+","+user_province+","+user_city+","+user_city_grade+","+user_child+","+user_marry+","+user_accu_insure_singular+","+user_now_efficient_singula+","+user_accu_insure_money+","+user_mean_insure_money+","+user_year_insure_money+","+user_fist_now_month+","+user_fist_insure_product+","+user_insure_product_num+","+user_insure_coverage+","+user_source+","+user_last_insure_date+","+user_last_date+","+user_craft_level+","+user_night_bird+","+user_early_bird+","+user_most_riding_date+","+user_riding_brand+","+user_riding_type+","+user_count_cycling_days+","+user_cycling_frequency+","+user_2_hours_quantum+","+user_on_worker+","+user_badminton+","+user_swimming+","+user_skiing+","+user_triathlon+","+user_water_entertainment+","+user_competition+","+user_marathon+","+user_skating+","+user_footwear+","+user_riding+","+user_insure_code+","+report_count+","+claim_count+","+death_count+","+disability_count+","+case_work_count+","+case_notwork_count+","+prepay_total+","+prepay_death+","+prepay_disability+","+prepay_work+","+prepay_notwork+","+finalpay_total+","+avg_prepay+","+avg_finalpay+","+case_overtime_count+","+avg_effecttime)
    })
     .toDF("strRes","value")

    val resMobile =  readMysqlTable(sqlContext, "mobile_tmp")
      .cache()

    val mobile_holder = resMobile
      .selectExpr("mobile","type")
      .distinct()

    val res_holder = mobile_holder.join(bussValueTemp,mobile_holder("mobile")===bussValueTemp("strRes"),"leftouter")
      .selectExpr("mobile","type","value")
    res_holder.printSchema()
//    res_holder.insertInto("odsdb_prd.user_hbase_res",true)
//    val mobile_holder = readMysqlTable(sqlContext, "mobile_tmp")
//      .where("type = '1'")
//      .distinct()
//      .selectExpr("mobile")
//    val res_holder = mobile_holder.join(bussValueTemp,mobile_holder("mobile")===bussValueTemp("strRes"))
//
//    val mobile_insured = readMysqlTable(sqlContext, "mobile_tmp")
//      .where("type = '2'")
//      .distinct()
//      .selectExpr("mobile")
//    val res_insured = mobile_insured.join(bussValueTemp,mobile_insured("mobile")===bussValueTemp("strRes"))
//
//    val mobile58_cumm = readMysqlTable(sqlContext, "mobile_tmp")
//      .where("type = '3'")
//      .distinct()
//      .selectExpr("mobile")
//    val res_cumm= mobile58_cumm.join(bussValueTemp,mobile58_cumm("mobile")===bussValueTemp("strRes"))
//
//    val mobile58_car = readMysqlTable(sqlContext, "mobile_tmp")
//      .where("type = '4'")
//      .distinct()
//      .selectExpr("mobile")
//    val res_car= mobile58_car.join(bussValueTemp,mobile58_car("mobile")===bussValueTemp("strRes"))
//
//    val mobile_share = readMysqlTable(sqlContext, "mobile_tmp")
//      .where("type = '5'")
//      .distinct()
//      .selectExpr("mobile")
//    val res_share= mobile_share.join(bussValueTemp,mobile_share("mobile")===bussValueTemp("strRes"))
//
//    saveASMysqlTable(res_holder,"user_hbase_sport_holder",SaveMode.Overwrite)
//    saveASMysqlTable(res_insured,"user_hbase_sport_insured",SaveMode.Overwrite)
//    saveASMysqlTable(res_cumm,"user_hbase_sport_58_car",SaveMode.Overwrite)
//    saveASMysqlTable(res_car,"user_hbase_sport_58_cumm",SaveMode.Overwrite)
//    saveASMysqlTable(res_share,"user_hbase_sport_share",SaveMode.Overwrite)
  }

  //得到个人标签数据
  def getHbaseBussValue(sc: SparkContext): RDD[(ImmutableBytesWritable, Result)] = {
    //定义HBase的配置
    val conf: Configuration = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "172.16.11.106")
    conf.set("mapreduce.task.timeout", "1200000")
    conf.set("hbase.client.scanner.timeout.period", "600000")
    conf.set("hbase.rpc.timeout", "600000")
    conf.setInt("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily", 3000)

    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, "labels:label_user_person")

    val usersRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
      conf,
      classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result]
    )
    usersRDD
  }
  /**
    * 将DataFrame保存为Mysql表
    *
    * @param dataFrame 需要保存的dataFrame
    * @param tableName 保存的mysql 表名
    * @param saveMode  保存的模式 ：Append、Overwrite、ErrorIfExists、Ignore
    */
  def saveASMysqlTable(dataFrame: DataFrame, tableName: String, saveMode: SaveMode) = {
    var table = tableName
    val properties: Properties = getProPerties()
    val prop = new Properties //配置文件中的key 与 spark 中的 key 不同 所以 创建prop 按照spark 的格式 进行配置数据库
    prop.setProperty("user", properties.getProperty("mysql.username"))
    prop.setProperty("password", properties.getProperty("mysql.password"))
    prop.setProperty("driver", properties.getProperty("mysql.driver"))
    prop.setProperty("url", properties.getProperty("mysql.url"))
    if (saveMode == SaveMode.Overwrite) {
      var conn: Connection = null
      try {
        conn = DriverManager.getConnection(
          prop.getProperty("url"),
          prop.getProperty("user"),
          prop.getProperty("password")
        )
        val stmt = conn.createStatement
        table = table.toLowerCase
        stmt.execute(s"truncate table $table") //为了不删除表结构，先truncate 再Append
        conn.close()
      }
      catch {
        case e: Exception =>
          println("MySQL Error:")
          e.printStackTrace()
      }
    }
    dataFrame.write.mode(SaveMode.Append).jdbc(prop.getProperty("url"), table, prop)
  }

  /**
    * 获取 Mysql 表的数据
    *
    * @param sqlContext
    * @param tableName 读取Mysql表的名字
    * @return 返回 Mysql 表的 DataFrame
    */
  def readMysqlTable(sqlContext: SQLContext, tableName: String) = {
    val properties: Properties = getProPerties()
    sqlContext
      .read
      .format("jdbc")
      .option("url", properties.getProperty("mysql.url.103"))
      .option("driver", properties.getProperty("mysql.driver"))
      .option("user", properties.getProperty("mysql.username.103"))
      .option("password", properties.getProperty("mysql.password.103"))
      //        .option("dbtable", tableName.toUpperCase)
      .option("numPartitions","30")
      .option("partitionColumn","policy_id")
      .option("lowerBound", "0")
      .option("upperBound","20000")
      .option("dbtable", tableName)
      .load()

  }

  /**
    * 获取配置文件
    * @return
    */
  def getProPerties() = {
    val lines_source = Source.fromURL(getClass.getResource("/config_scala.properties")).getLines.toSeq
    var properties: Properties = new Properties()
    for (elem <- lines_source) {
      val split = elem.split("==")
      val key = split(0)
      val value = split(1)
      properties.setProperty(key,value)
    }
    properties
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
