package bzn.ods.policy

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import bzn.job.common.Until
import bzn.ods.util.SparkUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.hive.HiveContext

import scala.io.Source
import scala.math.BigDecimal.RoundingMode

/**
  * author:xiaoYuanRen
  * Date:2019/6/19
  * Time:14:58
  * describe: 接口和婚礼纪的数据
  **/
object OdsInterAndWeddingPremiumDetailTest extends SparkUtil with Until{
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName,"local[*]")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    getInterAndWeddingPremiumDEtail(hiveContext)
    sc.stop()
  }

  /**
    * 接口和婚礼纪的数据
    * @param sqlContext
    */
  def getInterAndWeddingPremiumDEtail(sqlContext:HiveContext) ={
    import sqlContext.implicits._
    sqlContext.udf.register("clean", (str: String) => clean(str))
    sqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    sqlContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")//设置日期格式
      val date = df.format(new Date())// new Date()为获取当前系统时间
      (date + "")
    })

    /**
      * 计算接口的数据
      */
    val interDate = readMysqlTable(sqlContext,"open_stat_policy_amount_bzndate")
      .selectExpr("id","product_code","product_name","amount","premium","statistical_date","create_time","update_time")
      .map(x => {
        val id = x.getAs[Int]("id")
        val productCode = x.getAs[String]("product_code")
        val productName = x.getAs[String]("product_name")
        val amount = x.getAs[Int]("amount")
        var premium = x.getAs[java.math.BigDecimal]("premium")
        var premiumDecimal = BigDecimal(0.0000).doubleValue()
        if(premium!=null){
          premiumDecimal = premium.doubleValue()
        }
        var statisticalDate = x.getAs[java.sql.Date]("statistical_date")
        var day_id = ""
        if(statisticalDate != null){
          day_id = x.getAs[java.sql.Date]("statistical_date").toString.replaceAll("-","")
        }else{
          day_id = null
        }
        ((day_id,productCode),(id,productName,amount,premiumDecimal))
      })
      .reduceByKey((x1,x2) => {
        val maxId = if(x1._1>x2._1) x1 else x2
        maxId
      })
      .map(x => {
        var product_code = x._1._2
        var product_name = x._2._2
        var premium_type = 1
        var amount = x._2._3
        var sum_premium = x._2._4
        var day_id = x._1._1
        val premium_res = updataInter(product_code,amount,sum_premium)
        (product_code,product_name,premium_type,amount,premium_res,day_id)
      })
      .toDF("product_code","product_name","premium_type","policy_count","sum_premium","day_id")

    /**
      * 婚礼纪数据
      */
    val weddingDate = readMysqlTable(sqlContext,"open_policy_bznapi")
      .selectExpr("apply_date","product_code","channel_name","status","apply_num","total_actual_premium")
        .where("status = 1")
      .map(x => {
        var applyDate = x.getAs[Timestamp]("apply_date")
        var day_id = ""
        if(applyDate!= null){
          day_id = applyDate.toString.substring(0,10).replaceAll("-","")
        }
        val productCode = x.getAs[String]("product_code")
        val productName = x.getAs[String]("channel_name")
        val amount = x.getAs[Int]("apply_num") //人数
        var premium = x.getAs[Double]("total_actual_premium")//钱
        if(premium == null){
          premium = 0.0
        }
        val premiumDecimal = BigDecimal.apply(premium).setScale(4,BigDecimal.RoundingMode.HALF_UP)
        ((day_id,productCode,productName),(amount,premiumDecimal))
      })
      .reduceByKey((x1,x2) => {
        val amount = x1._1+x2._1
        val sum_premium = x1._2.+(x2._2).setScale(4,BigDecimal.RoundingMode.HALF_UP)
        (amount,sum_premium)
      })
      .map(x => {
        (x._1._2,x._1._3,1,x._2._1,x._2._2.doubleValue(),x._1._1)
      })
      .toDF("product_code","product_name","premium_type","policy_count","sum_premium","day_id")

    val unionRes  = interDate.unionAll(weddingDate)
      .selectExpr("product_code","product_name","premium_type","policy_count","sum_premium","day_id")

    /**
      * 读取产品维度表
      */
    val odsProductDetail = sqlContext.sql("select product_code as product_code_slave,one_level_pdt_cate from odsdb.ods_product_detail")

    val res = unionRes.join(odsProductDetail,unionRes("product_code")===odsProductDetail("product_code_slave"))
      .selectExpr("getUUID() as id","clean(product_code) as product_code","clean(product_name) as product_name","clean(one_level_pdt_cate) as one_level_pdt_cate",
        "premium_type","policy_count", "cast(sum_premium as decimal(14,4)) as sum_premium","day_id","getNow() as dw_create_time")

    res.show(1000)
    res.printSchema()
  }
//  root
//  |-- id: string (nullable = true)
//  |-- product_code: string (nullable = true)
//  |-- product_name: string (nullable = true)
//  |-- one_level_pdt_cate: string (nullable = true)
//  |-- premium_type: integer (nullable = false)
//  |-- amount: integer (nullable = false)
//  |-- sum_premium: double (nullable = true)
//  |-- day_id: string (nullable = true)
//  |-- dw_create_time: string (nullable = true)

  /**
    * 更新接口保费
    */
  def updataInter(product_code:String,policy_cnt:Int,sum_premium:Double): Double ={
    var sum_premium_res = if(product_code=="OFO00001"){policy_cnt*0.02}
    else if(product_code=="OFO00002"){policy_cnt*0.04}
    else if(product_code.contains("BZN_QJDC_001") == true){policy_cnt*0.06}
    else if(product_code.contains("BZN_QJDC_002") == true){policy_cnt*0.12}
    else if(product_code.contains("BZN_DDBIKE") == true){policy_cnt*0.08}
    else if(product_code.contains("BZN_SCBIKE") == true){policy_cnt*0.05}
    else if(product_code.contains("9900") == true){policy_cnt*0.5}
    else if(product_code=="BZN02DM001"){policy_cnt*0.2}
    else if(product_code=="BZN02DM301"){policy_cnt*0.4}
    else if(product_code=="BZN04DM002"){policy_cnt*0.4}
    else if(product_code=="BZN04DM302"){policy_cnt*0.6}
    else if(product_code=="BZN06DM003"){policy_cnt*0.6}
    else if(product_code=="BZN06DM303"){policy_cnt*0.8}
    else if(product_code=="BZN08DM004"){policy_cnt*0.8}
    else if(product_code=="BZN08DM304"){policy_cnt*1.0}
    else if(product_code=="BZN09DM005"){policy_cnt*0.9}
    else if(product_code=="BZN09DM305"){policy_cnt*1.1}
    else if(product_code=="BZN04ZHR001"){policy_cnt*0.4}
    else if(product_code=="BZN06ZHR002"){policy_cnt*0.6}
    else if(product_code=="BZN08ZHR003"){policy_cnt*0.8}
    else if(product_code.contains("BDWM") == true){policy_cnt*1.2}
    else if(product_code.contains("BZN_EXPRESS") == true){policy_cnt*0.24}
    else if(product_code.contains("BZN_FLASHBIKE") == true){policy_cnt*0.08}
    else if(product_code.contains("BZN_COOLQI") == true){policy_cnt*0.05}
    else if(product_code.contains("BZN_DEERBIKE") == true){policy_cnt*0.05}
    else if(product_code.contains("BZN_XJDC") == true){policy_cnt*0.09}
    else if(product_code.contains("BZN_FD_") == true){policy_cnt*0.02}
    else if(product_code=="BZN_MISSPAO_001" && policy_cnt<50000){policy_cnt*0.06}
    else if(product_code=="BZN_MISSPAO_001" && policy_cnt>=50000){policy_cnt*0.04}
    else if(product_code=="BZN_BLF_001"){policy_cnt*0.05}
    else if(product_code=="BZN_FEIGE_001"){policy_cnt*0.05}
    else if(product_code=="BZN_BATZB_001"){policy_cnt*0.55}
    else if(product_code=="BZN_D2ZC_001"){policy_cnt*0.8}
    else if(product_code=="BZN_D2ZC_002"){policy_cnt*1.2}
    else if(product_code=="BZN_D2ZC_003"){policy_cnt*1.6}
    else if(product_code=="BZN02DLR001"){policy_cnt*0.2}
    else if(product_code=="BZN04DLR002"){policy_cnt*0.4}
    else if(product_code=="BZN06DLR003"){policy_cnt*0.6}
    else if(product_code=="BZN08DLR004"){policy_cnt*0.8}
    else if(product_code=="BZN09DLR005"){policy_cnt*0.9}
    else if(product_code=="BZN_VANCL_001"){policy_cnt*0.08}
    else if(product_code=="BZN_HZ_DIABETES"){policy_cnt*80}
    else if(product_code=="BZN_HZ_HYPERTENSION"){policy_cnt*80}
    else if(product_code=="BZN_SOYOUNG_04"){policy_cnt*15}
    else if(product_code=="BZN_SOYOUNG_05"){policy_cnt*59}
    else if(product_code=="BZN_SOYOUNG_01"){policy_cnt*5}
    else if(product_code=="BZN_SOYOUNG_08"){policy_cnt*70}
    else if(product_code=="BZN_SOYOUNG_07"){policy_cnt*59}
    else if(product_code=="BZN_SOYOUNG_03"){policy_cnt*5}
    else if(product_code=="BZN_SOYOUNG_11"){policy_cnt*70}
    else if(product_code=="BZN_SOYOUNG_10"){policy_cnt*70}
    else if(product_code=="BZN_SOYOUNG_02"){policy_cnt*5}
    else if(product_code=="BZN_SOYOUNG_06"){policy_cnt*59}
    else if(product_code=="BZN_SOYOUNG_09"){policy_cnt*70}
    else {sum_premium}
    val res = BigDecimal.apply(sum_premium_res).setScale(4,RoundingMode.HALF_UP).doubleValue()
    res
  }

  /**
    * 获取 Mysql 表的数据
    * @param sqlContext
    * @param tableName 读取Mysql表的名字
    * @return 返回 Mysql 表的 DataFrame
    */
  def readMysqlTable(sqlContext: SQLContext, tableName: String): DataFrame = {
    val properties: Properties = getProPerties()
    sqlContext
      .read
      .format("jdbc")
      .option("url", properties.getProperty("mysql.url.106"))
      .option("driver", properties.getProperty("mysql.driver"))
      .option("user", properties.getProperty("mysql.username.106"))
      .option("password", properties.getProperty("mysql.password.106"))
      .option("numPartitions","10")
      .option("partitionColumn","id")
      .option("lowerBound", "0")
      .option("upperBound","200")
      .option("dbtable", tableName)
      .load()
  }

  /**
    * 获取配置文件
    *
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
}
