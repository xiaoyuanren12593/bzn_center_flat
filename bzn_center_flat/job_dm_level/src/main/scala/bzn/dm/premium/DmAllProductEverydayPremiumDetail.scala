package bzn.dm.premium

import java.sql.{Connection, DriverManager}
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import bzn.job.common.Until
import bzn.dm.util.SparkUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

import scala.io.Source
import scala.math.BigDecimal.RoundingMode

/**
  * author:xiaoYuanRen
  * Date:2019/6/19
  * Time:10:06
  * describe: 计算官网和接口所有产品的每天的新投和保全总单数和总保费
  **/
object DmAllProductEverydayPremiumDetail extends SparkUtil with Until{
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName,"")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    val res = DmAllProductEverydayPremium(hiveContext)
    /**
      * hive和mysql同步一份数据
      */
    res.write.mode(SaveMode.Overwrite).saveAsTable("dmdb.dm_products_everyday_premium_detail")
    saveASMysqlTable(res,"dm_products_everyday_premium_detail",SaveMode.Overwrite)
    sc.stop()
  }

  /**
    * 所有产品每日总保费
    * @param sqlContext
    */
  def DmAllProductEverydayPremium(sqlContext:HiveContext) = {
    import sqlContext.implicits._

    sqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    sqlContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")//设置日期格式
      val date = df.format(new Date())// new Date()为获取当前系统时间
      (date + "")
    })

    /**
      * 读取保费表 计算不同产品的保费
      */
    val productPremiumOne = sqlContext.sql("select one_level_pdt_cate,premium_type,sum_premium,day_id from dwdb.dw_policy_premium_detail")
      .map(x => {
        val oneLevelPdtCate = x.getAs[String]("one_level_pdt_cate")
        val premiumType = x.getAs[Int]("premium_type")
        val sumPremium = x.getAs[Double]("sum_premium")
        val premiumDecimal =  BigDecimal.valueOf(sumPremium)
        val dayId = x.getAs[String]("day_id")
        ((oneLevelPdtCate,dayId,premiumType),(1,premiumDecimal))
      })
      .reduceByKey((x1,x2) => {
        val policyCount = x1._1+x2._1
        val sumPremium = x1._2.+(x2._2)
        (policyCount,sumPremium)
      })
      .map(x => {
        //业务条线，day_id,所有类型的保单数量，总保费
        (x._1._1,x._1._2,x._1._3,x._2._1,x._2._2.setScale(4,RoundingMode.HALF_UP).doubleValue(),"官网")
      })
      .toDF("business_line","day_id","premium_type","policy_count","sum_premium","source")

    /**
      * 接口数据
      */
    val hljRes = sqlContext.sql("select one_level_pdt_cate,premium_type,policy_count,sum_premium,day_id from odsdb.ods_inter_hdj_premium_detail")
      .map(x => {
        val oneLevelPdtCate = x.getAs[String]("one_level_pdt_cate")
        val premiumType = x.getAs[Int]("premium_type")
        val policyCount = x.getAs[Int]("policy_count")
        val sumPremium = x.getAs[Double]("sum_premium")
        val premiumDecimal =  BigDecimal.valueOf(sumPremium)
        val dayId = x.getAs[String]("day_id")
        ((oneLevelPdtCate,dayId,premiumType),(1,premiumDecimal))
      })
      .reduceByKey((x1,x2) => {
        val policyCount = x1._1+x2._1
        val sumPremium = x1._2.+(x2._2)
        (policyCount,sumPremium)
      })
      .map(x => {
        //业务条线，day_id,所有类型的保单数量，总保费
        (x._1._1,x._1._2,x._1._3,x._2._1,x._2._2.setScale(4,RoundingMode.HALF_UP).doubleValue(),"接口")
      })
      .toDF("business_line","day_id","premium_type","policy_count","sum_premium","source")

    val res = productPremiumOne.unionAll(hljRes)
      .selectExpr("getUUID() as id","business_line","day_id","premium_type","policy_count","sum_premium","source","getNow() as dw_create_time")

    res
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
    prop.setProperty("user", properties.getProperty("mysql.username.106"))
    prop.setProperty("password", properties.getProperty("mysql.password.106"))
    prop.setProperty("driver", properties.getProperty("mysql.driver"))
    prop.setProperty("url", properties.getProperty("mysql_url.106.dmdb"))
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
}
