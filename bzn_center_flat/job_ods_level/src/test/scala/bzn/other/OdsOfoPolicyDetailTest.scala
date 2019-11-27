package bzn.other

import java.util.Properties

import bzn.job.common.MysqlUntil
import bzn.ods.util.Until
import bzn.util.SparkUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext

import scala.io.Source

/*
* @Author:liuxiang
* @Date：2019/11/27
* @Describe:
*/ object OdsOfoPolicyDetailTest  extends SparkUtil with Until with MysqlUntil{

  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "local[*]")

    val sc = sparkConf._2
    val sqlContext = sparkConf._3
    val hqlContext = sparkConf._4
    val res = readMysqlOtherTable(sqlContext)
    res.show(100)
    res.printSchema()
    /*hqlContext.sql("truncate table ods_open_ofo_twotosix_policy_detail")
    res.write.mode(SaveMode.Append).saveAsTable("ods_open_ofo_twotosix_policy_detail")*/

  }

  /**
    * ofo 2018-02 到2018-06 数据
    * @param sqlContext
    */
  def readMysqlOtherTable(sqlContext: SQLContext):DataFrame= {

    val properties: Properties = getProPerties()
    val url02: String = "jdbc:mysql://172.16.11.103:3306/bzn_open_201802?tinyInt1isBit=false&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&allowMultiQueries=true&user=root&password=123456"
    val url03: String = "jdbc:mysql://172.16.11.103:3306/bzn_open_201803?tinyInt1isBit=false&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&allowMultiQueries=true&user=root&password=123456"
    val url04: String = "jdbc:mysql://172.16.11.103:3306/bzn_open_201804?tinyInt1isBit=false&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&allowMultiQueries=true&user=root&password=123456"
    val url05: String = "jdbc:mysql://172.16.11.103:3306/bzn_open_201805?tinyInt1isBit=false&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&allowMultiQueries=true&user=root&password=123456"
    val url06: String = "jdbc:mysql://172.16.11.103:3306/bzn_open_201806?tinyInt1isBit=false&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&allowMultiQueries=true&user=root&password=123456"


    val data02: DataFrame = sqlContext.read.jdbc(url02, "open_ofo_policy", properties)
    val data03: DataFrame = sqlContext.read.jdbc(url03, "open_ofo_policy", properties)
    val data04: DataFrame = sqlContext.read.jdbc(url04, "open_ofo_policy", properties)
    val data05: DataFrame = sqlContext.read.jdbc(url05, "open_ofo_policy", properties)
    val data06: DataFrame = sqlContext.read.jdbc(url06, "open_ofo_policy", properties)
    val frame = data02
      .unionAll(data03)
      .unionAll(data04)
      .unionAll(data05)
      .unionAll(data06)
      .selectExpr("insured_name","insured_cert_no","insured_mobile","policy_id","start_date","end_date","create_time","update_time","product_code","null as sku_price","'ofo' as business_line","substring(month,1,7) as years")
    frame


  }

}
