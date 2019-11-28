package bzn.other

import java.util.Properties

import bzn.job.common.MysqlUntil
import bzn.ods.util.Until
import bzn.other.OdsOfoPolicyDetailTest.{readMysqlOtherTable, sparkConfInfo}
import bzn.other.OtherToHive.getProPerties
import bzn.util.SparkUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.hive.HiveContext

/*
* @Author:liuxiang
* @Date：2019/11/28
* @Describe:
*/ object OdsOtherPolicyDeatilTest extends SparkUtil with Until with MysqlUntil{

  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "local[*]")

    val sc = sparkConf._2
    val sqlContext = sparkConf._3
    val hqlContext = sparkConf._4
    val res = OtherData(sqlContext)

    /*hqlContext.sql("truncate table ods_open_ofo_twotosix_policy_detail")
    res.write.mode(SaveMode.Append).saveAsTable("ods_open_ofo_twotosix_policy_detail")*/

  }


  /**
    * 上下文
    *
    */
  def OtherData(sqlContext: SQLContext)={


    val properties: Properties = getProPerties()

    val url = "jdbc:mysql://172.16.11.103:3306/bzn_open_all?tinyInt1isBit=false&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&allowMultiQueries=true&user=root&password=123456"


    val predicates = Array[String](
      "month > '2019-09-01' and  month<=2019-10-02",
            "month > '2019-10-01' and  month<=2019-11-02",
            "month > '2019-11-01'"
    )

    val data13: DataFrame = sqlContext
      .read
      .jdbc(url, "open_other_policy", predicates, properties)
    data13.printSchema()


  }

}