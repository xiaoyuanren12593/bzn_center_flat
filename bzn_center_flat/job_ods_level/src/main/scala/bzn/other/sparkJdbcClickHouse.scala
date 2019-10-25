package bzn.other

import bzn.job.common.{ClickHouseUntil, Until}
import bzn.util.SparkUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}


/*
* @Author:liuxiang
* @Date：2019/10/10
* @Describe:
*/
object sparkJdbcClickHouse extends  SparkUtil with Until with ClickHouseUntil{

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName,"")

    val sc = sparkConf._2
    val hiveContext: HiveContext = sparkConf._4
    val res: DataFrame = hiveContext.sql("select policy_id,policy_code,product_code from odsdb.ods_policy_detail")
    writeClickHouseTable(res,"odsdb.ods_policy_test",SaveMode.Overwrite,
      "clickhouse.url","clickhouse.username","clickhouse.password")
    sc.stop()

  }

}
