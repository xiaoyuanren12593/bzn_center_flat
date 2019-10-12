package bzn.other

import bzn.job.common.{ClickHouseUntil, Until}
import bzn.util.SparkUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{ SQLContext}
import org.apache.spark.sql.hive.HiveContext


/*
* @Author:liuxiang
* @Date：2019/10/12
* @Describe:
*/object sparkJdbcClickHouseTest extends  SparkUtil with ClickHouseUntil {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "local[*]")

    val sc = sparkConf._2
    val hiveContext: HiveContext = sparkConf._4
 /*   val res: DataFrame = hiveContext.sql("select policy_id,policy_code,product_code  from odsdb.ods_policy_detail")

    writeClickHouseTable(res, "ods_policy_test",SaveMode.Overwrite,
      "clickhouse.url","clickhouse.username","clickhouse.password")*/

    val sql = sparkConf._3
    val res1 = readClickHouseTable(sql,"ods_policy_test","clickhouse.url","clickhouse.username","clickhouse.password")
    res1.show(10)
    println("666")
    sc.stop()


  }



}

