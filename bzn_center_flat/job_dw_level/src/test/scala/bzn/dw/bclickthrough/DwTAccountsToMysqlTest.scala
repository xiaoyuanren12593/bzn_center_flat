package bzn.dw.bclickthrough

import bzn.dw.util.SparkUtil
import bzn.job.common.{MysqlUntil, Until}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext

/*
* @Author:liuxiang
* @Date：2019/11/5
* @Describe:
*/
object DwTAccountsToMysqlTest extends SparkUtil with Until with MysqlUntil {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "local[*]")

    val sc = sparkConf._2
    val hqlContext = sparkConf._4
    val sqlContext = sparkConf._3
    //读取hive的表
    val res = hqlContext.sql("select * from dwdb.dw_t_update_accounts_employer_detail").drop("id")
    saveASMysqlTable(res, "t_accounts_employer_test", SaveMode.Overwrite, "mysql.username.103",
      "mysql.password.103", "mysql.driver", "mysql_url.103.odsdb")
    sc.stop()

  }
}
