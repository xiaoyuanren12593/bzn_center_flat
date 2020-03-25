package bzn.ods.policy

import bzn.job.common.{MysqlUntil, Until}
import bzn.util.SparkUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

object OdsClaimsDetail extends SparkUtil with Until with MysqlUntil {


  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName: String = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "local[*]")

    val sc: SparkContext = sparkConf._2
    val sqlContext = sparkConf._3
    val hiveContext: HiveContext = sparkConf._4
    ClaimsDetail(sqlContext)

  }

  def ClaimsDetail(SQLContext: SQLContext): Unit = {
    val user_106 = "mysql.username.106"
    val ps_106 = "mysql.password.106"
    val driver_106 = "mysql.driver"
    val sourcce_url_106 = "mysql.url.106"

  //读取报案表
  val fnolPhoenixfnol = readMysqlTable(SQLContext, "fnol_phoenixfnol", user_106, ps_106, driver_106, sourcce_url_106)
    .selectExpr("fnol_no", "insurance_policy_no", "occurrence_scene", "injury_type", "status")

    fnolPhoenixfnol.show(100)



  }


}
