package bzn.other

import java.text.SimpleDateFormat
import java.util.Date

import bzn.job.common.{MysqlUntil, Until}
import bzn.other.OdsOfficialMd5DetailTest.{MD5, OfficialMd5, WeddingMd5, sparkConfInfo}
import bzn.util.SparkUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.hive.HiveContext

object OdsInterIncrementMd5Test extends SparkUtil with Until with MysqlUntil {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName: String = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "local[*]")

    val sc: SparkContext = sparkConf._2
    val hiveContext = sparkConf._4


    sc.stop()


  }

  def InterMd5(hiveContext: HiveContext): DataFrame = {


    hiveContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    hiveContext.udf.register("MD5", (str: String) => MD5(str))
    hiveContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      //设置日期格式
      val date = df.format(new Date()) // new Date()为获取当前系统时间
      (date + "")
    })

    //读取接口数据
    val interDataTemp = hiveContext.sql("select * from odsdb.ods_all_business_person_base_info_detail where business_line='inter'")

    interDataTemp.selectExpr(
      "insured_cert_no",
      "MD5(insured_cert_no) as "



    )






    null
  }
}