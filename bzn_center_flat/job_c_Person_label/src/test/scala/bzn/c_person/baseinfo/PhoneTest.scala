package bzn.c_person.baseinfo

import java.util.regex.Pattern

import bzn.c_person.util.SparkUtil
import bzn.job.common.Until
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object PhoneTest extends SparkUtil with Until {

  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName: String = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "local[*]")

    val sc: SparkContext = sparkConf._2
    val hiveContext: HiveContext = sparkConf._4

    getPhone(hiveContext).saveAsTextFile("d://mobile")

  }

  /**
    * 获取11位无重复手机号
    * @param hiveContext
    * @return
    */
  def getPhone(hiveContext: HiveContext): RDD[String] = {

    val insuredMobile: DataFrame = hiveContext.sql("select insured_mobile from odsdb.ods_policy_insured_detail")
      .where("length(insured_mobile) > 0")
      .selectExpr("insured_mobile as mobile")


    val holderMobile: DataFrame = hiveContext.sql("select mobile from odsdb.ods_holder_detail")
      .where("length(mobile) > 0")

    val result: RDD[String] = insuredMobile
      .unionAll(holderMobile)
       .map(x => {
         val phone = x.getAs[String]("mobile")
         phone
       })
      .filter(x => {
        val pattern = Pattern.compile("^1\\d{10}$")
        pattern.matcher(x).matches()
      })
      .distinct()
      .repartition(1)

    result

  }

}
