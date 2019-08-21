package bzn.c_person.centinfo

import bzn.c_person.util.SparkUtil
import bzn.job.common.{HbaseUtil, Until}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * author:xiaoYuanRen
  * Date:2019/7/30
  * Time:14:44
  * describe: 统计hbase所有有数的列的个数
  **/
object HbaseColumnCountTest extends SparkUtil with HbaseUtil with Until{
  def main (args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "local[*]")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4

    import hiveContext.implicits._
    val hbaseData = getHbaseBussValue(sc,"label_person").cache()
    val hbaseBase = hbaseData
      .map(x => {
        val baseArray = Array ("base_name",
            "base_gender",
            "base_birthday",
            "base_age",
            "base_age_time",
            "base_age_section",
            "base_is_retire",
            "base_email",
            "base_married",
            "base_bank_code",
            "base_bank_deposit",
            "base_province",
            "base_city",
            "base_area",
            "base_coastal",
            "base_city_type",
            "base_weather_feature",
            "base_city_weather",
            "base_city_deit",
            "base_cons_name",
            "base_cons_type",
            "base_cons_character",
            "bsae_tel",
            "base_habit",
            "base_child_cun",
            "base_child_age",
            "base_child_attend_sch")
        val res = baseArray.map(f => {
          val value = if(Bytes.toString(x._2.getValue("base_info".getBytes, f.getBytes)) == null){
            0
          }else{
            1
          }
         value
        })
        val strRes = res.mkString("\u0001")
        (1,(strRes,1))
      })
      .reduceByKey((x1,x2) => {
        val array1 = x1._1.split("\u0001").array
        val array2 = x2._1.split("\u0001").array
        for (i <- 0 until array1.length){
          array1(i) = (array1(i).toInt + array2(i).toInt).toString
        }
        val count = x1._2+x2._2
        (array1.mkString("\u0001"),count)
      })
      .map(x => {
        (x._2._1.toString.split("\u0001").mkString("`"),x._2._2)
      })
    hbaseBase.take(1).foreach(println)
    hbaseBase.repartition(1).saveAsTextFile("\\xing\\jar\\hbase_data_2019-7-30")

    val hbaseCent = hbaseData
      .map(x => {
        val key = Bytes.toString(x._2.getRow)
        val centArray = Array(
          "first_policy_time",
          "first_policy_pdt_name",
          "first_policy_pdt_code",
          "first_policy_plan",
          "first_policy_premium",
          "first_policy_source",
          "first_policy_section",
          "first_policy_pay_channel",
          "first_policy_age",
          "first_policy_province",
          "first_policy_city",
          "first_policy_city_level",
          "first_policy_insurant_name",
          "new_cus_active_info",
          "new_cus_active_acceptability",
          "new_cus_buy_cun",
          "new_cus_insured_cun",
          "new_cus_sum_premium",
          "become_old_time",
          "is_redeem",
          "is_awaken",
          "redeem_time",
          "awaken_time",
          "now_province",
          "now_city",
          "now_profession_name",
          "now_industry_name",
          "now_company_name",
          "now_profession_risk_level",
          "ninety_policy_cun",
          "ninety_policy_premium",
          "ninety_premium_avg",
          "policy_cun",
          "policy_insured_cun",
          "policy_premium",
          "premium_avg",
          "last_policy_days",
          "first_policy_days",
          "risk_cun",
          "pre_premium_sum",
          "effect_policy_cun",
          "old_cus_active_info",
          "old_cus_active_acceptability",
          "last_policy_end_date",
          "last_policy_date",
          "last_policy_product_code",
          "last_policy_product_name",
          "submit_paper_cun",
          "reject_claim_cun",
          "withdraw_claim_cun",
          "warn_cus_active_info",
          "warn_cus_active_acceptability",
          "loss_date",
          "life_days",
          "loss_cus_active_info",
          "old_cus_active_acceptability",
          "sleep_date",
          "awake_cus_active_info",
          "awake_cus_active_acceptability",
          "cus_source",
          "is_join_policy",
          "latent_product_info",
          "is_insured"
        )
        val res = centArray.map(f => {
          val value = if(Bytes.toString(x._2.getValue("cent_info".getBytes, f.getBytes)) == null){
            0
          }else{
            1
          }
          value
        })
        val strRes = res.mkString("\u0001")
        (1,(strRes,1))
      })
      .reduceByKey((x1,x2) => {
        val array1 = x1._1.split("\u0001").array
        val array2 = x2._1.split("\u0001").array
        for (i <- 0 until array1.length){
          array1(i) = (array1(i).toInt + array2(i).toInt).toString
        }
        val count = x1._2+x2._2
        (array1.mkString("\u0001"),count)
      })
      .map(x => {
        (x._2._1.toString.split("\u0001").mkString("`"),x._2._2)
      })

    hbaseCent.repartition(1).saveAsTextFile("\\xing\\jar\\hbase_data_2019-7-31")

    val hbaseHigh = hbaseData
      .map(x => {
        val key = Bytes.toString(x._2.getRow)
        val highArray = Array(
          "cus_level",
          "cus_type",
          "last_cus_type",
          "is_coxcombry",
          "wedding_stage",
          "wedding_month",
          "sports_rate",
          "ride_days",
          "max_ride_time_step",
          "max_ride_brand",
          "max_ride_date",
          "trip_rate",
          "internal_clock",
          "is_online_car",
          "part_time_nums",
          "is_h_house",
          "Amplitude_frequenter",
          "love_travel",
          "is_medical",
          "is_perceive"
        )
        val res = highArray.map(f => {
          val value = if(Bytes.toString(x._2.getValue("high_info".getBytes, f.getBytes)) == null){
            0
          }else{
            1
          }
          value
        })
        val strRes = res.mkString("\u0001")
        (1,(strRes,1))
      })
      .reduceByKey((x1,x2) => {
        val array1 = x1._1.split("\u0001").array
        val array2 = x2._1.split("\u0001").array
        for (i <- 0 until array1.length){
          array1(i) = (array1(i).toInt + array2(i).toInt).toString
        }
        val count = x1._2+x2._2
        (array1.mkString("\u0001"),count)
      })
      .map(x => {
        (x._2._1.toString.split("\u0001").mkString("`"),x._2._2)
      })

    hbaseHigh.repartition(1).saveAsTextFile("\\xing\\jar\\hbase_data_2019-7-32")
    sc.stop()
  }

  def getHbaseColumnCount(sc:SparkContext,sqlContext: HiveContext) = {
    import sqlContext.implicits._

    val centArray = Array(
      "first_policy_time",
      "first_policy_pdt_name",
      "first_policy_pdt_code",
      "first_policy_plan",
      "first_policy_premium",
      "first_policy_source",
      "first_policy_section",
      "first_policy_pay_channel",
      "first_policy_age",
      "first_policy_province",
      "first_policy_city",
      "first_policy_city_level",
      "first_policy_insurant_name",
      "new_cus_active_info",
      "new_cus_active_acceptability",
      "new_cus_buy_cun",
      "new_cus_insured_cun",
      "new_cus_sum_premium",
      "become_old_time",
      "is_redeem",
      "is_awaken",
      "redeem_time",
      "awaken_time",
      "now_province",
      "now_city",
      "now_profession_name",
      "now_industry_name",
      "now_company_name",
      "now_profession_risk_level",
      "ninety_policy_cun",
      "ninety_policy_premium",
      "ninety_premium_avg",
      "policy_cun",
      "policy_insured_cun",
      "policy_premium",
      "premium_avg",
      "last_policy_days",
      "first_policy_days",
      "risk_cun",
      "pre_premium_sum",
      "effect_policy_cun",
      "old_cus_active_info",
      "old_cus_active_acceptability",
      "last_policy_end_date",
      "last_policy_date",
      "last_policy_product_code",
      "last_policy_product_name",
      "submit_paper_cun",
      "reject_claim_cun",
      "withdraw_claim_cun",
      "warn_cus_active_info",
      "warn_cus_active_acceptability",
      "loss_date",
      "life_days",
      "loss_cus_active_info",
      "old_cus_active_acceptability",
      "sleep_date",
      "awake_cus_active_info",
      "awake_cus_active_acceptability",
      "cus_source",
      "is_join_policy",
      "latent_product_info",
      "is_insured"
    )

    val highArray = Array(
      "cus_level",
      "cus_type",
      "last_cus_type",
      "is_coxcombry",
      "wedding_stage",
      "wedding_month",
      "sports_rate",
      "ride_days",
      "max_ride_time_step",
      "max_ride_brand",
      "max_ride_date",
      "trip_rate",
      "internal_clock",
      "is_online_car",
      "part_time_nums",
      "is_h_house",
      "Amplitude_frequenter",
      "love_travel",
      "is_medical",
      "is_perceive"
    )

    /**
      * 读取hbase上的数据
      */

//      .map(x => {
//        val baseArray = Array ("base_name",
//          "base_gender",
//          "base_birthday",
//          "base_age",
//          "base_age_time",
//          "base_age_section",
//          "base_is_retire",
//          "base_email",
//          "base_married",
//          "base_bank_code",
//          "base_bank_deposit",
//          "base_province",
//          "base_city",
//          "base_area",
//          "base_coastal",
//          "base_city_type",
//          "base_weather_feature",
//          "base_city_weather",
//          "base_city_deit",
//          "base_cons_name",
//          "base_cons_type",
//          "base_cons_character",
//          "bsae_tel",
//          "base_habit",
//          "base_child_cun",
//          "base_child_age",
//          "base_child_attend_sch")
//        val key = Bytes.toString (x._2.getRow)
//        val flatRes = baseArray.map (f => {
//          val value = Bytes.toString (x._2.getValue ("base_info".getBytes, f.getBytes))
//          val res = if(value != null) {
//            f + "1"
//          } else {
//            f + "0"
//          }
//          res
//        })
//
//        flatRes
//      })

  }
}
