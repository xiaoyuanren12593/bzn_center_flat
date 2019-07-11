package bzn.c_person.centinfo

import java.sql.Timestamp

import bzn.c_person.util.SparkUtil
import bzn.job.common.{HbaseUtil, Until}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * author:xiaoYuanRen
  * Date:2019/7/10
  * Time:13:39
  * describe: c端核心标签清洗
  **/
object CPersonCenInfoTest extends SparkUtil with Until with HbaseUtil{
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "local[*]")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    getCPersonCenInfoDetail(hiveContext)

//    val confinfo: (Configuration, Configuration) = HbaseConf("label_person")
//    saveToHbase(test, "", "", confinfo._2, "", confinfo._1)
    sc.stop()
  }

  /**
    * 计算个人核心标签的数据
    * @param sqlContext
    */
  def getCPersonCenInfoDetail(sqlContext:HiveContext) = {

     /**
      * 读取投保人
      */
    val odsHolderDetail =
      sqlContext.sql("select policy_id,holder_name,holder_cert_type,holder_cert_no from odsdb.ods_holder_detail")
      .where("holder_cert_type = 1 and holder_cert_no is not null")
      .distinct()
      .limit(10)

    /**
      * 读取被保人表
      */
    val odsPolicyInsuredDetail =
      sqlContext.sql("select insured_id,insured_name,policy_id,insured_cert_type,insured_cert_no,start_date,end_date " +
        "from odsdb.ods_policy_insured_detail")
      .where("insured_cert_type = '1' and insured_cert_no is not null")
      .distinct()
      .limit(10)

    /**
      * 读取从属被保人
      */
    val odsPolicyInsuredSlaveDetail =
      sqlContext.sql("select slave_name, master_id,slave_cert_type,slave_cert_no from odsdb.ods_policy_insured_slave_detail")
      .where("slave_cert_type = '1' and slave_cert_no is not null")
      .distinct()
      .limit(10)

    /**
      * 所有人员证件号union
      */
    val certNos =odsHolderDetail.selectExpr("holder_cert_no as cert_no")
      .unionAll(odsPolicyInsuredDetail.selectExpr("insured_cert_no as cert_no"))
      .unionAll(odsPolicyInsuredSlaveDetail.selectExpr("slave_cert_no as cert_no"))
      .distinct()

//    odsHolderDetail.join(odsPolicyInsuredDetail)

    /**
      * 读取产品表
      */
    val odsProductDetail = sqlContext.sql("select product_code as product_code_slave,product_name,one_level_pdt_cate from odsdb.ods_product_detail")

    /**
      * 读取保单数据
      */
    val odsPolicyDetail = sqlContext.sql("select holder_name as holder_name_slave,policy_id as policy_id_slave,policy_code,policy_status,product_code,product_name," +
      "first_premium,sum_premium,sku_coverage,policy_start_date,policy_end_date,channel_id,channel_name,policy_create_time,payment_type,belongs_regional," +
      "insure_company_name from odsdb.ods_policy_detail")
      .where("policy_status in (0,1,-1)")
      .cache()

    val holderPolicy = odsHolderDetail.join(odsPolicyDetail,odsHolderDetail("policy_id")===odsPolicyDetail("policy_id_slave"),"leftouter")
      .selectExpr("policy_id","policy_code","holder_name","holder_cert_no","policy_status","product_code","product_name","first_premium","sum_premium",
        "sku_coverage","policy_start_date","policy_end_date","channel_id","channel_name","policy_create_time","payment_type","belongs_regional",
        "insure_company_name")

    HolderInfoNewDetal(sqlContext,holderPolicy,odsPolicyDetail,odsPolicyInsuredDetail)

  }

  /**
    * 投保人老客阶段
    * @param sqlContext
    * @param holderPolicy
    * @param odsPolicyDetail
    * @param odsPolicyInsuredDetail
    */
  def HolderInfoOldDetal(sqlContext:HiveContext,holderPolicy:DataFrame,odsPolicyDetail:DataFrame,odsPolicyInsuredDetail:DataFrame) = {
    import sqlContext.implicits._
    val newResOne = holderPolicy.map(x => {
      val policyId = x.getAs[String]("policy_id")
      val policyCode = x.getAs[String]("policy_code")
      val holderName = x.getAs[String]("holder_name")
      val holderCert_no = x.getAs[String]("holder_cert_no")
      val policyStatus = x.getAs[Int]("policy_status")
      val productCode = x.getAs[String]("product_code")
      val productName = x.getAs[String]("product_name")
      val firstPremium = x.getAs[Double]("first_premium")
      val sumPremium = x.getAs[Double]("sum_premium")
      val policyStartDate = x.getAs[java.sql.Timestamp]("policy_start_date")
      val policyEndDate = x.getAs[java.sql.Timestamp]("policy_end_date")
      val channelId = x.getAs[String]("channel_id")
      val channelName = x.getAs[String]("channel_name")
      val policyCreateTime = x.getAs[java.sql.Timestamp]("policy_create_time")
      val paymentType = x.getAs[Int]("payment_type")
      val belongsRegional = x.getAs[String]("belongs_regional")
      val insureCompanyName = x.getAs[String]("insure_company_name")
      var province = ""
      var city = ""
      if (belongsRegional != null) {
        if (belongsRegional.toString.length == 6) {
          province = belongsRegional.substring(0, 2) + "0000"
          city = belongsRegional.substring(0, 4) + "00"
        } else {
          province = null
          city = null
        }
      } else {
        province = null
        city = null
      }
      (holderCert_no, (policyId, policyCode, holderName, policyStatus, productCode, productName, firstPremium, sumPremium,
        policyStartDate, policyEndDate, channelId, channelName, policyCreateTime, paymentType, province, city, insureCompanyName))
    })
  }

  /**
    * 投保人新客标签数据处理
    * @param holderPolicy
    * @param odsPolicyDetail
    * @param odsPolicyInsuredDetail
    */
  def HolderInfoNewDetal(sQLContext:HiveContext,holderPolicy:DataFrame,odsPolicyDetail:DataFrame,odsPolicyInsuredDetail:DataFrame) = {
    import sQLContext.implicits._
    /**
      * 新客阶段贡献保费，转成老客时间
      */
    val newResOne = holderPolicy.map(x => {
      val policyId = x.getAs[String]("policy_id")
      val policyCode = x.getAs[String]("policy_code")
      val holderName = x.getAs[String]("holder_name")
      val holderCert_no = x.getAs[String]("holder_cert_no")
      val policyStatus = x.getAs[Int]("policy_status")
      val productCode = x.getAs[String]("product_code")
      val productName = x.getAs[String]("product_name")
      val firstPremium = x.getAs[Double]("first_premium")
      val sumPremium = x.getAs[Double]("sum_premium")
      val skuCoverage = x.getAs[String]("sku_coverage")
      val policyStartDate = x.getAs[java.sql.Timestamp]("policy_start_date")
      val policyEndDate = x.getAs[java.sql.Timestamp]("policy_end_date")
      val channelId = x.getAs[String]("channel_id")
      val channelName = x.getAs[String]("channel_name")
      val policyCreateTime = x.getAs[java.sql.Timestamp]("policy_create_time")
      val paymentType = x.getAs[Int]("payment_type")
      val belongsRegional  = x.getAs[String]("belongs_regional")
      val insureCompanyName  = x.getAs[String]("insure_company_name")
      var province = ""
      var city = ""
      if(belongsRegional != null){
        if(belongsRegional.toString.length == 6){
          province = belongsRegional.substring(0,2)+"0000"
          city = belongsRegional.substring(0,4)+"00"
        }else{
          province = null
          city = null
        }
      }else{
        province = null
        city = null
      }
      (holderCert_no,(policyId,policyCode,holderName,policyStatus,productCode,productName,firstPremium,sumPremium,skuCoverage,
        policyStartDate,policyEndDate,channelId,channelName,policyCreateTime,paymentType,province,city,insureCompanyName))
    })
      //对证件号进行分组，得到首次投保的保单信息
      .reduceByKey((x1,x2)=> {
        val res = if(x1._10.compareTo(x2._10) < 0) x1 else x2
        res
      })
      //如果开始时间是空  就用结束时间 作为首次投保时间
      .map(x => {
        val cert_no = x._1
        val policyStartDate = x._2._10
        val policyCreateTime = x._2._14
        var first_policy_time = policyStartDate
        if(policyStartDate == null){
          first_policy_time = policyCreateTime
        }
        //首次投保+90d
        val first_policy_time_90_days =
          if(first_policy_time != null){
            Timestamp.valueOf(dateAddNintyDay(first_policy_time.toString))
          }else if(policyCreateTime != null){
            Timestamp.valueOf(dateAddNintyDay(policyCreateTime.toString))
          }else{
            null
          }
        //年龄
        var age: Int = 0
        if(first_policy_time != null){
          age = getAgeFromBirthTime(cert_no,first_policy_time.toString)
        }
        (x._1,x._2._1,x._2._2,x._2._3,x._2._4,x._2._5,x._2._6,x._2._7,x._2._8,x._2._9,x._2._10,x._2._11,x._2._12,x._2._13,x._2._14,x._2._15,
          x._2._16,x._2._17,x._2._18,first_policy_time_90_days,age)
      })
      .toDF("holder_cert_no","policy_id","policy_code","holder_name","policy_status","product_code","product_name","first_premium","sum_premium",
        "sku_coverage","policy_start_date","policy_end_date","channel_id","channel_name","policy_create_time","payment_type","province","city",
        "insure_company_name","first_policy_time_90_days","age")

    val odsPolicyDetailTemp = odsPolicyDetail.selectExpr("policy_id_slave","holder_name_slave","sum_premium","policy_start_date")

    //新客阶段购买次数 和  累计保费
    val newResTwo = newResOne.selectExpr("holder_cert_no","policy_id","holder_name","first_policy_time_90_days")
      .join(odsPolicyDetailTemp,newResOne("policy_id")===odsPolicyDetailTemp("policy_id_slave"),"leftouter")
      .where("first_policy_time_90_days >= policy_start_date")
      .selectExpr("holder_cert_no","policy_id","sum_premium")
      .map(x => {
        val holderCert_no = x.getAs[String]("holder_cert_no")
        val sumPremium = x.getAs[Double]("sum_premium")
        val sumPremiumDecimal = BigDecimal.valueOf(sumPremium)
        (holderCert_no,(sumPremiumDecimal,1))
      })
      .reduceByKey((x1,x2) => {
        val sumPremiumDecimal = x1._1.+(x2._1)
        val firstCount = x1._2+x2._2
        (sumPremiumDecimal,firstCount)
      })
      .map(x => {
        (x._1,x._2._1.setScale(4,BigDecimal.RoundingMode.HALF_UP).doubleValue(),x._2._2)
      })
      .toDF("holder_cert_no_slave1","new_cus_sum_premium","new_cus_buy_cun")

    //新客阶段累计参保次数
    val odsPolicyInsuredDetailTemp = odsPolicyInsuredDetail.selectExpr("insured_cert_no","start_date")

    val newResThree = newResOne.selectExpr("holder_cert_no","policy_id","holder_name","first_policy_time_90_days")
      .join(odsPolicyInsuredDetailTemp,newResOne("holder_cert_no")===odsPolicyInsuredDetailTemp("insured_cert_no"))
      .where("first_policy_time_90_days >= start_date")
      .selectExpr("holder_cert_no")
      .map(x => (x.getAs[String]("holder_cert_no"),1))
      .reduceByKey(_+_)
      .map(x => (x._1,x._2))
      .toDF("holder_cert_no_slave2","new_cus_insured_cun")

    val resOne = newResOne.join(newResTwo,newResOne("holder_cert_no")===newResTwo("holder_cert_no_slave1"),"leftouter")
      .selectExpr("holder_cert_no","product_code","product_name","first_premium","sku_coverage","policy_start_date","policy_end_date",
        "channel_id","channel_name","policy_create_time","payment_type","province","city",
        "insure_company_name","age","new_cus_buy_cun","new_cus_sum_premium","first_policy_time_90_days")

    val res = resOne.join(newResThree,resOne("holder_cert_no")===newResThree("holder_cert_no_slave2"),"leftouter")
      .selectExpr("holder_cert_no","product_code","product_name","first_premium","sku_coverage","policy_start_date","policy_end_date",
        "channel_id","channel_name","policy_create_time","payment_type","province","city",
        "insure_company_name","age","new_cus_buy_cun","new_cus_sum_premium","new_cus_insured_cun","first_policy_time_90_days")

    res.show()
    res.printSchema()
  }

}
