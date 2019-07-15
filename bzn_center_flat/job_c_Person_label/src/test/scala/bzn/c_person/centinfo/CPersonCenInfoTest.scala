package bzn.c_person.centinfo

import java.sql.Timestamp

import bzn.c_person.util.SparkUtil
import bzn.job.common.{HbaseUtil, Until}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.hive.HiveContext

import scala.collection.immutable.Range
import scala.math.BigDecimal.RoundingMode

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
      .where("holder_cert_type = 1 and length(holder_cert_no) = 18")
      .distinct()
      .limit(10)

    /**
      * 读取被保人表
      */
    val odsPolicyInsuredDetail =
      sqlContext.sql("select insured_id,insured_name,policy_id,insured_cert_type,insured_cert_no,start_date,end_date,company_name,work_type " +
        "from odsdb.ods_policy_insured_detail")
      .where("insured_cert_type = '1' and length(insured_cert_no) = 18")
      .distinct()
      .limit(10)

    /**
      * 读取从属被保人
      */
    val odsPolicyInsuredSlaveDetail =
      sqlContext.sql("select slave_name, master_id,slave_cert_type,slave_cert_no,start_date as start_date_slave,end_date as end_date_slave from odsdb.ods_policy_insured_slave_detail")
      .where("slave_cert_type = '1' and length(slave_cert_no) = 18")
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

    /**
      * 读取职业码表
      */
    val odsWorktypeDimension = sqlContext.sql("select work_type as work_type_slave,level from odsdb.ods_worktype_dimension")

    /**
      * 读取理赔数据
      */
    val odsClaimsDetail =
      sqlContext.sql("select policy_no,case_no,risk_cert_no,pre_com,disable_level,case_status,final_payment from odsdb.ods_claims_detail")

    //    HolderInfoNewDetal(sqlContext,holderPolicy,odsPolicyDetail,odsPolicyInsuredDetail)
//    HolderInfoOldDetal(sqlContext,holderPolicy,odsPolicyDetail,odsPolicyInsuredDetail)
//    HolderInfoEasyToLossDetal(sqlContext,holderPolicy,odsPolicyDetail,odsPolicyInsuredDetail)
    InsuredInfoDetal(sqlContext,holderPolicy,odsPolicyDetail,odsPolicyInsuredDetail,odsPolicyInsuredSlaveDetail,odsWorktypeDimension)

  }

  /**
    * 投/被保人数据信息
    * @param sqlContext 上下文
    * @param holderPolicy  投保保单和投保人信息表
    * @param odsPolicyDetail 保单明细表
    * @param odsPolicyInsuredDetail 在保人明细表
    * @param odsPolicyInsuredSlaveDetail  被保人明细表
    * @param odsWorktypeDimension  工种码表
    */
  def InsuredInfoDetal(sqlContext:HiveContext,holderPolicy:DataFrame,odsPolicyDetail:DataFrame,odsPolicyInsuredDetail:DataFrame,odsPolicyInsuredSlaveDetail:DataFrame,odsWorktypeDimension:DataFrame) = {
    import sqlContext.implicits._

    //被保人详细信息 与 工种进行关联信息
    val odsPolicyInsuredDetailOne =
      odsPolicyInsuredDetail.join(odsWorktypeDimension,odsPolicyInsuredDetail("work_type")===odsWorktypeDimension("work_type_slave"),"leftouter")
      .selectExpr("insured_id","insured_cert_no","start_date","end_date","company_name","work_type","level")

    //从属被保人信息 没有在被保人信息表中出现的数据
    val odsPolicyInsuredSlaveDetailOne =
      odsPolicyInsuredSlaveDetail.join(odsPolicyInsuredDetail,odsPolicyInsuredSlaveDetail("slave_cert_no")===odsPolicyInsuredDetail("insured_cert_no"),"leftouter")
      .where("insured_cert_no is null")
      .selectExpr("master_id","slave_cert_no","start_date_slave","end_date_slave","'是' as is_join_policy")

    /**
      * 被保人职业信息
      * 所在单位
      * 职业风险等级
      * 是否参保
      */
    val odsPolicyInsuredDetailOneRes = odsPolicyInsuredDetailOne.map(x => {
      val insuredCertNo = x.getAs[String]("insured_cert_no")
      val startDate = x.getAs[String]("start_date")
      val endDate = x.getAs[String]("end_date")
      val companyName = x.getAs[String]("company_name")
      val workType = x.getAs[String]("work_type")
      val level = x.getAs[Int]("level")
      (insuredCertNo,(startDate,endDate,companyName,workType,level))
    })
      .reduceByKey((x1,x2) => {
         val res = if(x1._1.compareTo(x2._1) >= 0) x1 else x2
        res
      })
      .map( x => {
        //证件号  当前所在公司 当前工种  当前工种级别
        (x._1,x._2._3,x._2._4,x._2._5,"是")
      })
      .toDF("insured_cert_no","now_profession_name","now_company_name","now_profession_risk_level","is_join_policy")
//    odsPolicyInsuredDetailOneRes.show()
    /**
      * 累计参保次数
      * 是否在保
      */
    //当前时间
    val currDate = get_current_date(System.currentTimeMillis()).toString.substring(0,19)

    val odsPolicyInsuredDetailTwoRes = odsPolicyInsuredDetailOne.map(x => {
      val insuredCertNo = x.getAs[String]("insured_cert_no")
      val startDate = x.getAs[String]("start_date")
      val endDate = x.getAs[String]("end_date")
      (insuredCertNo,(startDate,endDate))
    })
      .map(x => {
        if((x._2._1.compareTo(currDate) <= 0) && (x._2._2.compareTo(currDate) >=0)){
          (x._1,(1,1))
        }else{
          (x._1,(0,1))
        }
      })
      .reduceByKey((x1,x2) => {
        var isInsuredCount = x1._1+x2._1 //在保数 如果在保数》0 说明又在保的保单
        var policyInsuredCun =  x1._2+x2._2 //总的参保次数
        (isInsuredCount,policyInsuredCun)
      })
      .map(x => {
        if(x._2._1 > 0){
          (x._1,"在保",x._2._2)
        }else{
          (x._1,"不在保",x._2._2)
        }
      })
      .toDF("insured_cert_no","is_insured","policy_insured_cun")
//    odsPolicyInsuredDetailTwoRes.show()

    val odsPolicyInsuredSlaveDetailOneRes = odsPolicyInsuredSlaveDetailOne.map(x => {
      val insuredCertNo = x.getAs[String]("slave_cert_no")
      val startDate = x.getAs[java.sql.Timestamp]("start_date_slave")
      val endDate = x.getAs[java.sql.Timestamp]("end_date_slave")
      val isJoinPolicy = x.getAs[String]("is_join_policy")
      ((insuredCertNo,isJoinPolicy),(startDate,endDate))
    })
      .map(x => {
        if(x._2._1 != null && x._2._2 != null){
          if((x._2._1.toString.compareTo(currDate) <= 0) && (x._2._2.toString.compareTo(currDate) >=0)){
            (x._1,(1,1))
          }else{
            (x._1,(0,1))
          }
        }else{
          (x._1,(0,1))
        }
      })
      .reduceByKey((x1,x2) => {
        var isInsuredCount = x1._1+x2._1
        var policyInsuredCun =  x1._2+x2._2
        (isInsuredCount,policyInsuredCun)
      })
      .map(x => {
        if(x._2._1 > 0){
          (x._1._1,"在保",x._2._2,x._1._2)
        }else{
          (x._1._1,"不在保",x._2._2,x._1._2)
        }
      })
      .toDF("insured_cert_no","is_insured","policy_insured_cun","is_join_policy")

    odsPolicyInsuredSlaveDetailOneRes.show()

  }

  /**
    * 投保人易流失阶段数据
    * @param sqlContext 上下文
    * @param holderPolicy  投保保单和投保人信息表
    * @param odsPolicyDetail 保单明细表
    * @param odsPolicyInsuredDetail 在保人明细表
    */
  def HolderInfoEasyToLossDetal(sqlContext:HiveContext,holderPolicy:DataFrame,odsPolicyDetail:DataFrame,odsPolicyInsuredDetail:DataFrame) = {
    import sqlContext.implicits._

    //保险止期
    val newResOne = holderPolicy.map(x => {
      val holderCert_no = x.getAs[String]("holder_cert_no")
      val policyEndDate = x.getAs[java.sql.Timestamp]("policy_end_date")
      (holderCert_no,policyEndDate)
    })
      .reduceByKey((x1,x2) => {
        var policyEndDate = if(x1.compareTo(x2) >= 0) x1 else x2
        policyEndDate
      })
      .map(x => (x._1,x._2))
      .toDF("holder_cert_no","last_policy_end_date")

//    newResOne.take(10).foreach(println)
    newResOne.printSchema()
  }

  /**
    * 投保人老客阶段
    * @param sqlContext 上下文
    * @param holderPolicy  投保保单和投保人信息表
    * @param odsPolicyDetail 保单明细表
    * @param odsPolicyInsuredDetail 在保人明细表
    */
  def HolderInfoOldDetal(sqlContext:HiveContext,holderPolicy:DataFrame,odsPolicyDetail:DataFrame,odsPolicyInsuredDetail:DataFrame) = {
    import sqlContext.implicits._

    //近90天时间
    val currDateDelNintyDay = Timestamp.valueOf(dateDelNintyDay(get_current_date(System.currentTimeMillis()).toString.substring(0, 19)))

    //当前时间
    val currDate = get_current_date(System.currentTimeMillis()).toString.substring(0,19)

    //投保信息
    val newResOne = holderPolicy.map(x => {
      val holderCert_no = x.getAs[String]("holder_cert_no")
      val policyStartDate = x.getAs[java.sql.Timestamp]("policy_start_date")
      val policyEndDate = x.getAs[java.sql.Timestamp]("policy_end_date")
      val productCode = x.getAs[String]("product_code")
      val productName = x.getAs[String]("product_name")
      val belongsRegional = x.getAs[String]("belongs_regional")
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
      (holderCert_no, (policyStartDate, policyEndDate,productCode,productName, province, city))
    })
      .reduceByKey((x1, x2) => {
        var res = if (x1._1.compareTo(x2._1) >= 0) x1 else x2
        res
      })
      .map(x => {
        //证件号 开始时间 结束时间 产品code，产品名称  省份 城市  最新保单距今天数
        if(x._2._1 != null){
          (x._1, x._2._1, x._2._2, x._2._3, x._2._4,x._2._5,x._2._6,
            getBeg_End_one_two_new(x._2._1.toString.substring(0,19),currDate))
        }else{
          (x._1, x._2._1, x._2._2, x._2._3, x._2._4,x._2._5,x._2._6,0L)
        }
      })
      .toDF("holder_cert_no", "last_policy_date", "policy_end_date", "last_policy_product_code","last_policy_product_name","now_province", "now_city",
        "last_policy_days")

    //近90天的数据
    val newResTwo = holderPolicy.map(x => {
      val holderCert_no = x.getAs[String]("holder_cert_no")
      val firstPremium = x.getAs[Double]("first_premium")
      val sumPremium = x.getAs[Double]("sum_premium")
      val policyStartDate = x.getAs[java.sql.Timestamp]("policy_start_date")
      val policyEndDate = x.getAs[java.sql.Timestamp]("policy_end_date")
      (holderCert_no, (policyStartDate, policyEndDate, firstPremium, sumPremium, currDateDelNintyDay, 1))
    })
      .filter(x => if (x._2._5.compareTo(x._2._1) <= 0) true else false)
      .map(x => {
        (x._1, (x._2._4, x._2._6))
      })
      .reduceByKey((x1,x2) => {
        var count: Int = x1._2 + x2._2
        var sumPremium = BigDecimal.valueOf(x1._1).setScale(4,RoundingMode.HALF_UP).+(BigDecimal.valueOf(x2._1).setScale(4,RoundingMode.HALF_UP))
        (sumPremium.doubleValue(),count)
      })
      .map(x => (x._1,x._2._1,x._2._2,BigDecimal.valueOf(x._2._1/x._2._2).setScale(4,RoundingMode.HALF_UP).doubleValue()))
      .toDF("holder_cert_no_slave","ninety_policy_premium","ninety_policy_cun","ninety_premium_avg")

    //累计数据信息
    val newResThree = holderPolicy.map(x => {
      val holderCert_no = x.getAs[String]("holder_cert_no")
      val sumPremium = x.getAs[Double]("sum_premium")
      (holderCert_no, (sumPremium,1))
    })
      .reduceByKey((x1,x2) => {
        var count = x1._2 + x2._2
        var sumPremium = BigDecimal.valueOf(x1._1).setScale(4,RoundingMode.HALF_UP).+(BigDecimal.valueOf(x2._1).setScale(4,RoundingMode.HALF_UP))
        (sumPremium.doubleValue,count)
      })
      .map(x => (x._1,x._2._1,x._2._2,BigDecimal.valueOf(x._2._1/x._2._2).setScale(4,RoundingMode.HALF_UP).doubleValue()))
      .toDF("holder_cert_no_slave","policy_premium","policy_cun","premium_avg")

    val oneRes = newResOne.join(newResTwo,newResOne("holder_cert_no")===newResTwo("holder_cert_no_slave"),"leftouter")
      .selectExpr("holder_cert_no", "last_policy_date", "policy_end_date", "last_policy_product_code","last_policy_product_name","now_province", "now_city",
        "last_policy_days","ninety_policy_premium","ninety_policy_cun","ninety_premium_avg")

    val res = oneRes.join(newResThree,oneRes("holder_cert_no")===newResThree("holder_cert_no_slave"),"leftouter")
      .selectExpr("holder_cert_no","last_policy_date","last_policy_product_code","last_policy_product_name","now_province", "now_city","last_policy_days",
        "ninety_policy_premium","ninety_policy_cun","ninety_premium_avg","policy_premium","policy_cun","premium_avg")

    res.show()
    res.printSchema()
  }

  /**
    * 投保人新客标签数据处理
    * @param holderPolicy  投保保单和投保人信息表
    * @param odsPolicyDetail 保单明细表
    * @param odsPolicyInsuredDetail 在保人明细表
    */
  def HolderInfoNewDetal(sQLContext:HiveContext,holderPolicy:DataFrame,odsPolicyDetail:DataFrame,odsPolicyInsuredDetail:DataFrame): Unit = {
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
        var firstPolicyTime = policyStartDate
        if(policyStartDate == null){
          firstPolicyTime = policyCreateTime
        }
        //首次投保+90d
        val first_policy_time_90_days =
          if(firstPolicyTime != null){
            Timestamp.valueOf(dateAddNintyDay(firstPolicyTime.toString))
          }else{
            null
          }
        //首次投保距今天的天数
        val firstPolicyDays = if(firstPolicyTime!= null){
          getBeg_End_one_two_new(firstPolicyTime.toString,get_current_date(System.currentTimeMillis()).toString.substring(0,19))
        }else {
          null
        }
        //年龄
        var age: Int = 0
        if(firstPolicyTime != null){
          age = getAgeFromBirthTime(cert_no,firstPolicyTime.toString)
        }
        (x._1,x._2._1,x._2._2,x._2._3,x._2._4,x._2._5,x._2._6,x._2._7,x._2._8,x._2._9,x._2._10,x._2._11,x._2._12,x._2._13,x._2._14,x._2._15,
          x._2._16,x._2._17,x._2._18,first_policy_time_90_days,age,firstPolicyDays)
      })
      .toDF("holder_cert_no","policy_id","policy_code","holder_name","policy_status","product_code","product_name","first_premium","sum_premium",
        "sku_coverage","policy_start_date","policy_end_date","channel_id","channel_name","policy_create_time","payment_type","province","city",
        "insure_company_name","first_policy_time_90_days","age","first_policy_days")

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
        "insure_company_name","age","new_cus_buy_cun","new_cus_sum_premium","first_policy_time_90_days","first_policy_days")

    val res = resOne.join(newResThree,resOne("holder_cert_no")===newResThree("holder_cert_no_slave2"),"leftouter")
      .selectExpr("holder_cert_no","product_code","product_name","first_premium","sku_coverage","policy_start_date","policy_end_date",
        "channel_id","channel_name","policy_create_time","payment_type","province","city",
        "insure_company_name","age","new_cus_buy_cun","new_cus_sum_premium","new_cus_insured_cun","first_policy_time_90_days","first_policy_days")

    res.show()
    res.printSchema()
  }

}
