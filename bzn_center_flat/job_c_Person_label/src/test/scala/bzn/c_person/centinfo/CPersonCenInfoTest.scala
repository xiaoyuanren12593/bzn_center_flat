package bzn.c_person.centinfo

import java.sql.Timestamp
import java.util

import c_person.util.SparkUtil
import bzn.job.common.{HbaseUtil, Until}
import com.alibaba.fastjson.{JSON, JSONObject}
import com.alibaba.fastjson.serializer.SerializerFeature
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.hive.HiveContext

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
    getCPersonCenInfoDetail(sc,hiveContext)

    sc.stop()
  }

  /**
    * 计算个人核心标签的数据
    * @param sc 上下文对象
    * @param sqlContext 上下文对象
    */
  def getCPersonCenInfoDetail(sc:SparkContext,sqlContext: HiveContext):Unit = {

    sqlContext.udf.register("notXing", (str: String) => {
      if (str != null && str.contains("*")) {
        0
      } else {
        1
      }
    })

    /**
      * 读取投保人
      */
    val odsHolderDetail =
      sqlContext.sql("select policy_id,holder_name,holder_cert_type,holder_cert_no from odsdb.ods_holder_detail")
        .where("holder_cert_type = 1 and length(holder_cert_no) = 18")
        .filter("notXing(holder_cert_no) = 1")
        .distinct()

    /**
      * 读取被保人表
      */
    val odsPolicyInsuredDetail =
      sqlContext.sql("select insured_id,insured_name,policy_id,insured_cert_type,insured_cert_no,start_date,end_date,company_name,work_type " +
        ",update_time from odsdb.ods_policy_insured_detail")
        .where("insured_cert_type = '1' and length(insured_cert_no) = 18")
        .distinct()

    /**
      * 读取从属被保人
      */
    val odsPolicyInsuredSlaveDetail =
      sqlContext.sql("select slave_name, master_id,slave_cert_type,slave_cert_no,start_date as start_date_slave,end_date as end_date_slave" +
        " from odsdb.ods_policy_insured_slave_detail")
        .where("slave_cert_type = '1' and length(slave_cert_no) = 18")
        .distinct()

    /**
      * 所有人员证件号union
      */
    val certNos = odsHolderDetail.selectExpr("holder_cert_no as cert_no")
      .unionAll(odsPolicyInsuredDetail.selectExpr("insured_cert_no as cert_no"))
      .unionAll(odsPolicyInsuredSlaveDetail.selectExpr("slave_cert_no as cert_no"))
      .distinct()

    /**
      * 读取产品表
      */
    val odsProductDetail = sqlContext.sql("select product_code as product_code_slave,product_name as product_name_slave,one_level_pdt_cate from odsdb.ods_product_detail")

    /**
      * 读取保单数据
      */
    val odsPolicyDetailTemp = sqlContext.sql("select holder_name as holder_name_slave,policy_id as policy_id_slave,policy_code,policy_status,product_code,product_name," +
      "first_premium,sum_premium,sku_coverage,policy_start_date,policy_end_date,channel_id,channel_name,policy_create_time,pay_way,belongs_regional," +
      "insure_company_name from odsdb.ods_policy_detail")

    val odsPolicyDetail =
      odsPolicyDetailTemp.join(odsProductDetail, odsPolicyDetailTemp("product_code") === odsProductDetail("product_code_slave"), "leftouter")
        .selectExpr("policy_id_slave", "policy_code", "holder_name_slave", "policy_status", "product_code", "product_name", "first_premium", "sum_premium",
          "sku_coverage", "policy_start_date", "policy_end_date", "channel_id", "channel_name", "policy_create_time", "pay_way", "belongs_regional",
          "insure_company_name")

    val holderPolicy = odsHolderDetail.join(odsPolicyDetail, odsHolderDetail("policy_id") === odsPolicyDetail("policy_id_slave"), "leftouter")
      .selectExpr("policy_id", "policy_code", "holder_name", "holder_cert_no", "policy_status", "product_code", "product_name", "first_premium", "sum_premium",
        "sku_coverage", "policy_start_date", "policy_end_date", "channel_id", "channel_name", "policy_create_time", "pay_way", "belongs_regional",
        "insure_company_name")

    /**
      * 读取职业码表
      */
    val odsWorktypeDimension = sqlContext.sql("select work_type as work_type_slave,level from odsdb.ods_worktype_dimension")

    /**
      * 读取地域码表
      */
    val odsAreaInfoDimension = sqlContext.sql("select code,short_name,province,city_type from odsdb.ods_area_info_dimension")

    /**
      * 读取理赔数据
      */
    val odsClaimsDetail =
      sqlContext.sql("select policy_no,case_no,risk_cert_no,pre_com,disable_level,case_status,final_payment from odsdb.ods_claims_detail")

    val  rowKeyName = "cert_no"

    val holderInfoNewDetailRes = HolderInfoNewDetail(sqlContext, holderPolicy, odsPolicyDetail, odsPolicyInsuredDetail,odsAreaInfoDimension)
//    toHBase(holderInfoNewDetailRes,"label_person_test","cent_info",rowKeyName)
//    putByList(sc,holderInfoNewDetailRes,"label_person_test","cent_info",rowKeyName)

    val holderInfoOldDetailRes = HolderInfoOldDetail(sqlContext, holderPolicy, odsPolicyDetail, odsPolicyInsuredDetail,odsAreaInfoDimension)
//    toHBase(holderInfoOldDetailRes,"label_person_test","cent_info",rowKeyName)
//    putByList(sc,holderInfoOldDetailRes,"label_person_test","cent_info",rowKeyName)

    val holderInfoEasyToLossDetailRes = HolderInfoEasyToLossDetail(sqlContext, holderPolicy, odsPolicyDetail, odsPolicyInsuredDetail)
//    toHBase(holderInfoEasyToLossDetailRes,"label_person_test","cent_info",rowKeyName)
//    putByList(sc,holderInfoEasyToLossDetailRes,"label_person_test","cent_info",rowKeyName)

    val InsuredInfoDetailRes = InsuredInfoDetail(sqlContext, holderPolicy, odsPolicyDetail, odsPolicyInsuredDetail, odsPolicyInsuredSlaveDetail,
      odsWorktypeDimension, certNos, odsClaimsDetail)
//    toHBase(InsuredInfoDetailRes,"label_person_test","cent_info",rowKeyName)
//    putByList(sc,InsuredInfoDetailRes,"label_person_test","cent_info",rowKeyName)

    val tatentInsuredInfoDetailRes = tatentInsuredInfoDetail(sqlContext, holderPolicy, odsPolicyDetail, odsPolicyInsuredDetail,
      odsPolicyInsuredSlaveDetail, certNos)
//    toHBase(tatentInsuredInfoDetailRes,"label_person_test","cent_info",rowKeyName)
//    putByList(sc,tatentInsuredInfoDetailRes,"label_person_test","cent_info",rowKeyName)


    holderInfoNewDetailRes.printSchema()
    holderInfoOldDetailRes.printSchema()
    holderInfoEasyToLossDetailRes.printSchema()
    InsuredInfoDetailRes.printSchema()
    tatentInsuredInfoDetailRes.printSchema()

  }

  /**
    * 当前生效报单数,参保产品code和产品名称
    * @param sqlContext                  上下文
    * @param holderPolicy                投保人保单信息
    * @param odsPolicyDetail             保单信息
    * @param odsPolicyInsuredDetail      被保人保单信息
    * @param odsPolicyInsuredSlaveDetail 从属被保人信息
    * @return
    */
  def tatentInsuredInfoDetail(sqlContext: HiveContext, holderPolicy: DataFrame, odsPolicyDetail: DataFrame, odsPolicyInsuredDetail: DataFrame,
                              odsPolicyInsuredSlaveDetail: DataFrame, certNos: DataFrame): DataFrame = {
    import sqlContext.implicits._
    /**
      * 被保人
      */
    val policyInsured = odsPolicyDetail.join(odsPolicyInsuredDetail, odsPolicyDetail("policy_id_slave") === odsPolicyInsuredDetail("policy_id"))
      .selectExpr("policy_code", "policy_status", "product_code", "product_name", "insured_id", "insured_cert_no","channel_name")


    /**
      * 从属被保人
      */
    val policySlave = policyInsured.join(odsPolicyInsuredSlaveDetail, policyInsured("insured_id") === odsPolicyInsuredSlaveDetail("master_id"))
      .selectExpr("policy_code", "policy_status", "product_code", "product_name", "insured_id", "slave_cert_no","channel_name")

    /**
      * 投保人
      */
    val policyHolder = holderPolicy.where("policy_status in (1,0,-1)")
      .selectExpr("policy_code", "holder_cert_no")

    /**
      * 投被保人生效保单数
      */
    val effectPolicyCun = policyInsured.where("policy_status in (1,0,-1)").selectExpr("policy_code", "insured_cert_no as cert_no")
      .unionAll(policySlave.where("policy_status in (1,0,-1)").selectExpr("policy_code", "slave_cert_no as cert_no"))
      .unionAll(policyHolder.selectExpr("policy_code", "holder_cert_no as cert_no"))
      .distinct()
      .map(x => {
        val certNo = x.getAs[String]("cert_no")
        (certNo, 1)
      })
      .reduceByKey(_ + _)
      .map(x => (x._1, x._2))
      .toDF("all_cert_no", "effect_policy_cun")

    /**
      * 参保产品code和产品名称
      */
    val latentProductInfo = policyInsured.selectExpr("product_code", "product_name", "insured_cert_no as cert_no")
      .unionAll(policySlave.selectExpr("product_code", "product_name", "slave_cert_no as cert_no"))
      .distinct()
      .map(x => {
        val certNo = x.getAs[String]("cert_no")
        val productCode = x.getAs[String]("product_code")
        val productName = x.getAs[String]("product_name")
        (certNo, (productCode, productName))
      })
      .groupByKey()
      .map(x => {
        val certNo = x._1
        val value: Iterable[(String, String)] = x._2
        val strRes: String = if (value.isEmpty) {
          null
        } else {
          val valueRes:List[(String, String)] = value.toList.distinct
          val ss = new util.ArrayList[(String, String)]
          valueRes.foreach(x => {
            ss.add(x)
          })
          val jsonString = JSON.toJSONString(ss, SerializerFeature.BeanToArray)
          jsonString
        }
        (certNo, strRes)
      })
      .toDF("cert_no_slave", "latent_product_info")

    /**
      * 客户来源
      */
    val threeData =
      policyInsured.selectExpr("insured_cert_no as slave_cert_no","channel_name").unionAll(policySlave.selectExpr("slave_cert_no","channel_name"))
        .distinct()
        .map(x => {
          val slaveCertNo = x.getAs[String]("slave_cert_no")
          val channelName = x.getAs[String]("channel_name")
          (slaveCertNo,channelName)
        })
        .groupByKey()
        .map(x => {
          val resTemp = x._2
          val str = if(resTemp.isEmpty){
            null
          }else{
            val values: Set[String] = x._2.toSet
            val ss = new util.ArrayList[String]
            values.foreach(x => {
              if(x != "" && x!= "null"){
                ss.add(x)
              }else{
                null
              }
            })
            val jsonString = JSON.toJSONString(ss, SerializerFeature.BeanToArray)
            jsonString
          }
          (x._1,str)
        })
        .toDF("all_cert_no","cus_source")

    val oneRes = certNos.join(effectPolicyCun, certNos("cert_no") === effectPolicyCun("all_cert_no"), "leftouter")
      .selectExpr("cert_no", "effect_policy_cun")

    val twoRes = oneRes.join(latentProductInfo, oneRes("cert_no") === latentProductInfo("cert_no_slave"), "leftouter")
      .selectExpr("cert_no", "effect_policy_cun", "latent_product_info")

    val res = twoRes.join(threeData,twoRes("cert_no")===threeData("all_cert_no"),"leftouter")
      .selectExpr("cert_no", "effect_policy_cun", "latent_product_info","cus_source")

    res
  }

  /**
    * 投/被保人数据信息
    *
    * @param sqlContext                  上下文
    * @param holderPolicy                投保保单和投保人信息表
    * @param odsPolicyDetail             保单明细表
    * @param odsPolicyInsuredDetail      在保人明细表
    * @param odsPolicyInsuredSlaveDetail 被保人明细表
    * @param odsWorktypeDimension        工种码表
    */
  def InsuredInfoDetail(sqlContext: HiveContext, holderPolicy: DataFrame, odsPolicyDetail: DataFrame, odsPolicyInsuredDetail: DataFrame,
                        odsPolicyInsuredSlaveDetail: DataFrame, odsWorktypeDimension: DataFrame, certNos: DataFrame, odsClaimsDetail: DataFrame): DataFrame = {
    import sqlContext.implicits._

    //被保人详细信息 与 工种进行关联信息
    val odsPolicyInsuredDetailOne =
      odsPolicyInsuredDetail.join(odsWorktypeDimension, odsPolicyInsuredDetail("work_type") === odsWorktypeDimension("work_type_slave"), "leftouter")
        .selectExpr("insured_id", "insured_cert_no", "start_date", "end_date", "update_time", "company_name", "work_type", "level")

    //从属被保人信息 没有在被保人信息表中出现的数据
    val odsPolicyInsuredSlaveDetailOne =
      odsPolicyInsuredSlaveDetail.join(odsPolicyInsuredDetail, odsPolicyInsuredSlaveDetail("slave_cert_no") === odsPolicyInsuredDetail("insured_cert_no"), "leftouter")
        .where("insured_cert_no is null")
        .selectExpr("master_id", "slave_cert_no", "start_date_slave", "end_date_slave", "'是' as is_join_policy", "'0' as cus_type")

    /**
      * 客户类型  投保人和被保人做全关联  如果投保人和被保人同事存在  为新客  如果投保人存在被保人不存在  也是新客  只有被保人存在就是潜在
      * 0 潜在
      * 1 新客
      * 2 老客
      * 3 易流失
      * 4 流失
      * 5 沉睡
      */
    val cusTypeTemp = holderPolicy.join(odsPolicyInsuredDetail,holderPolicy("holder_cert_no")===odsPolicyInsuredDetail("insured_cert_no"),"fullouter")
      .selectExpr("holder_cert_no","insured_cert_no")
      .distinct()

    val cusTypeOne = cusTypeTemp.where("holder_cert_no is not null").selectExpr("holder_cert_no","'1' as cus_type")
    val cusTypeTwo = cusTypeTemp.where("holder_cert_no is null and insured_cert_no is not null").selectExpr("insured_cert_no as holder_cert_no","'0' as cus_type")

    val cusType =  cusTypeOne.unionAll(cusTypeTwo)
    /**
      * 被保人职业信息
      * 所在单位
      * 职业风险等级
      * 是否参保
      */
    val odsPolicyInsuredDetailOneRes = odsPolicyInsuredDetailOne.map(x => {
      val insuredCertNo = x.getAs[String]("insured_cert_no")
      var startDate = x.getAs[String]("start_date")
      val updateTime = x.getAs[String]("update_time")
      if (startDate == null) {
        startDate = updateTime
      }
      val endDate = x.getAs[String]("end_date")
      val companyName = x.getAs[String]("company_name")
      val workType = x.getAs[String]("work_type")
      val level = x.getAs[Int]("level")
      (insuredCertNo, (startDate, endDate, companyName, workType, level))
    })
      .reduceByKey((x1, x2) => {
        val res = if (x1._1.compareTo(x2._1) >= 0) x1 else x2
        res
      })
      .map(x => {
        //证件号  当前所在公司 当前工种  当前工种级别
        (x._1, x._2._3, x._2._4, x._2._5, "是")
      })
      .toDF("insured_cert_no", "now_company_name", "now_profession_name", "now_profession_risk_level", "is_join_policy")
    //    odsPolicyInsuredDetailOneRes.show()
    /**
      * 累计参保次数
      * 是否在保
      */
    //当前时间
    val currDate = get_current_date(System.currentTimeMillis()).toString.substring(0, 19)

    val odsPolicyInsuredDetailTwoRes = odsPolicyInsuredDetailOne.map(x => {
      val insuredCertNo = x.getAs[String]("insured_cert_no")
      var startDate = x.getAs[String]("start_date")
      val updateTime = x.getAs[String]("update_time")
      if (startDate == null) {
        startDate = updateTime
      }
      var endDate = x.getAs[String]("end_date")
      if (endDate == null) {
        endDate = startDate
      }
      (insuredCertNo, (startDate, endDate))
    })
      .map(x => {
        if ((x._2._1.compareTo(currDate) <= 0) && (x._2._2.compareTo(currDate) >= 0)) {
          (x._1, (1, 1))
        } else {
          (x._1, (0, 1))
        }
      })
      .reduceByKey((x1, x2) => {
        val isInsuredCount = x1._1 + x2._1 //在保数 如果在保数》0 说明又在保的保单
        val policyInsuredCun = x1._2 + x2._2 //总的参保次数
        (isInsuredCount, policyInsuredCun)
      })
      .map(x => {
        if (x._2._1 > 0) {
          (x._1, "在保", x._2._2)
        } else {
          (x._1, "不在保", x._2._2)
        }
      })
      .toDF("insured_cert_no", "is_insured", "policy_insured_cun")
    //    odsPolicyInsuredDetailTwoRes.show()

    val odsPolicyInsuredSlaveDetailOneRes = odsPolicyInsuredSlaveDetailOne.map(x => {
      val insuredCertNo = x.getAs[String]("slave_cert_no")
      val startDate = x.getAs[java.sql.Timestamp]("start_date_slave")
      val endDate = x.getAs[java.sql.Timestamp]("end_date_slave")
      val isJoinPolicy = x.getAs[String]("is_join_policy")
      val cusType = x.getAs[String]("cus_type")
      ((insuredCertNo, isJoinPolicy, cusType), (startDate, endDate))
    })
      .map(x => {
        if (x._2._1 != null && x._2._2 != null) {
          if ((x._2._1.toString.compareTo(currDate) <= 0) && (x._2._2.toString.compareTo(currDate) >= 0)) {
            (x._1, (1, 1))
          } else {
            (x._1, (0, 1))
          }
        } else {
          (x._1, (0, 1))
        }
      })
      .reduceByKey((x1, x2) => {
        val isInsuredCount = x1._1 + x2._1
        val policyInsuredCun = x1._2 + x2._2
        (isInsuredCount, policyInsuredCun)
      })
      .map(x => {
        if (x._2._1 > 0) {
          (x._1._1, "在保", x._2._2, x._1._2, x._1._3)
        } else {
          (x._1._1, "不在保", x._2._2, x._1._2, x._1._3)
        }
      })
      .toDF("insured_cert_no", "is_insured_slave", "policy_insured_cun_slave", "is_join_policy_slave", "cus_type_slave")

    val oneRes = certNos.join(cusType, certNos("cert_no") === cusType("holder_cert_no"), "leftouter")
      .selectExpr("cert_no", "cus_type")

    val twoRes = oneRes.join(odsPolicyInsuredDetailOneRes, oneRes("cert_no") === odsPolicyInsuredDetailOneRes("insured_cert_no"), "leftouter")
      .selectExpr("cert_no", "cus_type", "now_profession_name", "now_company_name", "now_profession_risk_level", "is_join_policy")

    val threeRes = twoRes.join(odsPolicyInsuredDetailTwoRes, twoRes("cert_no") === odsPolicyInsuredDetailTwoRes("insured_cert_no"), "leftouter")
      .selectExpr("cert_no", "cus_type", "now_profession_name", "now_company_name", "now_profession_risk_level",
        "is_join_policy", "is_insured", "policy_insured_cun")

    val resTemp = threeRes.join(odsPolicyInsuredSlaveDetailOneRes, threeRes("cert_no") === odsPolicyInsuredSlaveDetailOneRes("insured_cert_no"), "leftouter")
      .selectExpr("cert_no", "case when cus_type_slave is not null then cus_type_slave else cus_type end as cus_type", "now_profession_name", "now_company_name", "now_profession_risk_level",
        "case when insured_cert_no is not null then is_join_policy_slave else is_join_policy end as is_join_policy",
        "case when insured_cert_no is not null then is_insured_slave else is_insured end as is_insured",
        "case when insured_cert_no is not null then policy_insured_cun_slave else policy_insured_cun end as policy_insured_cun")

    /**
      * 累计出险次数
      * 累计赔付保费
      */
    val claimOne =
      odsClaimsDetail.selectExpr("risk_cert_no", "pre_com", "final_payment")
        .map(x => {
          val riskCertNo = x.getAs[String]("risk_cert_no")
          val preCom = x.getAs[String]("pre_com")
          var preComRes = 0.0
          if (preCom != null) {
            preComRes = preCom.toDouble
          }
          val finalPayment = x.getAs[String]("final_payment")
          var finalPaymentRes = 0.0

          if (finalPayment != null && finalPayment != "") {
            finalPaymentRes = finalPayment.toDouble
          } else {
            finalPaymentRes = preComRes
          }
          (riskCertNo, (finalPaymentRes, 1))
        })
        .reduceByKey((x1, x2) => {
          val prePremium = BigDecimal.valueOf(x1._1).setScale(4, RoundingMode.HALF_UP).+(BigDecimal.valueOf(x2._1).setScale(4, RoundingMode.HALF_UP))
          val riskCount = x1._2 + x2._2
          (prePremium.doubleValue(), riskCount)
        })
        .map(x => (x._1, x._2._1, x._2._2))
        .toDF("risk_cert_no", "pre_premium_sum", "risk_cun")

    /**
      * 拒赔案件数
      * 撤案案件数
      */
    val claimTwo = odsClaimsDetail.selectExpr("risk_cert_no", "case_status")
      .where("case_status in ('拒赔','撤案')")
      .map(x => {
        val riskCertNo = x.getAs[String]("risk_cert_no")
        val caseStatus = x.getAs[String]("case_status")

        ((riskCertNo, caseStatus), 1)
      })
      .reduceByKey(_ + _)
      .map(x => {
        (x._1._1, (x._1._2, x._2))
      })
      .groupByKey()
      .map(x => {
        val res = if (x._2.isEmpty) {
          (x._1, -1, -1)
        } else {
          val mapRes = x._2.toMap
          val rejectClaimCun = mapRes.getOrElse("拒赔", -1)
          val withdrawClaimCun = mapRes.getOrElse("撤案", -1)
          (x._1, rejectClaimCun, withdrawClaimCun)
        }
        res
      })
      .map(x => (x._1, x._2, x._3))
      .toDF("risk_cert_no", "reject_claim_cun", "withdraw_claim_cun")
      .selectExpr("risk_cert_no", "case when reject_claim_cun = -1 then null else reject_claim_cun end as reject_claim_cun",
        "case when withdraw_claim_cun = -1 then null else withdraw_claim_cun end as withdraw_claim_cun")

    val resTempRes = resTemp.join(claimOne, resTemp("cert_no") === claimOne("risk_cert_no"), "leftouter")
      .selectExpr("cert_no", "cus_type", "now_profession_name", "now_company_name", "now_profession_risk_level", "is_join_policy", "is_insured",
        "policy_insured_cun", "pre_premium_sum", "risk_cun")

    val res = resTempRes.join(claimTwo, resTempRes("cert_no") === claimTwo("risk_cert_no"), "leftouter")
      .selectExpr("cert_no", "cus_type", "now_profession_name", "now_company_name", "now_profession_risk_level", "is_join_policy", "is_insured",
        "policy_insured_cun", "pre_premium_sum", "risk_cun", "reject_claim_cun", "withdraw_claim_cun")

    res
  }

  /**
    * 投保人易流失阶段数据
    *
    * @param sqlContext             上下文
    * @param holderPolicy           投保保单和投保人信息表
    * @param odsPolicyDetail        保单明细表
    * @param odsPolicyInsuredDetail 在保人明细表
    */
  def HolderInfoEasyToLossDetail(sqlContext: HiveContext, holderPolicy: DataFrame, odsPolicyDetail: DataFrame, odsPolicyInsuredDetail: DataFrame): DataFrame = {
    import sqlContext.implicits._

    //保险止期
    val newResOne = holderPolicy.map(x => {
      val holderCert_no = x.getAs[String]("holder_cert_no")
      val policyEndDate = x.getAs[java.sql.Timestamp]("policy_end_date")
      (holderCert_no, policyEndDate)
    })
      //过滤止期为空的数据
      .filter(x => x._2 != null)
      .reduceByKey((x1, x2) => {
        val policyEndDate = if (x1.compareTo(x2) >= 0) x1 else x2
        policyEndDate
      })
      .map(x => (x._1, x._2))
      .toDF("cert_no", "last_policy_end_date")

    newResOne
  }

  /**
    * 投保人老客阶段
    *
    * @param sqlContext             上下文
    * @param holderPolicy           投保保单和投保人信息表
    * @param odsPolicyDetail        保单明细表
    * @param odsPolicyInsuredDetail 在保人明细表
    */
  def HolderInfoOldDetail(sqlContext: HiveContext, holderPolicy: DataFrame, odsPolicyDetail: DataFrame, odsPolicyInsuredDetail: DataFrame,
                          odsAreaInfoDimension:DataFrame): DataFrame = {
    import sqlContext.implicits._

    //近90天时间
    val currDateDelNintyDay = Timestamp.valueOf(dateDelNintyDay(get_current_date(System.currentTimeMillis()).toString.substring(0, 19)))

    //当前时间
    val currDate = get_current_date(System.currentTimeMillis()).toString.substring(0, 19)

    //投保信息
    val newResOne = holderPolicy.map(x => {
      val holderCert_no = x.getAs[String]("holder_cert_no")
      var policyStartDate = x.getAs[java.sql.Timestamp]("policy_start_date")
      val policyEndDate = x.getAs[java.sql.Timestamp]("policy_end_date")
      val policyCreateTime = x.getAs[java.sql.Timestamp]("policy_create_time")
      val productCode = x.getAs[String]("product_code")
      val productName = x.getAs[String]("product_name")
      val belongsRegional = x.getAs[String]("belongs_regional")
      if (policyStartDate == null) {
        policyStartDate = policyCreateTime
      }
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
      (holderCert_no, (policyStartDate, policyEndDate, productCode, productName, province, city))
    })
      .reduceByKey((x1, x2) => {
        val res = if (x1._1.compareTo(x2._1) >= 0) x1 else x2
        res
      })
      .map(x => {
        //证件号 开始时间 结束时间 产品code，产品名称  省份 城市  最新保单距今天数
        if (x._2._1 != null) {
          (x._1, x._2._1, x._2._2, x._2._3, x._2._4, x._2._6,
            getBeg_End_one_two_new(x._2._1.toString.substring(0, 19), currDate))
        } else {
          (x._1, x._2._1, x._2._2, x._2._3, x._2._4, x._2._6, 0L)
        }
      })
      .toDF("holder_cert_no", "last_policy_date", "policy_end_date", "last_policy_product_code", "last_policy_product_name","now_city",
        "last_policy_days")

    //近90天的数据
    val newResTwo = holderPolicy.map(x => {
      val holderCert_no = x.getAs[String]("holder_cert_no")
      val firstPremium = x.getAs[Double]("first_premium")
      val sumPremium = x.getAs[Double]("sum_premium")
      var policyStartDate = x.getAs[java.sql.Timestamp]("policy_start_date")
      val policyCreateTime = x.getAs[java.sql.Timestamp]("policy_create_time")
      if (policyStartDate == null) {
        policyStartDate = policyCreateTime
      }
      val policyEndDate = x.getAs[java.sql.Timestamp]("policy_end_date")
      (holderCert_no, (policyStartDate, policyEndDate, firstPremium, sumPremium, currDateDelNintyDay, 1))
    })
      .filter(x => if (x._2._5.compareTo(x._2._1) <= 0) true else false)
      .map(x => {
        (x._1, (x._2._4, x._2._6))
      })
      .reduceByKey((x1, x2) => {
        val count: Int = x1._2 + x2._2
        val sumPremium = BigDecimal.valueOf(x1._1).setScale(4, RoundingMode.HALF_UP).+(BigDecimal.valueOf(x2._1).setScale(4, RoundingMode.HALF_UP))
        (sumPremium.doubleValue(), count)
      })
      .map(x => (x._1, x._2._1, x._2._2, BigDecimal.valueOf(x._2._1 / x._2._2).setScale(4, RoundingMode.HALF_UP).doubleValue()))
      .toDF("holder_cert_no_slave", "ninety_policy_premium", "ninety_policy_cun", "ninety_premium_avg")

    //累计数据信息
    val newResThree = holderPolicy.map(x => {
      val holderCert_no = x.getAs[String]("holder_cert_no")
      val sumPremium = x.getAs[Double]("sum_premium")
      (holderCert_no, (sumPremium, 1))
    })
      .reduceByKey((x1, x2) => {
        val count = x1._2 + x2._2
        val sumPremium = BigDecimal.valueOf(x1._1).setScale(4, RoundingMode.HALF_UP).+(BigDecimal.valueOf(x2._1).setScale(4, RoundingMode.HALF_UP))
        (sumPremium.doubleValue, count)
      })
      .map(x => (x._1, x._2._1, x._2._2, BigDecimal.valueOf(x._2._1 / x._2._2).setScale(4, RoundingMode.HALF_UP).doubleValue()))
      .toDF("holder_cert_no_slave", "policy_premium", "policy_cun", "premium_avg")

    val oneRes = newResOne.join(newResTwo, newResOne("holder_cert_no") === newResTwo("holder_cert_no_slave"), "leftouter")
      .selectExpr("holder_cert_no", "last_policy_date", "policy_end_date", "last_policy_product_code", "last_policy_product_name","now_city",
        "last_policy_days", "ninety_policy_premium", "ninety_policy_cun", "ninety_premium_avg")

    val resTwo = oneRes.join(newResThree, oneRes("holder_cert_no") === newResThree("holder_cert_no_slave"), "leftouter")
      .selectExpr("holder_cert_no", "last_policy_date", "last_policy_product_code", "last_policy_product_name", "now_city", "last_policy_days",
        "ninety_policy_premium", "ninety_policy_cun", "ninety_premium_avg", "policy_premium", "policy_cun", "premium_avg")

    val res = resTwo.join(odsAreaInfoDimension,resTwo("now_city")===odsAreaInfoDimension("code"),"leftouter")
      .selectExpr("holder_cert_no as cert_no", "last_policy_date", "last_policy_product_code", "last_policy_product_name", "province as now_province", "short_name as now_city",
        "last_policy_days","ninety_policy_premium", "ninety_policy_cun", "ninety_premium_avg", "policy_premium", "policy_cun", "premium_avg")

    res
  }

  /**
    * 投保人新客标签数据处理
    *
    * @param holderPolicy           投保保单和投保人信息表
    * @param odsPolicyDetail        保单明细表
    * @param odsPolicyInsuredDetail 在保人明细表
    */
  def HolderInfoNewDetail(sQLContext: HiveContext, holderPolicy: DataFrame, odsPolicyDetail: DataFrame, odsPolicyInsuredDetail: DataFrame,
                          odsAreaInfoDimension:DataFrame): DataFrame = {
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
      var policyStartDate = x.getAs[java.sql.Timestamp]("policy_start_date")
      val policyEndDate = x.getAs[java.sql.Timestamp]("policy_end_date")
      val channelId = x.getAs[String]("channel_id")
      val channelName = x.getAs[String]("channel_name")
      val policyCreateTime = x.getAs[java.sql.Timestamp]("policy_create_time")
      val paymentType = x.getAs[Int]("pay_way")
      val belongsRegional = x.getAs[String]("belongs_regional")
      val insureCompanyName = x.getAs[String]("insure_company_name")
      if (policyStartDate == null) {
        policyStartDate = policyCreateTime
      }
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
      (holderCert_no, (policyId, policyCode, holderName, policyStatus, productCode, productName, firstPremium, sumPremium, skuCoverage,
        policyStartDate, policyEndDate, channelId, channelName, policyCreateTime, paymentType, province, city, insureCompanyName))
    })
      //对证件号进行分组，得到首次投保的保单信息
      .reduceByKey((x1, x2) => {
      val res = if (x1._10.compareTo(x2._10) < 0) x1 else x2
      res
    })
      //如果开始时间是空  就用创建时间 作为首次投保时间
      .map(x => {
      val cert_no = x._1
      val policyStartDate = x._2._10
      val firstPolicyTime = policyStartDate
      //首次投保+90d
      val first_policy_time_90_days =
        if (firstPolicyTime != null) {
          Timestamp.valueOf(dateAddNintyDay(firstPolicyTime.toString))
        } else {
          null
        }
      //首次投保距今天的天数
      val firstPolicyDays = if (firstPolicyTime != null) {
        getBeg_End_one_two_new(firstPolicyTime.toString, get_current_date(System.currentTimeMillis()).toString.substring(0, 19))
      } else {
        -1
      }
      //年龄
      var age: Int = 0
      if (firstPolicyTime != null) {
        age = getAgeFromBirthTime(cert_no, firstPolicyTime.toString)
      }
      val firstPolicySection = firstPolicyTime+"-"+x._2._11
      (x._1, x._2._1, x._2._3, x._2._4, x._2._5, x._2._6, x._2._7, x._2._8, x._2._9, x._2._10, x._2._11, x._2._12, x._2._13, x._2._14, x._2._15,
        x._2._16, x._2._17, x._2._18, first_policy_time_90_days, age, firstPolicyDays,firstPolicySection)
    })
      .toDF("holder_cert_no", "policy_id", "holder_name", "policy_status", "first_policy_pdt_code", "first_policy_pdt_name","first_policy_premium",
        "sum_premium","first_policy_plan", "first_policy_time", "policy_end_date", "channel_id", "first_policy_source","policy_create_time",
        "first_policy_pay_channel", "province", "city","first_policy_insurant_name", "first_policy_time_90_days", "first_policy_age", "first_policy_days",
        "first_policy_section")

    val odsPolicyDetailTemp = odsPolicyDetail.selectExpr("policy_id_slave", "holder_name_slave", "sum_premium", "policy_start_date")

    //新客阶段购买次数 和  累计保费
    val newResTwo = newResOne.selectExpr("holder_cert_no", "policy_id", "holder_name", "first_policy_time_90_days")
      .join(odsPolicyDetailTemp, newResOne("policy_id") === odsPolicyDetailTemp("policy_id_slave"), "leftouter")
      .where("first_policy_time_90_days >= policy_start_date")
      .selectExpr("holder_cert_no", "policy_id", "sum_premium")
      .map(x => {
        val holderCert_no = x.getAs[String]("holder_cert_no")
        val sumPremium = x.getAs[Double]("sum_premium")
        val sumPremiumDecimal = BigDecimal.valueOf(sumPremium)
        (holderCert_no, (sumPremiumDecimal, 1))
      })
      .reduceByKey((x1, x2) => {
        val sumPremiumDecimal = x1._1.+(x2._1)
        val firstCount = x1._2 + x2._2
        (sumPremiumDecimal, firstCount)
      })
      .map(x => {
        (x._1, x._2._1.setScale(4, BigDecimal.RoundingMode.HALF_UP).doubleValue(), x._2._2)
      })
      .toDF("holder_cert_no_slave1", "new_cus_sum_premium", "new_cus_buy_cun")

    //新客阶段累计参保次数
    val odsPolicyInsuredDetailTemp = odsPolicyInsuredDetail.selectExpr("insured_cert_no", "start_date")

    val newResThree = newResOne.selectExpr("holder_cert_no", "policy_id", "holder_name", "first_policy_time_90_days")
      .join(odsPolicyInsuredDetailTemp, newResOne("holder_cert_no") === odsPolicyInsuredDetailTemp("insured_cert_no"))
      .where("first_policy_time_90_days >= start_date")
      .selectExpr("holder_cert_no")
      .map(x => (x.getAs[String]("holder_cert_no"), 1))
      .reduceByKey(_ + _)
      .map(x => (x._1, x._2))
      .toDF("holder_cert_no_slave2", "new_cus_insured_cun")

    val resOne = newResOne.join(newResTwo, newResOne("holder_cert_no") === newResTwo("holder_cert_no_slave1"), "leftouter")
      .selectExpr("holder_cert_no","first_policy_pdt_code", "first_policy_pdt_name","first_policy_premium",
        "first_policy_plan", "first_policy_time","first_policy_source","policy_create_time", "first_policy_pay_channel",
        "city","first_policy_insurant_name", "first_policy_time_90_days", "first_policy_age", "new_cus_buy_cun", "new_cus_sum_premium",
        "case when first_policy_days = -1 then null else first_policy_days end as first_policy_days","first_policy_section")

    val resTwo = resOne.join(newResThree, resOne("holder_cert_no") === newResThree("holder_cert_no_slave2"), "leftouter")
      .selectExpr("holder_cert_no","first_policy_pdt_code", "first_policy_pdt_name","first_policy_premium",
        "first_policy_plan", "first_policy_time","first_policy_source","policy_create_time", "first_policy_pay_channel",
        "city","first_policy_insurant_name", "first_policy_time_90_days", "first_policy_age", "new_cus_buy_cun", "new_cus_sum_premium",
        "first_policy_days","first_policy_section","new_cus_insured_cun")

    val res = resTwo.join(odsAreaInfoDimension,resTwo("city")===odsAreaInfoDimension("code"),"leftouter")
      .selectExpr("holder_cert_no as cert_no","first_policy_pdt_code","first_policy_pdt_name","first_policy_premium","first_policy_plan", "first_policy_time","first_policy_source",
        "first_policy_pay_channel","short_name as first_policy_city","province as first_policy_province","city_type as first_policy_city_level","first_policy_insurant_name",
        "first_policy_time_90_days", "first_policy_age", "new_cus_buy_cun","new_cus_sum_premium","first_policy_days","first_policy_section","new_cus_insured_cun")

    res
  }
}