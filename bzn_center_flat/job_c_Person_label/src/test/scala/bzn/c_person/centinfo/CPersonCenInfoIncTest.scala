package bzn.c_person.centinfo

import java.sql.Timestamp
import java.util

import bzn.job.common.{HbaseUtil, Until}
import c_person.util.SparkUtil
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.math.BigDecimal.RoundingMode

/**
  * author:xiaoYuanRen
  * Date:2019/7/10
  * Time:13:39
  * describe: c端核心标签清洗
  **/
object CPersonCenInfoIncTest extends SparkUtil with Until with HbaseUtil{
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "local[*]")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    import hiveContext.implicits._
    /**
      * 读取hbase上的数据
      */
    val hbaseData: DataFrame = getHbaseBussValue(sc,"label_person")
      .map(x => {
        val key = Bytes.toString(x._2.getRow)
        val cusType = Bytes.toString(x._2.getValue("cent_info".getBytes, "cus_type".getBytes))
        val newCusBuyCun = Bytes.toString(x._2.getValue("cent_info".getBytes, "new_cus_buy_cun".getBytes))
        val newCusInsuredCun = Bytes.toString(x._2.getValue("cent_info".getBytes, "new_cus_insured_cun".getBytes))
        val newCusSumPremium = Bytes.toString(x._2.getValue("cent_info".getBytes, "new_cus_sum_premium".getBytes))
        val policyCun = Bytes.toString(x._2.getValue("cent_info".getBytes, "policy_cun".getBytes))
        val policyInsuredCun = Bytes.toString(x._2.getValue("cent_info".getBytes, "policy_insured_cun".getBytes))
        val policyPremium = Bytes.toString(x._2.getValue("cent_info".getBytes, "policy_premium".getBytes))
        val premiumAvg = Bytes.toString(x._2.getValue("cent_info".getBytes, "premium_avg".getBytes))
        val lastPolicyEndDate = Bytes.toString(x._2.getValue("cent_info".getBytes, "last_policy_end_date".getBytes))
        val cusSource = Bytes.toString(x._2.getValue("cent_info".getBytes, "cus_source".getBytes))
        val latentProductInfo = Bytes.toString(x._2.getValue("cent_info".getBytes, "latent_product_info".getBytes))
        val lastCusType = Bytes.toString(x._2.getValue("high_info".getBytes, "last_cus_type".getBytes)) //前一次投保类型
        val becomeCurrCusTime = Bytes.toString(x._2.getValue("high_info".getBytes, "become_curr_cus_time".getBytes))
        (key,cusType,newCusBuyCun,newCusInsuredCun,newCusSumPremium,policyCun,policyInsuredCun,policyPremium,premiumAvg,lastPolicyEndDate,
          cusSource,latentProductInfo,lastCusType,becomeCurrCusTime)
      })
      .toDF("cert_no","cus_type","new_cus_buy_cun","new_cus_insured_cun","new_cus_sum_premium","policy_cun","policy_insured_cun",
        "policy_premium","premium_avg","last_policy_end_date","cus_source","latent_product_info","last_cus_type","become_curr_cus_time")
      .where("cus_type in ('0','1','2','3','4','5')")

    getIncCPersonCenInfoDetail(hbaseData,sc,hiveContext)

    sc.stop()
  }

  /**
    * 增量数据标签
    * @param hbaseData //hbase 数据
    * @param sc 上下文
    * @param sqlContext sql上下文
    */
  def getIncCPersonCenInfoDetail(hbaseData:DataFrame,sc:SparkContext,sqlContext: HiveContext) ={

    sqlContext.udf.register("getAge",(certNo:String,dateTime:String) => {
      val age = getAgeFromBirthTime(certNo,dateTime)
      age
    })

    /**
      * 变成老客的时间
      */
    sqlContext.udf.register("getBecomeOldTime",(firstPolicyTime:String) => {
      val becomeOldTime = if(firstPolicyTime != null){
        dateAddNintyDay(firstPolicyTime.toString)
      }else{
        null
      }
      becomeOldTime
    })

    /**
      * 新客阶段购买数量
      */
    sqlContext.udf.register("getNewCusBuyCun",(certNo:String,newCusBuyCun:String,cusType:String) => {
      val newCusBuyCunRes = if(certNo == null || cusType == "0"){
        1
      }else if(cusType == "1"){
        if(newCusBuyCun == null){
          1
        }else{
          newCusBuyCun.toInt+1
        }
      }else{
        newCusBuyCun
      }
      newCusBuyCunRes.toString
    })

    /**
      * 获取增量信息
      */
    val  dwPolicyDetailIncTemp =
      sqlContext.sql("select policy_id,policy_status,first_premium,sum_premium,product_code,product_name,policy_start_date,policy_end_date," +
        "channel_name,insure_company_name,sku_coverage,pay_way,concat(substr(belongs_regional,1,4),'00') as belongs_regional,inc_type from dwdb.dw_policy_detail_inc")
      .where("policy_status in (0,1,-1) and inc_type = 0")
      .cache()

    /**
      * 读取产品信息
      */
    val odsProductDetail =
      sqlContext.sql("select product_code as product_code_slave,product_name as product_name_slave,one_level_pdt_cate from odsdb.ods_product_detail")

    val dwPolicyDetailInc = dwPolicyDetailIncTemp.join(odsProductDetail,dwPolicyDetailIncTemp("product_code")===odsProductDetail("product_code_slave"),"leftouter")
      .selectExpr("policy_id","policy_status","first_premium","sum_premium","product_code","product_name_slave as product_name","policy_start_date","policy_end_date",
        "channel_name","insure_company_name","sku_coverage","pay_way","belongs_regional","inc_type")

    /**
      * 读取所有保单信息
      */
    val  odsPolicyDetail =
      sqlContext.sql("select policy_id,policy_status,first_premium,sum_premium,product_code,product_name,policy_start_date,policy_end_date," +
        "channel_name,insure_company_name,sku_coverage,pay_way,concat(substr(belongs_regional,1,4),'00') as belongs_regional from odsdb.ods_policy_detail")
        .where("policy_status in (0,1,-1)")
        .cache()

    /**
      * 读取地域码表
      */
    val odsAreaInfoDimension = sqlContext.sql("select code,short_name,province,city_type from odsdb.ods_area_info_dimension")

    val dwPolicyDetailIncRes = dwPolicyDetailInc.join(odsAreaInfoDimension,dwPolicyDetailInc("belongs_regional")===odsAreaInfoDimension("code"),"leftouter")
      .selectExpr("policy_id","policy_status","first_premium","sum_premium","product_code","product_name","policy_start_date","policy_end_date",
        "channel_name","insure_company_name","sku_coverage","pay_way","short_name as city","province","city_type")

    /**
      * 读取理赔表
      */
    val odsClaimsDetail = sqlContext.sql("select policy_no,risk_cert_no,pre_com,final_payment,case_status from odsdb.ods_claims_detail")

    /**
      * 得到投保人信息
      */
    val odsHolderDetail =
      sqlContext.sql("select policy_id as policy_id_holder,holder_cert_type,holder_cert_no from odsdb.ods_holder_detail")
      .where("holder_cert_type = 1 and length(holder_cert_no) = 18")
      .cache()

    /**
      * 读取被保人信息
      */
    val odsPolicyInsuredDetail =
      sqlContext.sql("select policy_id as policy_id_insured,insured_id,insured_cert_type,insured_cert_no,start_date,end_date,work_type,company_name from odsdb.ods_policy_insured_detail")
        .where("insured_cert_type = '1' and length(insured_cert_no) = 18")

    /**
      * 读取从属被保人
      */
    val odsPolicyInsuredSlaveDetail =
      sqlContext.sql("select slave_name, master_id,slave_cert_type,slave_cert_no,start_date as start_date_slave,end_date as end_date_slave" +
        " from odsdb.ods_policy_insured_slave_detail")
        .where("slave_cert_type = '1' and length(slave_cert_no) = 18")
        .distinct()

    /**
      * 读取工种信息
      */
    val odsWorktypeDimension = sqlContext.sql("select work_type as work_type_slave,level from odsdb.ods_worktype_dimension")

    /**
      * 新增的保单和投保人关联
      */
    val policyHolderData = dwPolicyDetailIncRes.join(odsHolderDetail,dwPolicyDetailIncRes("policy_id")===odsHolderDetail("policy_id_holder"))
      .selectExpr("policy_id","first_premium","sum_premium","product_code","product_name","policy_start_date","policy_end_date","holder_cert_no",
      "channel_name","insure_company_name","sku_coverage","pay_way","city","province","city_type")
      .cache()

    /**
      * 全量的保单和投保人关联数据
      */
    val policyHolderDataAll = odsPolicyDetail.join(odsHolderDetail,odsPolicyDetail("policy_id")===odsHolderDetail("policy_id_holder"))
      .selectExpr("policy_id","first_premium","sum_premium","product_code","product_name","policy_start_date","policy_end_date","holder_cert_no",
        "channel_name","insure_company_name","sku_coverage","pay_way")

    /**
      * 新客阶段和老客阶段的部分数据
      * 证件号，当前客户类型的时间，首次投保时间，首次投保产品，首次投保产品code，首次投保保额，首次投保保费，首次投保来源，首次投保支付方式
      * 首次投保保险公司 首次投保省份  首次投保城市  首次投保城市级别  首次投保区间
      * 老客阶段--成为老客的时间
      */
    val newInfoDateTemp = getNewInfoDate(sqlContext,policyHolderData,hbaseData)
    val newInfoDateTwo = newInfoDateTemp.drop("become_curr_cus_time")
      .printSchema()

    /**
      * 新客阶段和老了阶段累计参保次数
      */
    getInsuredCount(sqlContext,odsPolicyInsuredDetail,policyHolderData,hbaseData,odsPolicyInsuredSlaveDetail:DataFrame)

    /**
      * 新客阶段和老了阶段累计贡献保费和累计购买次数和件均保费 以及新客阶段购买次数和贡献保费
      */
    getBuysAndPremiums(sqlContext,policyHolderData,hbaseData)

    /**
      * 累计出险次数,累计赔付保费
      */
    getPrePremiumAndRiskCount(sqlContext,odsClaimsDetail)

    /**
      * 最后一次投保产品code  最后一次投保产品名称 当前居住省份 当前居住城市  最后一次投保时间
      */
    getLastStartDateInfo(sqlContext,policyHolderData)

    /**
      * 保险止期
      */
    getPolicyEndDate(sqlContext,policyHolderData,hbaseData)

    /**
      * 拒赔案件数,撤案案件数
      */
    getRrejectClaimCunAndWithdrawClaimCun(sqlContext,odsClaimsDetail)

    /**
      * 当前职业名称（被保人）[工种]  当前所在单位名称当前  职业风险等级
      */
    getInsuredProfessionInfo(sqlContext,dwPolicyDetailInc,odsPolicyInsuredDetail,odsWorktypeDimension)

    /**
      * 近90天购买次数 近90天贡献保费 近90天件均保费
      */
    getNintyDaysInfo(sqlContext,policyHolderDataAll)

    /**
      * 最后一次投保距今天的天数
      */
    getLastPolicyDisTodayDays(sqlContext,policyHolderDataAll)

    /**
      * 第一次投保至今天数
      */
    getFirstPolicyDisTodayDays(sqlContext,policyHolderDataAll)

    /**
      * 当前生效报单数
      */
    getEffectPolicyCunInfo(sqlContext,policyHolderDataAll,odsPolicyDetail,odsPolicyInsuredDetail,odsPolicyInsuredSlaveDetail)

    /**
      * 是否在保
      */
    getIsInsured(sqlContext,odsPolicyDetail,odsPolicyInsuredDetail,odsPolicyInsuredSlaveDetail)

    /**
      * 是否参保 客户来源 参保的所有产品名称和产品code
      */
    getJoinAndSouceAndProductInfo(sqlContext,dwPolicyDetailInc,odsPolicyInsuredDetail,odsPolicyInsuredSlaveDetail,hbaseData)
  }

  /**
    * 是否参保 客户来源 参保的所有产品名称和产品code
    * @param sqlContext 上下文
    * @param dwPolicyDetailInc 增量的保单信息
    * @param odsPolicyInsuredDetail 被保人保单信息
    * @param odsPolicyInsuredSlaveDetail 从属被保人信息
    */
  def getJoinAndSouceAndProductInfo(sqlContext:HiveContext,dwPolicyDetailInc:DataFrame,odsPolicyInsuredDetail:DataFrame,
                                    odsPolicyInsuredSlaveDetail:DataFrame,hbaseData:DataFrame) :DataFrame= {
    import sqlContext.implicits._

    /**
      * 新增的被保人信息
      */
    val insuredIncInfo = dwPolicyDetailInc.join(odsPolicyInsuredDetail,dwPolicyDetailInc("policy_id")===odsPolicyInsuredDetail("policy_id_insured"))
      .selectExpr("insured_id","insured_cert_no","product_code","product_name","channel_name")

    val slaveInfo = insuredIncInfo.join(odsPolicyInsuredSlaveDetail,insuredIncInfo("insured_id")===odsPolicyInsuredSlaveDetail("master_id"))
      .selectExpr("slave_cert_no","product_code","product_name","channel_name")

    /**
      * 被保人和从被保人去重
      */
    val insuredSlave = insuredIncInfo.selectExpr("insured_cert_no as cert_no","product_code","product_name","channel_name")
      .unionAll(slaveInfo.selectExpr("slave_cert_no as cert_no","product_code","product_name","channel_name"))
      .distinct()

    val latentProductInfo = insuredSlave.map(x => {
        val certNo = x.getAs[String]("cert_no")
        val productCode = x.getAs[String]("product_code")
        val productName = x.getAs[String]("product_name")
        (certNo,(productCode,productName))
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
      insuredSlave.selectExpr("cert_no","channel_name")
        .distinct()
        .map(x => {
          val certNo = x.getAs[String]("cert_no")
          val channelName = x.getAs[String]("channel_name")
          (certNo,channelName)
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

    val allCertNo = latentProductInfo.join(threeData,latentProductInfo("cert_no_slave")===threeData("all_cert_no"))
      .selectExpr("all_cert_no","cus_source as cus_source_all","latent_product_info as latent_product_info_all","'是' as is_join_policy")

    val res = allCertNo.join(hbaseData,allCertNo("all_cert_no")===hbaseData("cert_no"),"leftouter")
      .selectExpr("all_cert_no","cus_source_all","latent_product_info_all","is_join_policy","latent_product_info","cus_source")
      .map(x => {
        val allCertNo = x.getAs[String]("all_cert_no")
        val cusSourceAll = x.getAs[String]("cus_source_all")
        val latentProductInfoAll = x.getAs[String]("latent_product_info_all")
        val isJoinPolicy = x.getAs[String]("is_join_policy")
        var latentProductInfo = x.getAs[String]("latent_product_info")
        var cusSource = x.getAs[String]("cus_source")
        /**
          * 潜在客户来源
          */
        if(cusSourceAll != null){
          if(cusSource != null){
            val jsonTemp = JSON.parseArray(cusSourceAll).fluentAddAll(JSON.parseArray(cusSource)).toArray.toSet.toArray
            cusSource = JSON.toJSONString(jsonTemp,SerializerFeature.BeanToArray)
          }else{
            cusSource = cusSourceAll
          }
        }
        /**
          * 潜在客户产品信息
          */
        if(latentProductInfoAll != null){
          if(latentProductInfo != null){
            val jsonTemp = JSON.parseArray(latentProductInfoAll).fluentAddAll(JSON.parseArray(latentProductInfo)).toArray().toSet.toArray
            latentProductInfo = JSON.toJSONString(jsonTemp,SerializerFeature.BeanToArray)
          }else{
            latentProductInfo = latentProductInfoAll
          }
        }
        (allCertNo,cusSource,latentProductInfo,isJoinPolicy)
      })
      .toDF("cert_no","cus_source","latent_product_info","is_join_policy")
    res.printSchema()
    res
  }

  /**
    * 是否在保
    * @param sqlContext 上下文
    * @param odsPolicyDetail 保单信息
    * @param odsPolicyInsuredDetail 被保人保单信息
    * @param odsPolicyInsuredSlaveDetail 从属被保人信息
    */
  def getIsInsured(sqlContext:HiveContext,odsPolicyDetail:DataFrame,odsPolicyInsuredDetail:DataFrame,odsPolicyInsuredSlaveDetail:DataFrame) :DataFrame= {
    import sqlContext.implicits._

    //当前时间
    val currDate = get_current_date(System.currentTimeMillis()).toString.substring(0, 19)

    //被保人详细信息
    val odsPolicyInsuredDetailOne =
      odsPolicyInsuredDetail.selectExpr("insured_id", "insured_cert_no", "start_date", "end_date")

    //从属被保人信息 没有在被保人信息表中出现的数据
    val odsPolicyInsuredSlaveDetailOne =
      odsPolicyInsuredSlaveDetail.join(odsPolicyInsuredDetailOne, odsPolicyInsuredSlaveDetail("slave_cert_no") === odsPolicyInsuredDetailOne("insured_cert_no"), "leftouter")
        .where("insured_cert_no is null")
        .selectExpr("master_id", "slave_cert_no", "start_date_slave", "end_date_slave")

    val res = odsPolicyInsuredDetailOne.selectExpr("insured_cert_no as cert_no", "start_date", "end_date")
      .unionAll(odsPolicyInsuredSlaveDetailOne.selectExpr("slave_cert_no as cert_no", "start_date_slave as start_date", "end_date_slave as end_date"))
      .map(x => {
        val certNo = x.getAs[String]("cert_no")
        var startDate = x.getAs[String]("start_date")
        var endDate = x.getAs[String]("end_date")
        if(startDate == null){
          startDate = endDate
        }
        if(endDate == null){
          endDate = startDate
        }
        (certNo,(startDate,endDate))
      })
      .filter(x=> x._2._1 != null && x._2._2 != null)
      .map(x => {
        if ((x._2._1.compareTo(currDate) <= 0) && (x._2._2.compareTo(currDate) >= 0)) {
          (x._1, 1)
        } else {
          (x._1, 0)
        }
      })
      .reduceByKey((x1, x2) => {
        val isInsuredCount = x1 + x2 //在保数 如果在保数》0 说明又在保的保单
        (isInsuredCount)
      })
      .map(x => {
        if (x._2 > 0) {
          (x._1, "在保")
        } else {
          (x._1, "不在保")
        }
      })
      .toDF("cert_no","is_insured")
    res.printSchema()
    res
  }

  /**
    * 当前生效报单数
    * @param sqlContext 上下文
    * @param policyHolderDataAll 投保人保单信息
    * @param odsPolicyDetail  保单信息
    * @param odsPolicyInsuredDetail  被保人保单信息
    * @param odsPolicyInsuredSlaveDetail 从属被保人信息
    * @return
    */
  def getEffectPolicyCunInfo(sqlContext: HiveContext, policyHolderDataAll: DataFrame, odsPolicyDetail: DataFrame, odsPolicyInsuredDetail: DataFrame,
                              odsPolicyInsuredSlaveDetail: DataFrame): DataFrame = {
    import sqlContext.implicits._
    /**
      * 被保人
      */
    val policyInsured = odsPolicyDetail.join(odsPolicyInsuredDetail, odsPolicyDetail("policy_id") === odsPolicyInsuredDetail("policy_id_insured"))
      .selectExpr("policy_id","product_code", "product_name", "insured_id", "insured_cert_no","channel_name")

    /**
      * 从属被保人
      */
    val policySlave = policyInsured.join(odsPolicyInsuredSlaveDetail, policyInsured("insured_id") === odsPolicyInsuredSlaveDetail("master_id"))
      .selectExpr("policy_id","product_code", "product_name", "insured_id", "slave_cert_no","channel_name")

    /**
      * 投保人
      */
    val policyHolder = policyHolderDataAll.selectExpr("policy_id", "holder_cert_no")

    /**
      * 投被保人生效保单数
      */
    val effectPolicyCun = policyInsured.selectExpr("policy_id", "insured_cert_no as cert_no")
      .unionAll(policySlave.selectExpr("policy_id", "slave_cert_no as cert_no"))
      .unionAll(policyHolder.selectExpr("policy_id", "holder_cert_no as cert_no"))
      .distinct()
      .map(x => {
        val certNo = x.getAs[String]("cert_no")
        (certNo, 1)
      })
      .reduceByKey(_ + _)
      .map(x => (x._1, x._2))
      .toDF("cert_no", "effect_policy_cun")
    effectPolicyCun.printSchema()
    effectPolicyCun
  }

  /**
    * 第一次投保至今天数
    * @param sqlContext sql上下文
    * @param policyHolderDataAll 全量的保单和投保人信息
    * @return 结果
    */
  def getFirstPolicyDisTodayDays(sqlContext:HiveContext,policyHolderDataAll:DataFrame) :DataFrame = {
    import sqlContext.implicits._
    //当前时间
    val currDate = get_current_date(System.currentTimeMillis()).toString.substring(0, 19)
    val newResOne = policyHolderDataAll.map(x => {
      val holderCert_no = x.getAs[String]("holder_cert_no")
      var policyStartDate = x.getAs[java.sql.Timestamp]("policy_start_date")
      val policyEndDate = x.getAs[java.sql.Timestamp]("policy_end_date")
      if (policyStartDate == null) {
        policyStartDate = policyEndDate
      }
      (holderCert_no, policyStartDate)
    })
      .reduceByKey((x1, x2) => {
        val res = if(x1 == null) {
          x2
        }else if(x2 == null){
          x1
        }else{
          if (x1.compareTo(x2) < 0) x1 else x2
        }
        res
      })
      .map(x => {
        //证件号 开始时间 结束时间 产品code，产品名称  省份 城市  最新保单距今天数
        if (x._2 != null) {
          (x._1,getBeg_End_one_two_new(x._2.toString.substring(0, 19), currDate))
        } else {
          (x._1,0L)
        }
      })
      .toDF("cert_no","last_policy_days")
    newResOne.printSchema()
    newResOne
  }

  /**
    * 最后一次投保距今天的天数
    * @param sqlContext sql上下文
    * @param policyHolderDataAll 全量的保单和投保人信息
    */
  def getLastPolicyDisTodayDays(sqlContext:HiveContext,policyHolderDataAll:DataFrame) :DataFrame = {
    import sqlContext.implicits._
    //当前时间
    val currDate = get_current_date(System.currentTimeMillis()).toString.substring(0, 19)
    val newResOne = policyHolderDataAll.map(x => {
    val holderCert_no = x.getAs[String]("holder_cert_no")
    var policyStartDate = x.getAs[java.sql.Timestamp]("policy_start_date")
    val policyEndDate = x.getAs[java.sql.Timestamp]("policy_end_date")
    if (policyStartDate == null) {
      policyStartDate = policyEndDate
    }
    (holderCert_no, policyStartDate)
    })
    .reduceByKey((x1, x2) => {
      val res = if(x1 == null) {
        x2
      }else if(x2 == null){
        x1
      }else{
        if (x1.compareTo(x2) >= 0) x1 else x2
      }
      res
    })
      .map(x => {
        //证件号 开始时间 结束时间 产品code，产品名称  省份 城市  最新保单距今天数
        if (x._2 != null) {
          (x._1,getBeg_End_one_two_new(x._2.toString.substring(0, 19), currDate))
        } else {
          (x._1,0L)
        }
      })
      .toDF("cert_no","last_policy_days")
    newResOne.printSchema()
    newResOne
  }

  /**
    * 近90天购买次数 近90天贡献保费 近90天件均保费
    * @param sqlContext sql上下文
    * @param policyHolderDataAll 全量的保单和投保人信息
    */
  def getNintyDaysInfo(sqlContext:HiveContext,policyHolderDataAll:DataFrame) :DataFrame = {
    import sqlContext.implicits._

    //近90天时间
    val currDateDelNintyDay = Timestamp.valueOf(dateDelNintyDay(get_current_date(System.currentTimeMillis()).toString.substring(0, 19)))
    //近90天的数据
    val newResTwo = policyHolderDataAll.map(x => {
      val holderCert_no = x.getAs[String]("holder_cert_no")
      val firstPremium = x.getAs[Double]("first_premium")
      val sumPremium = x.getAs[Double]("sum_premium")
      var policyStartDate = x.getAs[java.sql.Timestamp]("policy_start_date")
      val policyEndDate = x.getAs[java.sql.Timestamp]("policy_end_date")
      if (policyStartDate == null) {
        policyStartDate = policyEndDate
      }
      (holderCert_no, (policyStartDate, policyEndDate, firstPremium, sumPremium, currDateDelNintyDay, 1))
    })
      .filter(x => {
        if(x._2._1 == null){
          false
        }else if (x._2._5.compareTo(x._2._1) <= 0) true else false
      })
      .map(x => {
        (x._1, (x._2._4, x._2._6))
      })
      .reduceByKey((x1, x2) => {
        val count: Int = x1._2 + x2._2
        val sumPremium = BigDecimal.valueOf(x1._1).setScale(4, RoundingMode.HALF_UP).+(BigDecimal.valueOf(x2._1).setScale(4, RoundingMode.HALF_UP))
        (sumPremium.doubleValue(), count)
      })
      .map(x => (x._1, x._2._1, x._2._2, BigDecimal.valueOf(x._2._1 / x._2._2).setScale(4, RoundingMode.HALF_UP).doubleValue()))
      .toDF("cert_no", "ninety_policy_premium", "ninety_policy_cun", "ninety_premium_avg")
    newResTwo.printSchema()
    newResTwo
  }


  /**
    * 当前职业名称（被保人）[工种]  当前所在单位名称当前  职业风险等级
    * @param sqlContext sql上下文
    * @param dwPolicyDetailInc //新增保单信息
    * @param odsPolicyInsuredDetail 被保人明细表
    */
  def getInsuredProfessionInfo(sqlContext:HiveContext,dwPolicyDetailInc:DataFrame,odsPolicyInsuredDetail:DataFrame,odsWorktypeDimension:DataFrame) :DataFrame= {
    import sqlContext.implicits._

    val insuredDetail = dwPolicyDetailInc.join(odsPolicyInsuredDetail,dwPolicyDetailInc("policy_id")===odsPolicyInsuredDetail("policy_id_insured"))
      .selectExpr("insured_cert_no","start_date","end_date","work_type","company_name")

    /**
      * 保单止期最大的保单信息
      */
    val res = insuredDetail.join(odsWorktypeDimension,insuredDetail("work_type")===odsWorktypeDimension("work_type_slave"),"leftouter")
      .selectExpr("insured_cert_no","start_date","end_date","work_type as now_profession_name","company_name as now_company_name","level as now_profession_risk_level")
      .map(x => {
        val insuredCertNo = x.getAs[String]("insured_cert_no")
        val startDate = x.getAs[String]("start_date")
        var endDate = x.getAs[String]("end_date")
        val nowProfessionName = x.getAs[String]("now_profession_name")
        val nowCompanyName = x.getAs[String]("now_company_name")
        val nowProfessionRiskLevel = x.getAs[Int]("now_profession_risk_level")
        if(endDate == null){
          endDate = startDate
        }
        (insuredCertNo,(endDate,nowProfessionName,nowCompanyName,nowProfessionRiskLevel))
      })
      .reduceByKey((x1,x2) => {
        val res = if(x1._1 == null){
          x2
        }else if(x2._1 == null){
          x1
        }else{
          if(x1._1.compareTo(x2._1) > 0) x1 else x2
        }
        res
      })
      .map(x => {
        (x._1,x._2._2,x._2._3,x._2._4)
      })
      .toDF("cert_no","now_profession_name","now_company_name","now_profession_risk_level")
    res.printSchema()
    res
  }

  /**
    * 新客阶段和老了阶段累计贡献保费和累计购买次数和件均保费 以及新客阶段购买次数和贡献保费
    * @param sqlContet //sql上下文
    * @param policyHolderData //新增保单和投保人信息
    * @param hbaseData hbase数据
    */
  def getBuysAndPremiums(sqlContet:HiveContext,policyHolderData:DataFrame,hbaseData:DataFrame) :DataFrame={
    import sqlContet.implicits._
    /**
      * 购买次数和贡献保费
      */
    val buysAndPremoums = policyHolderData.map(x => {
      var firstPremium:Double = x.getAs [Double]("first_premium")
      firstPremium = if(firstPremium == null) 0.0 else firstPremium
      val holderCertNo = x.getAs [String]("holder_cert_no")
      (holderCertNo,(firstPremium,1))
    })
      .reduceByKey((x1,x2) => {
        val firstPremiums = (BigDecimal.valueOf(x1._1).setScale(4,RoundingMode.HALF_UP).+(BigDecimal.valueOf(x1._1).setScale(4,RoundingMode.HALF_UP))).toDouble
        val count = x1._2+x2._2
        (firstPremiums,count)
      })
      .map(x => {
        (x._1,x._2._1,x._2._2)
      })
      .toDF("holder_cert_no","premiums","buy_counts")

    val hbaseBuysAndPremoums = buysAndPremoums.join(hbaseData,buysAndPremoums("holder_cert_no")===hbaseData("cert_no"),"leftouter")
      .map(x => {
        val holderCertNo = x.getAs[String]("holder_cert_no")
        val certNo = x.getAs[String]("cert_no")
        val premiums = x.getAs[Double]("premiums")
        val buyCounts = x.getAs[Int]("buy_counts")
        val cusType = x.getAs[String]("cus_type")
        var policyPremium = x.getAs[String]("policy_premium")
        var policyCun = x.getAs[String]("policy_cun")
        var newCusBuyCun = x.getAs[String]("new_cus_buy_cun")
        var newCusSumPremium = x.getAs[String]("new_cus_sum_premium")
        var premiumAvg = ""
        if(certNo!= null){
          if(policyPremium!= null){
            policyPremium = BigDecimal.valueOf(premiums+policyPremium.toDouble).setScale(4,RoundingMode.HALF_UP).toString
          }else{
            policyPremium = BigDecimal.valueOf(premiums).setScale(4,RoundingMode.HALF_UP).toString
          }
          if(policyCun != null){
            policyCun = (buyCounts+policyCun.toInt).toString
          }else{
            policyCun =buyCounts.toString
          }
          premiumAvg = if(policyPremium != null && policyCun!= null){
            (policyPremium.toDouble / policyCun.toInt).toString
          }else{
            null
          }
          //已经存在新客阶段的数据--新客阶段参保次数
          if(cusType == "1"){
            if(newCusSumPremium!= null){
              newCusSumPremium = BigDecimal.valueOf(premiums+newCusSumPremium.toDouble).setScale(4,RoundingMode.HALF_UP).toString
            }else{
              newCusSumPremium = BigDecimal.valueOf(premiums).setScale(4,RoundingMode.HALF_UP).toString
            }
            if(policyCun != null){
              newCusBuyCun = (buyCounts+policyCun.toInt).toString
            }else{
              newCusBuyCun = buyCounts.toString
            }
          }
        }else{
          if(newCusSumPremium!= null){
            newCusSumPremium = BigDecimal.valueOf(premiums+newCusSumPremium.toDouble).setScale(4,RoundingMode.HALF_UP).toString
          }else{
            newCusSumPremium = BigDecimal.valueOf(premiums).setScale(4,RoundingMode.HALF_UP).toString
          }
          if(policyCun != null){
            newCusBuyCun = (buyCounts+policyCun.toInt).toString
          }else{
            newCusBuyCun = buyCounts.toString
          }
          policyPremium = if(premiums != null) premiums.toString else null
          policyCun = if(buyCounts != null) buyCounts.toString else null
          premiumAvg = if(policyPremium != null && policyCun!= null){
            (BigDecimal.valueOf(policyPremium.toDouble / policyCun.toInt).setScale(4,RoundingMode.HALF_UP).doubleValue()).toString
          }else null
        }
        (holderCertNo,newCusSumPremium,newCusBuyCun,policyPremium,policyCun,premiumAvg)
      })
      .toDF("cert_no","new_cus_sum_premium","new_cus_buy_cun","policy_premium","policy_cun","premium_avg")

    hbaseBuysAndPremoums.printSchema()
    hbaseBuysAndPremoums
  }

  /**
    * 新客阶段和老了阶段累计参保次数
    * @param odsPolicyInsuredDetail 在保人明细表
    * @param policyHolderData //新增的保单和投保人数据
    * @param odsPolicyInsuredSlaveDetail //从属被保人
    * @param hbaseData hbase数据
    */
  def getInsuredCount(sqlContext:HiveContext,odsPolicyInsuredDetail:DataFrame,policyHolderData:DataFrame,hbaseData:DataFrame,odsPolicyInsuredSlaveDetail:DataFrame) :DataFrame= {
    import sqlContext.implicits._
    /**
      * 被保人参保次数
      */
    val insuredInfo = policyHolderData.join(odsPolicyInsuredDetail,policyHolderData("policy_id")===odsPolicyInsuredDetail("policy_id_insured"))
      .selectExpr("insured_cert_no","insured_id","policy_id_insured")
      .distinct()

    /**
      * 被保人和从属被保人的参保次数
      */
    val slaveInfo = insuredInfo.join(odsPolicyInsuredSlaveDetail,insuredInfo("insured_id")===odsPolicyInsuredSlaveDetail("master_id"))
    val insuredSlaveCount = insuredInfo.selectExpr("insured_cert_no as cert_no","policy_id_insured")
      .unionAll(slaveInfo.selectExpr("slave_cert_no as cert_no","policy_id_insured"))
      .distinct()
      .map(x => {
        val certNo = x.getAs[String]("cert_no")
        (certNo,1)
      })
      .reduceByKey(_+_)
      .map(x => (x._1,x._2))
      .toDF("cert_no_inc","insured_count")

    val hbaseInsuredCount = insuredSlaveCount.join(hbaseData,insuredSlaveCount("cert_no_inc")===hbaseData("cert_no"),"leftouter")
      .map(x => {
        val certNoInc = x.getAs[String]("cert_no_inc")
        val certNo = x.getAs[String]("cert_no")
        val insureCount = x.getAs[Int]("insured_count")
        val cusType = x.getAs[String]("cus_type")
        var policyInsuredCun = x.getAs[String]("policy_insured_cun")
        var newCusInsuredCun = x.getAs[String]("new_cus_insured_cun")
        if(certNo != null){
          //累计参保次数
          if(policyInsuredCun != null){
            policyInsuredCun = (insureCount + policyInsuredCun.toInt).toString
          }else{
            policyInsuredCun = insureCount.toString
          }
          //已经存在新客阶段的数据--新客阶段参保次数
          if(cusType == "1"){
            if(policyInsuredCun != null){
              newCusInsuredCun = (insureCount + policyInsuredCun.toInt).toString
            }else{
              newCusInsuredCun = insureCount.toString
            }
          }
        }else{//新客或者潜在客户
          policyInsuredCun = insureCount.toString
          newCusInsuredCun = insureCount.toString
        }
        (certNoInc,policyInsuredCun,newCusInsuredCun)
      })
      .toDF("cert_no","policy_insured_cun","new_cus_insured_cun")

    hbaseInsuredCount.printSchema()
    hbaseInsuredCount
  }

  /**
    * 新客阶段和老客阶段的部分数据
    * 证件号，当前客户类型的时间，首次投保时间，首次投保产品，首次投保产品code，首次投保保额，首次投保保费，首次投保来源，首次投保支付方式
    * 首次投保保险公司 首次投保省份  首次投保城市  首次投保城市级别  首次投保区间
    * 老客阶段--成为老客的时间
    * @param sqlContext sql上下文
    * @param policyHolderData 新增的保单和投保人的数据集
    * @param hbaseData hbase 数据
    */
  def getNewInfoDate(sqlContext: HiveContext,policyHolderData:DataFrame,hbaseData:DataFrame) :DataFrame={

    import sqlContext.implicits._
    /**
      * 所有新投保单中找到最新的保单信息
      */
    val holderNewRes = policyHolderData.map(x => {
      val policyId = x.getAs [String]("policy_id")
      val firstPremium = x.getAs [Double]("first_premium")
      val sumPremium = x.getAs [Double]("sum_premium")
      val productCode = x.getAs [String]("product_code")
      val productName = x.getAs [String]("product_name")
      val policyStartDate = x.getAs [java.sql.Timestamp]("policy_start_date")
      val policyEndDate = x.getAs [java.sql.Timestamp]("policy_end_date")
      val holderCertNo = x.getAs [String]("holder_cert_no")
      val channelName = x.getAs [String]("channel_name")
      val insureCompanyName = x.getAs [String]("insure_company_name")
      val skuCoverage = x.getAs [String]("sku_coverage")
      val payWay = x.getAs [Int]("pay_way")
      val city = x.getAs [String]("city")
      val province = x.getAs [String]("province")
      val cityType = x.getAs [String]("city_type")
      (holderCertNo,(policyId,firstPremium,sumPremium,productCode,productName,policyStartDate,policyEndDate,channelName,
        insureCompanyName,skuCoverage,payWay,city,province,cityType))
    })
      .reduceByKey((x1,x2) => {
        val res = if(x1._6 == null){
          x2
        }else if(x2._6 == null){
          x1
        }else{
          if(x1._6.compareTo(x2._6) < 0) x1 else x2
        }
        res
      })
      .map(x => {
        (x._2._1,x._2._2,x._2._3,x._2._4,x._2._5,x._2._6,x._2._7,x._1,x._2._8,x._2._9,x._2._10,x._2._11,x._2._12,x._2._13,x._2._14)
      })
      .toDF("policy_id","first_premium","sum_premium","product_code","product_name","policy_start_date","policy_end_date","holder_cert_no",
        "channel_name","insure_company_name","sku_coverage","pay_way","city","province","city_type")
      .cache()

    /**
      * 新增的保单和hbase中已存在的用户进行关联 如果不存在就 判断是新客，
      * 如果已经存在 判断是否是新客阶段  如果是不做任何处理  如果不是处理新客阶段的数据  如果是其他阶段的数据
      * 第一个结果是没有出现过的人或者从潜在客户变成投保人的人
      */
    val incHolderNewInfoTemp = holderNewRes.join(hbaseData,holderNewRes("holder_cert_no")===hbaseData("cert_no"),"leftouter")
      .selectExpr("holder_cert_no as cert_no",
        "case when cert_no is null or cus_type = '0' then policy_start_date end as become_curr_cus_time",
        "case when cert_no is null or cus_type = '0' then policy_start_date end as first_policy_time",
        "case when cert_no is null or cus_type = '0' then product_name end as first_policy_pdt_name",
        "case when cert_no is null or cus_type = '0' then product_code end as first_policy_pdt_code",
        "case when cert_no is null or cus_type = '0' then sku_coverage end as first_policy_plan",
        "case when cert_no is null or cus_type = '0' then first_premium end as first_policy_premium",
        "case when cert_no is null or cus_type = '0' then channel_name end as first_policy_source",
        "case when cert_no is null or cus_type = '0' then pay_way end as first_policy_pay_channel",
        "case when cert_no is null or cus_type = '0' then getAge(holder_cert_no,policy_start_date) end as first_policy_age",
        "case when cert_no is null or cus_type = '0' then insure_company_name end as first_policy_insurant_name",
        "case when cert_no is null or cus_type = '0' then getBecomeOldTime(policy_start_date) end as become_old_time",
        "case when cert_no is null or cus_type = '0' then city else null end as first_policy_city",
        "case when cert_no is null or cus_type = '0' then province else null end as first_policy_province",
        "case when cert_no is null or cus_type = '0' then city_type else null end as first_policy_city_level",
        "case when cert_no is null or cus_type = '0' then concat(cast(policy_start_date as string),'-',cast(policy_end_date as string)) end as first_policy_section"
      )

    incHolderNewInfoTemp.printSchema()
    incHolderNewInfoTemp
  }

  /**
    * 累计出险次数,累计赔付保费
    * @param sqlContext  sql上下文
    * @param odsClaimsDetail 理赔数据集
    */
  def getPrePremiumAndRiskCount(sqlContext:HiveContext,odsClaimsDetail:DataFrame) :DataFrame= {
   import sqlContext.implicits._
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
        .toDF("cert_no", "pre_premium_sum", "risk_cun")
    claimOne.printSchema()
    claimOne
  }

  /**
    * 最后一次投保产品code  最后一次投保产品名称 当前居住省份 当前居住城市  最后一次投保时间
    * @param sqlContext sql上下文
    * @param policyHolderData //新增保单和投保人信息信息
    */
  def getLastStartDateInfo(sqlContext:HiveContext,policyHolderData:DataFrame) :DataFrame= {
    import sqlContext.implicits._
    /**
      * 起期最大的保单信息
      */
    val res = policyHolderData
      .map(x => {
        val holderCertNo = x.getAs[String]("holder_cert_no")
        val productCode = x.getAs[String]("product_code")
        val productName = x.getAs[String]("product_name")
        val province = x.getAs[String]("province")
        val city = x.getAs[String]("city")
        var policyStartDate = x.getAs[java.sql.Timestamp]("policy_start_date")
        val policyEndDate = x.getAs[java.sql.Timestamp]("policy_end_date")
        if(policyStartDate == null){
          policyStartDate = policyEndDate
        }
        (holderCertNo,(productCode,productName,province,city,policyStartDate))
      })
      .reduceByKey((x1,x2) => {
        val res = if(x1._5 == null){
          x2
        }else if(x2._5 == null){
          x1
        }else{
          if(x1._5.compareTo(x2._5) > 0) x1 else x2
        }
        res
      })
      .map(x => {
        (x._1,x._2._1,x._2._2,x._2._3,x._2._4,x._2._5)
      })
      .toDF("cert_no","last_policy_product_code","last_policy_product_name","now_province","now_city","last_policy_date")
    res.printSchema()
    res
  }

  /**
    * 保险止期
    * @param sqlContext sql上下文
    * @param policyHolderData 增量的投保人和
    */
  def getPolicyEndDate(sqlContext:HiveContext,policyHolderData:DataFrame,hbaseData:DataFrame) :DataFrame= {
    import sqlContext.implicits._
    //保险止期
    val newResOne = policyHolderData.map(x => {
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
      .toDF("holder_cert_no", "last_policy_end_date_temp")
    val res = newResOne.join(hbaseData,newResOne("holder_cert_no")===hbaseData("cert_no"),"leftouter")
      .selectExpr("holder_cert_no","last_policy_end_date_temp","last_policy_end_date")
      .map(x => {
        val holderCertNo = x.getAs[String]("holder_cert_no")
        val lastPlicyEndDateTemp = x.getAs[java.sql.Timestamp]("last_policy_end_date_temp")
        var lastPlicyEndDate = x.getAs[String]("last_policy_end_date")
        if(lastPlicyEndDateTemp != null){
          if(lastPlicyEndDate != null && lastPlicyEndDate.compareTo(lastPlicyEndDateTemp.toString) < 0){
            lastPlicyEndDate = lastPlicyEndDateTemp.toString
          }
          if(lastPlicyEndDate == null){
            lastPlicyEndDate = lastPlicyEndDateTemp.toString
          }
        }
        (holderCertNo,lastPlicyEndDate)
      })
      .toDF("cert_no","last_policy_end_date")
    res.printSchema()
    res
  }
  /**
    * 拒赔案件数,撤案案件数
    * @param sqlContext  sql上下文
    * @param odsClaimsDetail 理赔数据集
    */
  def getRrejectClaimCunAndWithdrawClaimCun(sqlContext:HiveContext,odsClaimsDetail:DataFrame) :DataFrame={
    import sqlContext.implicits._
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
      .toDF("cert_no", "reject_claim_cun", "withdraw_claim_cun")
      .selectExpr("cert_no", "case when reject_claim_cun = -1 then null else reject_claim_cun end as reject_claim_cun",
        "case when withdraw_claim_cun = -1 then null else withdraw_claim_cun end as withdraw_claim_cun")
    claimTwo.printSchema()
    claimTwo
  }
}