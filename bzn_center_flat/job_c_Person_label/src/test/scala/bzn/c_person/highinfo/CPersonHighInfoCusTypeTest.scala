package bzn.c_person.highinfo

import java.sql.Timestamp
import java.util

import bzn.job.common.{HbaseUtil, Until}
import c_person.util.SparkUtil
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext


/**
  * author:xiaoYuanRen
  * Date:2019/8/1
  * Time:11:33
  * describe: 高级标签的客户类型和变成当前客户的时间和上一个客户类型的状态
  * 说明：
  * 1.纯被保人情况
  *   1）新增客户 添加新增客户的客户类型 变成当前客户的时间
  *   2）如果不是新增客户 不变化
*   2.投保人情况
  *   1）如果是新的客户 添加新客的客户类型
  *   2）如果是已经存在的客户，如果是新客不变化，如果是老客不变化，如果是易流失或者流失，为变成老客并且是挽回客户，如果是沉睡用户，变成老客并且是挽回客户。
  *   3）之前的客户类型：（不存在新客户的那部分数据）
  *      a）如果当前是新客，变成老客时间小于当前时间 变成老客
  *      b）易流失
  * 如果不是在保保单 30天内无复购 - 易流失
  * 如果在保保单保障期间大于30 天  而且还与30天到期的 易流失 如果保障期间小30 30天内无复购 易流失
  * 如果在保情况 使用最大的开始时间作为复购条件
  * c)
  **/
object CPersonHighInfoCusTypeTest extends SparkUtil with Until with HbaseUtil{
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "local[4]")

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
        val becomeOldTime = Bytes.toString(x._2.getValue("cent_info".getBytes, "become_old_time".getBytes))
        val lastCusType = Bytes.toString(x._2.getValue("high_info".getBytes, "last_cus_type".getBytes)) //前一次投保类型
        val becomeCurrCusTime = Bytes.toString(x._2.getValue("high_info".getBytes, "become_curr_cus_time".getBytes))
        (key,cusType,lastCusType,becomeOldTime,becomeCurrCusTime)
      })
      .toDF("cert_no","cus_type","last_cus_type","become_old_time","become_curr_cus_time")
      .where("cus_type in ('0','1','2','3','4','5')")
    getAllInfo(hiveContext,hbaseData)
    sc.stop()
  }

  def getAllInfo(sqlContext:HiveContext,hbaseData: DataFrame) ={
    /**、
      *
      * 止期最大   最近投保
      */
    /**
      * 获取增量信息
      */
    val  dwPolicyDetailInc =
      sqlContext.sql("select policy_id,policy_status,policy_start_date,policy_end_date,inc_type from dwdb.dw_policy_detail_inc")
        .where("policy_status in (0,1,-1) and inc_type = 0")
        .cache()

    /**
      * 读取所有保单信息
      */
    val  odsPolicyDetail =
      sqlContext.sql("select policy_id as policy_id_all,policy_status,policy_start_date,policy_end_date,policy_create_time from odsdb.ods_policy_detail")
        .where("policy_status in (0,1,-1)")
        .cache()

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
      sqlContext.sql("select policy_id as policy_id_insured,insured_id,insured_cert_type,insured_cert_no,start_date,end_date from odsdb.ods_policy_insured_detail")
        .where("insured_cert_type = '1' and length(insured_cert_no) = 18")

    /**
      * 读取从属被保人
      */
    val odsPolicyInsuredSlaveDetail =
      sqlContext.sql("select master_id,slave_cert_type,slave_cert_no,start_date as start_date_slave,end_date as end_date_slave" +
        " from odsdb.ods_policy_insured_slave_detail")
        .where("slave_cert_type = '1' and length(slave_cert_no) = 18")
        .distinct()

    /**
      * 新增的保单和投保人关联
      */
    val policyHolderData = dwPolicyDetailInc.join(odsHolderDetail,dwPolicyDetailInc("policy_id")===odsHolderDetail("policy_id_holder"))
      .selectExpr("policy_id","policy_start_date","policy_end_date","holder_cert_no")
      .cache()

    /**
      * 全量的保单和投保人关联数据
      */
    val policyHolderDataAll = odsPolicyDetail.join(odsHolderDetail,odsPolicyDetail("policy_id_all")===odsHolderDetail("policy_id_holder"))
      .selectExpr("policy_id_all","policy_start_date","policy_end_date","holder_cert_no")
      .cache()

    /**
      *  客户类型 是否是挽回客户 是否是被唤醒客户 挽回时长  唤醒时长 原客户类型
      */
    getNewInfo(sqlContext,dwPolicyDetailInc,policyHolderData,odsPolicyInsuredDetail,odsPolicyInsuredSlaveDetail,hbaseData)
    /**
      * 客户类型 变成当前客户类型的时间  原客户类型
      */
    getOldInfo(sqlContext,dwPolicyDetailInc,odsPolicyDetail,policyHolderData,odsPolicyInsuredDetail,odsPolicyInsuredSlaveDetail,odsHolderDetail,hbaseData)
  }

  /**
    * 客户类型 变成当前客户类型的时间  原客户类型
    * @param sqlContext sql上下文
    * @param dwPolicyDetailInc 增量的保单信息
    * @param odsPolicyDetail 总保单信息
    * @param policyHolderData 投保保单信息和投保人信息
    * @param odsPolicyInsuredDetail 被保人信息
    * @param odsPolicyInsuredSlaveDetail 从被保人信息
    * @param odsHolderDetail 投保人信息
    * @param hbaseData hbase data
    * @return 返回值
    */
  def getOldInfo(sqlContext:HiveContext,dwPolicyDetailInc:DataFrame,odsPolicyDetail:DataFrame,policyHolderData:DataFrame,odsPolicyInsuredDetail:DataFrame,
    odsPolicyInsuredSlaveDetail:DataFrame,odsHolderDetail:DataFrame,hbaseData:DataFrame) ={

    import sqlContext.implicits._

    //######## 增量
    /**
      * 新增投保人
      */
    val incHolderRes = policyHolderData.where("holder_cert_no is not null")
      .selectExpr("holder_cert_no")
      .distinct()

    //#######全量数据

    /**
      * 得到止期最大的保单
      */
    val holderMaxEnd = odsHolderDetail.join(odsPolicyDetail,odsHolderDetail("policy_id_holder")===odsPolicyDetail("policy_id_all"))
      .selectExpr("holder_cert_no","case when policy_start_date is null then policy_create_time else policy_start_date end as policy_start_date","policy_end_date")
      .where("policy_end_date is not null")
      .map(x => {
        val holderCert_no = x.getAs[String]("holder_cert_no")
        val policyStartDate = x.getAs[java.sql.Timestamp]("policy_start_date")
        val policyEndDate = x.getAs[java.sql.Timestamp]("policy_end_date")
        (holderCert_no,(policyStartDate,policyEndDate))
      })
      .reduceByKey((x1, x2) => {
        val policyEndDate = if (x1._2.compareTo(x2._2) >= 0) x1 else x2
        policyEndDate
      })
      .map(x => (x._1, x._2._1,x._2._2))
      .toDF("cert_no", "policy_start_date","policy_end_date")

    /**
      * 最近的保单
      */
    val holderNewPolicy = odsHolderDetail.join(odsPolicyDetail,odsHolderDetail("policy_id_holder")===odsPolicyDetail("policy_id_all"))
      .selectExpr("holder_cert_no","case when policy_start_date is null then policy_create_time else policy_start_date end as policy_start_date")
      .where("policy_start_date is not null")
      .map(x => {
        val holderCert_no = x.getAs[String]("holder_cert_no")
        val policyStartDate = x.getAs[java.sql.Timestamp]("policy_start_date")
        (holderCert_no,policyStartDate)
      })
      .reduceByKey((x1, x2) => {
        val policyEndDate = if (x1.compareTo(x2) >= 0) x1 else x2
        policyEndDate
      })
      .map(x => (x._1, x._2))
      .toDF("cert_no_slave", "policy_new_start_date")

    /**
      * 保单止期最大保单和最近保单信息关联
      * 如果保单止期最大的保单的开始时间小于最近保单的开始时间用最近保单的开始时间
      */
    val holdInfo = holderMaxEnd.join(holderNewPolicy,holderMaxEnd("cert_no")===holderNewPolicy("cert_no_slave"))
      .selectExpr("cert_no",
        "case when policy_new_start_date > policy_start_date then policy_new_start_date else policy_start_date end policy_new_start_date",
        "policy_start_date","policy_end_date")

    /***
      * 全量的投保人和新增的投保人关联  去掉新增的投保人
      */
    val incHolderResult = holdInfo.join(incHolderRes,holdInfo("cert_no")===incHolderRes("holder_cert_no"),"leftouter")
      .where("holder_cert_no is null")
      .selectExpr("cert_no as cert_no_inc","policy_new_start_date","policy_start_date","policy_end_date")

    /**
      * 标签数据和投保人数据进行关联
      */
    val currTime = getNowTime()
    val res = incHolderResult.join(hbaseData,incHolderResult("cert_no_inc")===hbaseData("cert_no"))
      .selectExpr("cert_no","cus_type","become_old_time","become_curr_cus_time","policy_new_start_date","policy_start_date","policy_end_date","last_cus_type")
      .map(x => {
        val certNo = x.getAs[String]("cert_no")
        var cusType = x.getAs[String]("cus_type")
        var lastCusType = x.getAs[String]("last_cus_type")
        val becomeOldTime = x.getAs[String]("become_old_time")
        var becomeCurrCusTime = x.getAs[String]("become_curr_cus_time")
        val policyNewStartDate = x.getAs[java.sql.Timestamp]("policy_new_start_date")
        val policyStartDate = x.getAs[java.sql.Timestamp]("policy_start_date")
        val policyEndDate = x.getAs[java.sql.Timestamp]("policy_end_date")
        val lastCusTypeRes = JSON.parseArray(lastCusType)
        val cuurTimeNew = Timestamp.valueOf(currTime)

        /**
          * 沉睡
          * 客户标签为流失，且近180天未在保&未投保的客户
          */
        if(cusType == "4"){
          if(policyEndDate != null){
            val days2 = getBeg_End_one_two_new(policyEndDate.toString,cuurTimeNew.toString) //终止天数--未投保天数 复购天数
            if(days2 > 180){//在保
              cusType = "5"
              lastCusTypeRes.add(("4",timeSubstring(becomeCurrCusTime)))
              becomeCurrCusTime = currTimeFuction(policyEndDate.toString,180)
            }
          }
        }

        /**
          * 客户标签为新客或易流失&保单到期&60内无复购
          */
        var days = Long.MinValue
        var lossTime = ""
        if(policyEndDate != null){
          days = getBeg_End_one_two_new(policyEndDate.toString,currTime)//保险止期和当前日期所差天数
          lossTime = currTimeFuction(policyEndDate.toString,60)
        }
        if(cusType == "1" && days > 60){
          cusType = "4"
          becomeCurrCusTime = timeSubstring(lossTime)
        }
        if(cusType == "3" && days > 60){
          lastCusTypeRes.add(("3",timeSubstring(becomeCurrCusTime)))
          cusType = "4"
          becomeCurrCusTime = timeSubstring(lossTime)
        }

        /**
          * 易流失
          * 如果不是在保保单 30天内无复购 - 易流失
          * 如果在保保单保障期间大于30 天  而且还与30天到期的 易流失 如果保障期间小30 30天内无复购 易流失
          */
        if(cusType == "2"){
          if(policyEndDate != null && policyStartDate != null ){
            if((cuurTimeNew.compareTo(policyEndDate) <= 0) && (cuurTimeNew.compareTo(policyStartDate) >= 0)){//在保
            val days1 = getBeg_End_one_two_new(policyStartDate.toString,policyEndDate.toString)//保障期间
            val days2 = getBeg_End_one_two_new(cuurTimeNew.toString,policyEndDate.toString) //终止天数
            var days3 = Long.MaxValue
              if(policyNewStartDate != null){
                days3 = getBeg_End_one_two_new(cuurTimeNew.toString,policyNewStartDate.toString) //复购天数
              }
              if(days1 >= 30 && days2 <= 60){//长期险
                cusType = "3"
                lastCusTypeRes.add(("2",timeSubstring(becomeCurrCusTime)))
                becomeCurrCusTime = timeSubstring(cuurTimeNew.toString)
              }else if(days1 < 30 && days3 < 30){ //短期险
                cusType = "3"
                lastCusTypeRes.add(("2",timeSubstring(becomeCurrCusTime)))
                becomeCurrCusTime = timeSubstring(cuurTimeNew.toString)
              }
            }else{//不在保
              var days3 = Long.MinValue
              var easyToLossTime = ""
              if(policyEndDate != null){ //不在保情况
                days3 = getBeg_End_one_two_new(policyEndDate.toString,cuurTimeNew.toString) //复购天数
                if(policyEndDate.compareTo(Timestamp.valueOf(becomeCurrCusTime)) >= 0){
                  easyToLossTime = currTimeFuction(policyEndDate.toString,30)
                }else{
                  easyToLossTime = currTimeFuction(becomeCurrCusTime.toString,1)
                }
              }
              if(days3 > 30){
                if(easyToLossTime != ""){
                  cusType = "3"
                  lastCusTypeRes.add(("2",timeSubstring(becomeCurrCusTime)))
                  becomeCurrCusTime = timeSubstring(easyToLossTime)
                }
              }
            }
          }
        }

        /**
          * 老客
          */
        //两个成为老客时间小于当前时间兵器《最大止期时间+60d
        if(cusType == "1" && (Timestamp.valueOf(becomeOldTime).compareTo(cuurTimeNew) <= 0)){
          if(policyEndDate != null && getBeg_End_one_two_new(policyEndDate.toString,becomeOldTime) <= 60){
            cusType = "2"
            becomeCurrCusTime = timeSubstring(becomeOldTime)
          }
        }

        val jsonString = JSON.toJSONString(lastCusTypeRes, SerializerFeature.BeanToArray)
        (certNo,cusType,becomeCurrCusTime,jsonString)
      })
      .toDF("cert_no","cus_type","become_curr_cus_time","last_cus_type")
    res.printSchema()
    res
  }

  /**
    * 新增的数据
    * 客户类型 是否是挽回客户 是否是被唤醒客户 挽回时长  唤醒时长 原客户类型
    * @param sqlContext sql上下文
    * @param dwPolicyDetailInc 增量的保单信息
    * @param policyHolderData 增量的投保人和保单信息
    * @param odsPolicyInsuredDetail 被保人明细表
    * @param odsPolicyInsuredSlaveDetail 从被保人明细表
    * @param hbaseData hbase data
    */
  def getNewInfo(sqlContext:HiveContext,dwPolicyDetailInc:DataFrame,policyHolderData:DataFrame,odsPolicyInsuredDetail:DataFrame,
                 odsPolicyInsuredSlaveDetail:DataFrame,hbaseData:DataFrame) = {

    import sqlContext.implicits._
    /**
      * 增量的被保人信息
      */
    val incInsuredDetail = dwPolicyDetailInc.join(odsPolicyInsuredDetail,dwPolicyDetailInc("policy_id")===odsPolicyInsuredDetail("policy_id_insured"))
      .selectExpr("policy_id","insured_id","insured_cert_no","start_date","end_date")

    /**
      * 新增的从被保人信息
      */
    val incSlaveDetail = incInsuredDetail.join(odsPolicyInsuredSlaveDetail,incInsuredDetail("insured_id")===odsPolicyInsuredSlaveDetail("master_id"))
      .selectExpr("policy_id","slave_cert_no","start_date_slave","end_date_slave")

    /**
      * 新增的被保人和从被保人
      */
    val incInsuredSlaveDetail = incInsuredDetail.selectExpr("insured_cert_no as cert_no","start_date","end_date")
      .unionAll(incSlaveDetail.selectExpr("slave_cert_no as cert_no","start_date_slave as start_date","end_date_slave as end_date"))
      .distinct()

    /**
      * 被保人和从被保人以及投保人信息
      */
    val insuredSlaveHolderDetail = incInsuredSlaveDetail.join(policyHolderData,incInsuredSlaveDetail("cert_no")===policyHolderData("holder_cert_no"),"fullouter")
      .selectExpr("cert_no","start_date","end_date","policy_start_date","policy_end_date","holder_cert_no")

    /**
      * 新增投保人
      */
    val incHolderRes = insuredSlaveHolderDetail.where("holder_cert_no is not null")
      .selectExpr("holder_cert_no")
      .distinct()

    /**
      * 新增被保人
      */
    val incInsuredRes = insuredSlaveHolderDetail.where("holder_cert_no is null and cert_no is not null")
      .selectExpr("cert_no as cert_no_insured")
      .distinct()

    val nowTime = getNowTime()

    val incHolderResult = incHolderRes.join(hbaseData,incHolderRes("holder_cert_no")===hbaseData("cert_no"),"leftouter")
      .map(x => {
        val holderCertNo = x.getAs[String]("holder_cert_no")
        val certNo = x.getAs[String]("cert_no")
        var cusType = x.getAs[String]("cus_type")
        val lastCusType = x.getAs[String]("last_cus_type")
        var becomeCurrCusTime = x.getAs[String]("become_curr_cus_time")
        val lastCusTypeRes =  JSON.parseArray(lastCusType)
        //空的集合
        val arrayList = new util.ArrayList[(String, String)]
        //是否是被挽回客户
        var is_redeem = ""
        //是否是被唤醒客户
        var is_awaken = ""
        //挽回时长
        var redeem_time = ""
        //唤醒时长
        var awaken_time = ""

        if(certNo  !=  null){
          if(cusType == "0"){ //从潜在客户变成新客
            cusType = "1"
            if(lastCusTypeRes == null){
              arrayList.add(("0",null))
            }else{
              lastCusTypeRes.add(("0",null))
            }
            becomeCurrCusTime = timeSubstring(nowTime)
            is_redeem = null
            is_awaken = null
            redeem_time = null
            awaken_time = null
          }
          if(cusType == "3" || cusType == "4" || cusType == "5"){  //如果是易流失或者流失或者沉睡重新投保单 则变成老客  并且是回归客户
            lastCusTypeRes.add((cusType,becomeCurrCusTime))
            if(cusType == "3" || cusType == "4"){
              is_redeem = "是"
              redeem_time = getBeg_End_one_two_new(becomeCurrCusTime,nowTime).toString
            }else{
              is_redeem = null
              redeem_time = null
            }
            if(cusType == "5"){
              is_awaken = "是"
              awaken_time = getBeg_End_one_two_new(becomeCurrCusTime,nowTime).toString
            }else{
              is_awaken = null
              awaken_time = null
            }
            becomeCurrCusTime = timeSubstring(nowTime)
            cusType = "2"
          }
        }else{
          cusType = "1"
          becomeCurrCusTime = timeSubstring(nowTime)
          arrayList.add(("1",becomeCurrCusTime))
          is_redeem = null
          redeem_time = null
          is_awaken = null
          awaken_time = null
        }
        val jsonString = if(!arrayList.isEmpty){
          JSON.toJSONString(arrayList, SerializerFeature.BeanToArray)
        }else{
          JSON.toJSONString(lastCusTypeRes, SerializerFeature.BeanToArray)
        }
        (holderCertNo,cusType,jsonString,becomeCurrCusTime,is_redeem,is_awaken,redeem_time,awaken_time)
      })
      .toDF("cert_no","cus_type","last_cus_type","become_curr_cus_time","is_redeem","is_awaken","redeem_time","awaken_time")

    incHolderResult.printSchema()

    /**
      * 被保人客户类型
      */
    val incInsuredResult = incInsuredRes.join(hbaseData,incInsuredRes("cert_no_insured")===hbaseData("cert_no"),"leftouter")
      .map(x => {
        val certNoInsured = x.getAs[String]("cert_no_insured")
        val certNo = x.getAs[String]("cert_no")
        var cusType = x.getAs[String]("cus_type")
        if(certNo == null){
          cusType = "0"
        }
        (certNoInsured,cusType)
      })
      .toDF("cert_no","cus_type")

    incInsuredResult.printSchema()
    incInsuredResult
  }

}
