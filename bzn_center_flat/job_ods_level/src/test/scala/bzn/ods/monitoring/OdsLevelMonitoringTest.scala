package bzn.ods.monitoring

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.text.SimpleDateFormat
import java.util.{Date, Properties, UUID}

import bzn.job.common.Until
import bzn.ods.util.SparkUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext

/**
  * author:xiaoYuanRen
  * Date:2019/6/5
  * Time:9:27
  * describe: 对ods层的数据进行预警
  **/
object OdsLevelMonitoringTest extends SparkUtil with Until{
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName,"local[*]")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4

    hiveContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    hiveContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")//设置日期格式
      val date = df.format(new Date())// new Date()为获取当前系统时间
      (date + "")
    })

    import hiveContext.implicits._
    val res1 = monitoringPolicyInfo(hiveContext,sc)
    val res2 = monitoringPolicyInsuredInfo(hiveContext,sc)
    val res3 = monitoringPreserveInfo(hiveContext,sc)
    val res4 = monitoringPreserveInsuredInfo(hiveContext,sc)
    val resUnion = res1.union(res2).union(res3).union(res4)
    val resDF = resUnion.toDF("monotoring_name","curr_value","true_value","isOrNot_monotoring","monotoring_desc","monotoring_source")
      .selectExpr("getUUID() as id","monotoring_name","curr_value","true_value","isOrNot_monotoring","monotoring_desc","monotoring_source","getNow() as dw_create_time")
//    resDF.write.mode(SaveMode.Overwrite).saveAsTable("odsdb.ods_monotoring_detail")
    resDF.printSchema()
    sc.stop()
  }

  /**
    * 保全预警信息
    */
  def monitoringPreserveInsuredInfo(sqlContext:HiveContext,sc:SparkContext) ={
    /**
      * 读取保单明细表
      */
    val odsPolicyInsuredDetail = sqlContext.sql("select preserve_id,insured_cert_type,preserve_type from odsdb.ods_preservation_master_detail")
      .cache()
    /**
      * 查询所有保全id长度，结果拼接成字符串
      */
    val lengthPreserveId: Seq[Int] = odsPolicyInsuredDetail.selectExpr("length(preserve_id) as lengthPreserveId")
      .map(x => {
        val lengthPreserveId = x.getAs[Int]("lengthPreserveId")
        lengthPreserveId
      }).distinct()
      .collect().toList.sorted
    val lengthPreserveIdList = Seq(13,18).sorted
    val lengthPreserveIdRes = if(lengthPreserveId.sameElements(lengthPreserveIdList)){
      //监控字段+当前值+正确值+预警+预警描述+监控来源
      "preserve_id"+"\u0001"+lengthPreserveId.mkString(" ")+"\u0001"+lengthPreserveIdList.mkString(" ")+"\u0001"+"0"+"\u0001"+"无"+"\u0001"+"odsdb.ods_preservation_master_detail"
    }else{
      "preserve_id"+"\u0001"+lengthPreserveId.mkString(" ")+"\u0001"+lengthPreserveIdList.mkString(" ")+"\u0001"+"1"+"\u0001"+"保全长度有变化"+"\u0001"+"odsdb.ods_preservation_master_detail"
    }

    /**
      * 保全类型
      */
    val preserveType: Seq[Int] = odsPolicyInsuredDetail.selectExpr("preserve_type")
      .map(x => {
        val preserveType = x.getAs[Int]("preserve_type")
        preserveType
      }).distinct()
      .collect().toList.sorted
    val preserveTypeList = Seq(1,2,-1).sorted
    val preserveTypeRes = if(preserveType.sameElements(preserveTypeList)){
      //监控字段+当前值+正确值+预警+预警描述+监控来源
      "preserve_type"+"\u0001"+preserveType.mkString(" ")+"\u0001"+preserveTypeList.mkString(" ")+"\u0001"+"0"+"\u0001"+"无"+"\u0001"+"odsdb.ods_preservation_master_detail"
    }else{
      "preserve_type"+"\u0001"+preserveType.mkString(" ")+"\u0001"+preserveTypeList.mkString(" ")+"\u0001"+"1"+"\u0001"+"保全类型有变化"+"\u0001"+"odsdb.ods_preservation_master_detail"
    }

    /**
      * 证件号类型
      */
    val insuredCertType: Seq[Int] = odsPolicyInsuredDetail.selectExpr("insured_cert_type")
      .map(x => {
        var insuredCertType = x.getAs[Int]("insured_cert_type")
        insuredCertType
      }).distinct()
      .collect().toList.sorted
    val insuredCertTypeList = Seq(1,-1).sorted
    val insuredCertTypeRes = if(insuredCertType.sameElements(insuredCertTypeList)){
      //监控字段+当前值+正确值+预警+预警描述+监控来源
      "insured_cert_type"+"\u0001"+insuredCertType.mkString(" ")+"\u0001"+insuredCertTypeList.mkString(" ")+"\u0001"+"0"+"\u0001"+"无"+"\u0001"+"odsdb.ods_preservation_master_detail"
    }else{
      "insured_cert_type"+"\u0001"+insuredCertType.mkString(" ")+"\u0001"+insuredCertTypeList.mkString(" ")+"\u0001"+"1"+"\u0001"+"证件号类型有变化"+"\u0001"+"odsdb.ods_preservation_master_detail"
    }

    val res1 = sc.parallelize(List(lengthPreserveIdRes,preserveTypeRes,insuredCertTypeRes))
      .map(x => {
        val split = x.split("\u0001")
        //监控字段+当前值+正确值+预警+预警描述+监控来源
        (split(0),split(1),split(2),split(3),split(4),split(5))
      })
    res1
  }


  /**
    * 保全预警信息
    */
  def monitoringPreserveInfo(sqlContext:HiveContext,sc:SparkContext) ={
    /**
      * 读取保单明细表
      */
    val odsPolicyInsuredDetail = sqlContext.sql("select preserve_id,policy_id,preserve_status,pay_status from odsdb.ods_preservation_detail")
      .cache()
    /**
      * 查询所有保单长度，结果拼接成字符串
      */
    val policyId: Seq[Int] = odsPolicyInsuredDetail.selectExpr("length(policy_id) as lengthPolicyId")
      .map(x => {
        val lengthPolicyId = x.getAs[Int]("lengthPolicyId")
        lengthPolicyId
      }).distinct()
      .collect().toList.sorted
    val policyList = Seq(18,32).sorted
    val policyRes = if(policyList.sameElements(policyId)){
      //监控字段+当前值+正确值+预警+预警描述+监控来源
      "policy_id"+"\u0001"+policyId.mkString(" ")+"\u0001"+policyList.mkString(" ")+"\u0001"+"0"+"\u0001"+"无"+"\u0001"+"odsdb.ods_preservation_detail"
    }else{
      "policy_id"+"\u0001"+policyId.mkString(" ")+"\u0001"+policyList.mkString(" ")+"\u0001"+"1"+"\u0001"+"保单长度有变化"+"\u0001"+"odsdb.ods_preservation_detail"
    }

    /**
      * 查询所有保全id长度，结果拼接成字符串
      */
    val lengthPreserveId: Seq[Int] = odsPolicyInsuredDetail.selectExpr("length(preserve_id) as lengthPreserveId")
      .map(x => {
        val lengthPreserveId = x.getAs[Int]("lengthPreserveId")
        lengthPreserveId
      }).distinct()
      .collect().toList.sorted
    val lengthPreserveIdList = Seq(13,14,18).sorted
    val lengthPreserveIdRes = if(lengthPreserveId.sameElements(lengthPreserveIdList)){
      //监控字段+当前值+正确值+预警+预警描述+监控来源
      "preserve_id"+"\u0001"+lengthPreserveId.mkString(" ")+"\u0001"+lengthPreserveIdList.mkString(" ")+"\u0001"+"0"+"\u0001"+"无"+"\u0001"+"odsdb.ods_preservation_detail"
    }else{
      "preserve_id"+"\u0001"+lengthPreserveId.mkString(" ")+"\u0001"+lengthPreserveIdList.mkString(" ")+"\u0001"+"1"+"\u0001"+"保全长度有变化"+"\u0001"+"odsdb.ods_preservation_detail"
    }

    /**
      * 保全状态
      */
    val preserveStatus: Seq[String] = odsPolicyInsuredDetail.selectExpr("preserve_status")
      .map(x => {
        val preserveStatus = x.getAs[String]("preserve_status")
        preserveStatus
      }).distinct()
      .collect().toList.sorted
    val preserveStatusList = Seq("1","0","-1").sorted
    val preserveStatusRes = if(preserveStatus.sameElements(preserveStatusList)){
      //监控字段+当前值+正确值+预警+预警描述+监控来源
      "preserve_status"+"\u0001"+preserveStatus.mkString(" ")+"\u0001"+preserveStatusList.mkString(" ")+"\u0001"+"0"+"\u0001"+"无"+"\u0001"+"odsdb.ods_preservation_detail"
    }else{
      "preserve_status"+"\u0001"+preserveStatus.mkString(" ")+"\u0001"+preserveStatusList.mkString(" ")+"\u0001"+"1"+"\u0001"+"保全状态有变化"+"\u0001"+"odsdb.ods_preservation_detail"
    }

    /**
      * 支付状态
      */
    val payStatus: Seq[Int] = odsPolicyInsuredDetail.selectExpr("pay_status")
      .map(x => {
        var payStatus = x.getAs[Int]("pay_status")
        if(x.getAs[Int]("pay_status") ==null){
          payStatus = -1
        }
        payStatus
      }).distinct()
      .collect().toList.sorted
    val payStatusList = Seq(1,0,-1).sorted
    val payStatusRes = if(payStatus.sameElements(payStatusList)){
      //监控字段+当前值+正确值+预警+预警描述+监控来源
      "pay_status"+"\u0001"+payStatus.mkString(" ")+"\u0001"+payStatusList.mkString(" ")+"\u0001"+"0"+"\u0001"+"无"+"\u0001"+"odsdb.ods_preservation_detail"
    }else{
      "pay_status"+"\u0001"+payStatus.mkString(" ")+"\u0001"+payStatusList.mkString(" ")+"\u0001"+"1"+"\u0001"+"支付状态有变化"+"\u0001"+"odsdb.ods_preservation_detail"
    }

    val res1 = sc.parallelize(List(policyRes,lengthPreserveIdRes,preserveStatusRes,payStatusRes))
      .map(x => {
        val split = x.split("\u0001")
        //监控字段+当前值+正确值+预警+预警描述+监控来源
        (split(0),split(1),split(2),split(3),split(4),split(5))
      })
    res1
  }

  /**
    * 保单预警信息
    * @param sqlContext
    */
  def monitoringPolicyInfo(sqlContext:HiveContext,sc:SparkContext) ={

    /**
      * 读取保单明细表
      */
    val odsPolicyDetail = sqlContext.sql("select policy_id,policy_status from odsdb.ods_policy_detail")
      .cache()
    /**
      * 查询所有保单长度，结果拼接成字符串
      */
    val policyId: Seq[Int] = odsPolicyDetail.selectExpr("length(policy_id) as lengthPolicyId")
      .map(x => {
        val lengthPolicyId = x.getAs[Int]("lengthPolicyId")
        lengthPolicyId
      }).distinct()
        .collect().toList.sorted
    val policyList = Seq(18,32).sorted
    val policyRes = if(policyList.sameElements(policyId)){
      //监控字段+当前值+正确值+预警+预警描述+监控来源
      "policy_id"+"\u0001"+policyId.mkString(" ")+"\u0001"+policyList.mkString(" ")+"\u0001"+"0"+"\u0001"+"无"+"\u0001"+"odsdb.ods_policy_detail"
    }else{
      "policy_id"+"\u0001"+policyId.mkString(" ")+"\u0001"+policyList.mkString(" ")+"\u0001"+"1"+"\u0001"+"保单长度有变化"+"\u0001"+"odsdb.ods_policy_detail"
    }

    /**
      * 保单状态
      */
    val policyStatus: Seq[Int] = odsPolicyDetail.selectExpr("policy_status")
      .map(x => {
        val policyStatus = x.getAs[Int]("policy_status")
        policyStatus
      }).distinct()
      .collect().toList.sorted
    val statusList = Seq(1,0,-1,99).sorted
    val statusRes = if(policyStatus.sameElements(statusList)){
      //监控字段+当前值+正确值+预警+预警描述+监控来源
      "policy_status"+"\u0001"+policyStatus.mkString(" ")+"\u0001"+statusList.mkString(" ")+"\u0001"+"0"+"\u0001"+"无"+"\u0001"+"odsdb.ods_policy_detail"
    }else{
      "policy_status"+"\u0001"+policyStatus.mkString(" ")+"\u0001"+statusList.mkString(" ")+"\u0001"+"1"+"\u0001"+"保单状态有变化"+"\u0001"+"odsdb.ods_policy_detail"
    }

    val res1 = sc.parallelize(List(policyRes,statusRes))
      .map(x => {
        val split = x.split("\u0001")
        //监控字段+当前值+正确值+预警+预警描述+监控来源
        (split(0),split(1),split(2),split(3),split(4),split(5))
      })
    res1
  }

  /**
    * 在保人预警信息
    */
  def monitoringPolicyInsuredInfo(sqlContext:HiveContext,sc:SparkContext) ={
    /**
      * 读取在保人明细表
      */
    val odsPolicyInsuredDetail = sqlContext.sql("select policy_id,insured_cert_type,insured_status,policy_status from odsdb.ods_policy_insured_detail")
      .cache()
    /**
      * 查询所有保单长度，结果拼接成字符串
      */
    val policyId: Seq[Int] = odsPolicyInsuredDetail.selectExpr("length(policy_id) as lengthPolicyId")
      .map(x => {
        val lengthPolicyId = x.getAs[Int]("lengthPolicyId")
        lengthPolicyId
      }).distinct()
      .collect().toList.sorted
    val policyList = Seq(0,18,32).sorted
    val policyRes = if(policyList.sameElements(policyId)){
      //监控字段+当前值+正确值+预警+预警描述+监控来源
      "policy_id"+"\u0001"+policyId.mkString(" ")+"\u0001"+policyList.mkString(" ")+"\u0001"+"0"+"\u0001"+"无"+"\u0001"+"odsdb.ods_policy_insured_detail"
    }else{
      "policy_id"+"\u0001"+policyId.mkString(" ")+"\u0001"+policyList.mkString(" ")+"\u0001"+"1"+"\u0001"+"保单长度有变化"+"\u0001"+"odsdb.ods_policy_insured_detail"
    }

    /**
      * 保单状态
      */
    val policyStatus: Seq[String] = odsPolicyInsuredDetail.selectExpr("policy_status")
      .map(x => {
        val policyStatus = x.getAs[String]("policy_status")
        policyStatus
      }).distinct()
      .collect().toList.sorted
    val statusList = Seq("1","0").sorted
    val statusRes = if(policyStatus.sameElements(statusList)){
      //监控字段+当前值+正确值+预警+预警描述+监控来源
      "policy_status"+"\u0001"+policyStatus.mkString(" ")+"\u0001"+statusList.mkString(" ")+"\u0001"+"0"+"\u0001"+"无"+"\u0001"+"odsdb.ods_policy_insured_detail"
    }else{
      "policy_status"+"\u0001"+policyStatus.mkString(" ")+"\u0001"+statusList.mkString(" ")+"\u0001"+"1"+"\u0001"+"保单状态有变化"+"\u0001"+"odsdb.ods_policy_insured_detail"
    }

    /**
      * 证件号类型
      */
    val insuredCertType: Seq[String] = odsPolicyInsuredDetail.selectExpr("insured_cert_type")
      .map(x => {
        val insuredCertType = x.getAs[String]("insured_cert_type")
        insuredCertType
      }).distinct()
      .collect().toList.sorted
    val insuredCertTypeList = Seq("1","-1").sorted
    val insuredCertTypeRes = if(insuredCertType.sameElements(insuredCertTypeList)){
      //监控字段+当前值+正确值+预警+预警描述+监控来源
      "insured_cert_type"+"\u0001"+insuredCertType.mkString(" ")+"\u0001"+insuredCertTypeList.mkString(" ")+"\u0001"+"0"+"\u0001"+"无"+"\u0001"+"odsdb.ods_policy_insured_detail"
    }else{
      "insured_cert_type"+"\u0001"+insuredCertType.mkString(" ")+"\u0001"+insuredCertTypeList.mkString(" ")+"\u0001"+"1"+"\u0001"+"证件号类型有误"+"\u0001"+"odsdb.ods_policy_insured_detail"
    }

    /**
      * 在保人在职状态
      */
    val insuredStatus: Seq[String] = odsPolicyInsuredDetail.selectExpr("insured_status")
      .map(x => {
        val insuredStatus = x.getAs[String]("insured_status")
        insuredStatus
      }).distinct()
      .collect().toList.sorted
    val InsuredStatusList = Seq("1","0").sorted
    val InsureStatusRes = if(insuredStatus.sameElements(InsuredStatusList)){
      //监控字段+当前值+正确值+预警+预警描述+监控来源
      "insured_status"+"\u0001"+insuredStatus.mkString(" ")+"\u0001"+InsuredStatusList.mkString(" ")+"\u0001"+"0"+"\u0001"+"无"+"\u0001"+"odsdb.ods_policy_insured_detail"
    }else{
      "insured_status"+"\u0001"+insuredStatus.mkString(" ")+"\u0001"+InsuredStatusList.mkString(" ")+"\u0001"+"1"+"\u0001"+"在保人在职有变化"+"\u0001"+"odsdb.ods_policy_insured_detail"
    }


    val res1 = sc.parallelize(List(policyRes,statusRes,insuredCertTypeRes,InsureStatusRes))
      .map(x => {
        val split = x.split("\u0001")
        //监控字段+当前值+正确值+预警+预警描述+监控来源
        (split(0),split(1),split(2),split(3),split(4),split(5))
      })
    res1
  }

}
