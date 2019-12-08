package bzn.dw.premium

import java.text.SimpleDateFormat
import java.util.Date
import java.util.regex.Pattern

import bzn.dw.util.SparkUtil
import bzn.job.common.Until
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * author:xiaoYuanRen
  * Date:2019/6/13
  * Time:15:22
  * describe: 理赔表的预估赔付
  **/
object DwPolicyClaimDetail extends SparkUtil with Until{
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName,"")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    val res = dwPolicyClaimDetail(hiveContext)
    hiveContext.sql("truncate table dwdb.dw_policy_claim_detail")
    res.repartition(1).write.mode(SaveMode.Append).saveAsTable("dwdb.dw_policy_claim_detail")
//    res.repartition(1).write.mode(SaveMode.Overwrite).parquet("/dw_data/dw_data/dw_policy_claim_detail")
    sc.stop()
  }

  /**
    * 理赔数据和保单数据整合
    * @param sqlContext 上下文
    */
  def dwPolicyClaimDetail(sqlContext:HiveContext) ={
    import sqlContext.implicits._
    sqlContext.udf.register("clean", (str: String) => clean(str))
    sqlContext.udf.register("getUUID",()=>(java.util.UUID.randomUUID() + "").replace("-", ""))
    sqlContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")//设置日期格式
      val date = df.format(new Date())// new Date()为获取当前系统时间
      date + ""
    })
    /**
      * 读取保单表
      */
    val odsPolicyDetail = sqlContext.sql("select policy_id,policy_code,product_code," +
      "holder_name,policy_status from odsdb.ods_policy_detail")
      .where("policy_status in (0,1,-1)")

    /**
      * 读取理赔表
      */
    val odsClaimDetailOne = sqlContext.sql("select id, clean(case_no) as case_no, " +
      "clean(policy_no) as policy_no, " +
      "clean(risk_date) as risk_date, " +
      "clean(report_date) as report_date, clean(risk_name) as risk_name," +
      "clean(risk_cert_no) as risk_cert_no, clean(mobile) as mobile, " +
      "clean(insured_company) as insured_company, " +
      "pre_com, clean(disable_level) as disable_level, clean(scene) as scene, " +
      "clean(case_type) as case_type, clean(case_status) as case_status, " +
      "clean(case_close_date) as case_close_date, " +
      "cast(clean(hos_benefits) as decimal(14,4)) as hos_benefits, " +
      "cast(clean(medical_coverage) as decimal(14,4)) as medical_coverage, " +
      "cast(clean(delay_payment) as decimal(14,4)) as delay_payment, " +
      "cast(clean(disable_death_payment) as decimal(14,4)) as disable_death_payment," +
      "final_payment from odsdb.ods_claims_detail")

    /**
      * 如果最终赔付有值就用最终赔付，如果没有值就用预估赔付
      */
    val odsClaimDetailTwo = sqlContext.sql("select id,pre_com,final_payment,trim(risk_date) as risk_date from odsdb.ods_claims_detail")
      .map(x => {
        val id: String = clean(x.getAs[Long]("id").toString)

        val preCom: String = x.getAs[String]("pre_com")

        val riskDate = x.getAs[String]("risk_date")

        /**
          * 将出险时间改为正常的时间格式
          */
        val riskDateRes = if (riskDate != null && riskDate.length > 7){
          java.sql.Timestamp.valueOf(getFormatTime(getBeginTime(riskDate.replaceAll("/", "-").concat(" 00:00:00"))))
        }else{
          null
        }

        val finalPayment: String =  x.getAs[String]("final_payment")
        val finalPaymentRes = if (finalPayment == null || finalPayment == "" ) preCom else finalPayment
        //保单号  预估赔付   最终赔付        赔付
        (id,preCom,finalPayment,finalPaymentRes,riskDateRes)
      })
      .toDF("id_slave","pre_com_new","final_payment_new","res_pay","risk_date_res")

    val odsClaimDetail = odsClaimDetailOne.join(odsClaimDetailTwo,odsClaimDetailOne("id") === odsClaimDetailTwo("id_slave"))
      .selectExpr(
        "id_slave","case_no","policy_no",
        "risk_date","risk_date_res","report_date","risk_name","risk_cert_no",
        "mobile","insured_company",
        "pre_com_new",
        "disable_level","scene","case_type","case_status","case_close_date",
        "hos_benefits","medical_coverage",
        "delay_payment","disable_death_payment",
        "final_payment_new",
        "res_pay"
      )

    odsClaimDetail.where("policy_no is not null and res_pay is not null").show()
    /**
      * 保单明细数据和理赔明细数据通过保单号关联
      */
    val res = odsPolicyDetail.join(odsClaimDetail,odsPolicyDetail("policy_code") === odsClaimDetail("policy_no"),"leftouter")
      .selectExpr("getUUID() as id","id_slave","policy_id","policy_code","product_code","policy_no",
        "policy_status","case_no","policy_no as risk_policy_code",
        "risk_date","risk_date_res","report_date","risk_name","holder_name","risk_cert_no",
        "mobile","insured_company","cast(pre_com_new as decimal(14,4)) as pre_com",
        "disable_level",
        "scene","case_type","case_status","case_close_date","hos_benefits",
        "medical_coverage","delay_payment","disable_death_payment",
        "cast(final_payment_new as decimal(14,4)) as final_payment",
        "cast(res_pay as decimal(14,4)) as res_pay","getNow() as dw_create_time")

    //读取企业信息表
    val odsEnterpriseDetail: DataFrame = sqlContext.sql("select ent_id,ent_name from " +
      "odsdb.ods_enterprise_detail")

    //读取客户归属销售表
    val odsEntguzhuSalesmanDetail: DataFrame = sqlContext.sql("select ent_id as entid,ent_name as entname," +
      "channel_id,channel_name from odsdb.ods_ent_guzhu_salesman_detail")

    // 关联两个表
    val enterAndsalesman: DataFrame =
      odsEnterpriseDetail.join(odsEntguzhuSalesmanDetail,odsEnterpriseDetail("ent_id") === odsEntguzhuSalesmanDetail("entid"),"leftouter")
        .selectExpr("entid","entname","channel_id","channel_name")

    // 将理赔表与保单明细表的结果 与 客户归属销售表和企业信息表的结果关联
    val resEnd: DataFrame = res.join(enterAndsalesman, res("holder_name") === enterAndsalesman("entname"), "leftouter")
      .selectExpr(
        "id","id_slave as id_risk","policy_id", "policy_code", "product_code", "policy_status",
        "case_no", "risk_policy_code",
        "risk_date", "cast(risk_date_res as timestamp) as risk_date_res","report_date", "risk_name", "risk_cert_no", "mobile",
        " entid as ent_id","entname as ent_name","channel_id ","channel_name", "insured_company",
        "pre_com", "disable_level",
        "scene", "case_type", "case_status", "case_close_date", "hos_benefits",
        "medical_coverage",
        "delay_payment", "disable_death_payment",
        "final_payment",
        "res_pay", "dw_create_time"
      )
    resEnd
  }

  /**
    * 过滤含有中文字段
    */
  def containChiness(data:DataFrame) ={
    val res = data.map(x => x)
      .filter(x => {
        val policyNo =x.getAs[String]("policy_no")
        val preCom =x.getAs[String]("pre_com")
        val p = Pattern.compile("[\u4e00-\u9fa5]")
        val m = p.matcher(preCom)
        if (m.find) true else false
      })
    res
  }
}
