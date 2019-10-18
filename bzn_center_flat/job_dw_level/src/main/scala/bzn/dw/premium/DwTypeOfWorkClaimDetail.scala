package bzn.dw.premium

import java.text.SimpleDateFormat
import java.util.Date

import bzn.dw.util.SparkUtil
import bzn.job.common.Until
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext

/*
* @Author:liuxiang
* @Date：2019/9/26
* @Describe:
*/ object DwTypeOfWorkClaimDetail extends SparkUtil with Until {
  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    val res = DwTypeOfWorkMatching(hiveContext)
    hiveContext.sql("truncate table dwdb.dw_work_type_matching_claim_detail")
    res.repartition(10).write.mode(SaveMode.Append).saveAsTable("dwdb.dw_work_type_matching_claim_detail")
    sc.stop()

  }

  /**
    *
    * @param sqlContext
    * @return
    */
  def DwTypeOfWorkMatching(sqlContext: HiveContext) = {
    import sqlContext.implicits._
    sqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    sqlContext.udf.register("clean", (str: String) => clean(str))
    sqlContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      //设置日期格式
      val date = df.format(new Date()) // new Date()为获取当前系统时间
      (date + "")
    })

    /**
      * 读取每日已赚保费表
      */
    val dwYearAndMonthInsuredPremiumDetail = sqlContext.sql("select policy_id,insured_cert_no,insured_start_date,sku_day_price,day_id from dwdb.dw_year_and_month_insured_premium_detail")
      .where("cast(day_id as int) <= cast(regexp_replace(cast(current_date() as string),'-','') as int)")
      .map(x => {
        val policyId = x.getAs[String]("policy_id")
        val insuredCertNo = x.getAs[String]("insured_cert_no")
        val insuredStartDate = x.getAs[java.sql.Timestamp]("insured_start_date")
        val skuDayPrice = x.getAs[java.math.BigDecimal]("sku_day_price")
        ((policyId,insuredCertNo,insuredStartDate),skuDayPrice)
      })
      .reduceByKey(_.add(_))
      .map( x => {
        (x._1._1,x._1._2,x._1._3,x._2)
      })
      .toDF("policy_id_pemium","insured_cert_no_premium","start_date_premium","days_promium")

    /**
      * 读取dw层工种匹配表
      */
    val res1 = sqlContext.sql(" select id,policy_id, policy_code, sku_coverage,sku_append," +
      "sku_ratio,sku_price,sku_charge_type, holder_name, product_code, product_name, profession_type, " +
      "channel_id, channel_name, insured_subject,sum_premium, insure_company_name, insure_company_short_name,insured_name, " +
      "insured_cert_no, start_date,end_date," +
      "work_type,primitive_work,job_company, gender, age, " +
      "bzn_work_name,work_name,bzn_work_risk,gs_work_risk,recognition, whether_recognition,plan_recognition,gs_plan_recognition from dwdb.dw_work_type_matching_detail")

    val res2 = res1.selectExpr("policy_id","insured_cert_no","start_date","end_date")

    /**
      * 读取dw层的理赔表
      */
    val dwPolicyClaim = sqlContext.sql("select  policy_id as id_temp ,risk_cert_no,risk_date ,res_pay from dwdb.dw_policy_claim_detail")

    // 将res2与理赔表进行关联
    val res3: DataFrame = res2.join(dwPolicyClaim, 'policy_id === 'id_temp and 'insured_cert_no === 'risk_cert_no)
      .where("start_date is not null")
      .selectExpr("policy_id", "insured_cert_no", "start_date", "end_date", "risk_date","res_pay")
      .map( x=> {
        val policy_id = x.getAs[String]("policy_id")
        val insured_cert_no = x.getAs[String]("insured_cert_no")
        val start_date = x.getAs[java.sql.Timestamp]("start_date")
        val end_date = x.getAs[java.sql.Timestamp]("end_date")
        val riskDate = x.getAs[String]("risk_date")
        val riskDateRes = if(riskDate != null){
          java.sql.Timestamp.valueOf(getFormatTime(getBeginTime(riskDate.replaceAll("/", "-").concat(" 00:00:00"))))
        }else{
          null
        }
        println(start_date+"  "+end_date +"  "+riskDateRes)
        val res_pay = x.getAs[java.math.BigDecimal]("res_pay")
        (policy_id, insured_cert_no, start_date, end_date, riskDateRes,res_pay)
      }).toDF("id_temp", "risk_cert_no", "start_date", "end_date", "risk_date","res_pay")

    val res4 = res3.selectExpr("id_temp", "risk_cert_no", " start_date ", "end_date", "risk_date","res_pay",
      "case when risk_date is null then 1 when risk_date >= start_date and risk_date <= end_date then 1 else 0 end as tem")
      .where("tem = 1")

    val res5 = res4.selectExpr("id_temp", "risk_cert_no", "start_date as start_date_temp ", "end_date as end_date_temp", "risk_date", "res_pay",
      "tem")

    res5.registerTempTable("tempTable")
    val tenmpTbles = sqlContext.sql("select sum(res_pay) as res_pay,id_temp,risk_cert_no,start_date_temp from tempTable group by id_temp, risk_cert_no,start_date_temp")
    //sqlContext.sql("select sum(res_pay) as res_pay from tempTable").show()

    val res6 = res1.join(tenmpTbles, 'policy_id === 'id_temp and 'insured_cert_no === 'risk_cert_no and 'start_date === 'start_date_temp,"leftouter")
      .selectExpr( "id","policy_id", "policy_code", "sku_coverage", "sku_append", "sku_ratio",
        "sku_price", "sku_charge_type","holder_name",
        "product_code", "product_name", "profession_type", "channel_id", "channel_name",
        "insured_subject","sum_premium", "insure_company_name", "insure_company_short_name","insured_name", "insured_cert_no",
        "start_date", "end_date", "work_type", "primitive_work", "job_company", "gender", "age",
        "bzn_work_name", "work_name", "bzn_work_risk","gs_work_risk",
        "recognition", "whether_recognition","plan_recognition","gs_plan_recognition",
        "cast((case when res_pay is null then 0 else res_pay end) as decimal(14,4)) as res_pay")

    val res = res6.join(dwYearAndMonthInsuredPremiumDetail,'policy_id === 'policy_id_pemium and 'insured_cert_no === 'insured_cert_no_premium and 'start_date === 'start_date_premium,"leftouter")
      .selectExpr(
        "id",
        "policy_id",
        "policy_code",
        "sku_coverage",
        "sku_append",
        "sku_ratio",
        "sku_price",
        "sku_charge_type",
        "holder_name",
        "product_code",
        "product_name",
        "profession_type",
        "channel_id",
        "channel_name",
        "insured_subject",
        "sum_premium",
        "insure_company_name",
        "insure_company_short_name",
        "insured_name",
        "insured_cert_no",
        "start_date",
        "end_date",
        "work_type",
        "primitive_work",
        "job_company",
        "gender", "age",
        "bzn_work_name",
        "work_name",
        "bzn_work_risk",
        "gs_work_risk",
        "recognition",
        "whether_recognition",
        "plan_recognition",
        "gs_plan_recognition",
        "res_pay",
        "cast(days_promium as decimal(14,4)) as charge_premium",
        "getNow() as dw_create_time"
      )
    res
  }
}
