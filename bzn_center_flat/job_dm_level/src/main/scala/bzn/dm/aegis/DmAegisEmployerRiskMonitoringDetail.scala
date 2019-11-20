package bzn.dm.aegis

import java.text.SimpleDateFormat
import java.util.Date

import bzn.dm.util.SparkUtil
import bzn.job.common.{ClickHouseUntil, Until}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * author:xiaoYuanRen
  * Date:2019/11/19
  * Time:14:35
  * describe: 雇主风险监控
  **/
object DmAegisEmployerRiskMonitoringDetail extends SparkUtil with Until with ClickHouseUntil{
  def main (args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    val res = getAegisEmployerRiskMonitoring(hiveContext)
    hiveContext.sql("truncate table dmdb.dm_aegis_emp_risk_monitor_detail")
    res.repartition(100).write.mode(SaveMode.Append).saveAsTable("dmdb.dm_aegis_emp_risk_monitor_detail")
    val tableName = "emp_risk_monitor_kri_detail"
    val url = "clickhouse.url"
    val user = "clickhouse.username"
    val possWord = "clickhouse.password"
    val driver = "clickhouse.driver"
    writeClickHouseTable(res:DataFrame,tableName: String,SaveMode.Overwrite,url:String,user:String,possWord:String,driver:String)
    sc.stop()

  }

  /**
    * 得到雇主风控的基础数据
    * @param sqlContext 上下文
    */
  def getAegisEmployerRiskMonitoring(sqlContext:HiveContext): DataFrame ={
    import sqlContext.implicits._
    sqlContext.udf.register("getUUID",()=>(java.util.UUID.randomUUID() + "").replace("-", ""))
    sqlContext.udf.register("clean", (str: String) => clean(str))
    sqlContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      //设置日期格式
      val date = df.format(new Date()) // new Date()为获取当前系统时间
      date + ""
    })
    /**
      * 在保人表
      */
    val dwPolicyCurrInsuredDetail =
      sqlContext.sql("select policy_id as policy_id_insured,policy_code as policy_code_insured,day_id as day_id_insured,count from dwdb.dw_policy_curr_insured_detail")

    /**
      * 已赚保费表
      */
    val dwPolicyEverydayPremiumDetail =
      sqlContext.sql("select policy_id as policy_id_premium,day_id as day_id_premium,premium from dwdb.dw_policy_everyday_premium_detail")

    /**
      * 累计保费表
      */
    val dwPolicyPremiumDetail =
      sqlContext.sql("select policy_id,policy_code,day_id,add_person_count,del_person_count,add_premium,del_premium," +
        "case when sum_premium is null then 0 else sum_premium end as sum_premium from dwdb.dw_policy_premium_detail where one_level_pdt_cate = '蓝领外包'")
        .map(x => {
          val policyId = x.getAs[String]("policy_id")
          val policyCode = x.getAs[String]("policy_code")
          val dayId = x.getAs[String]("day_id")
          val sumPremium = x.getAs[java.math.BigDecimal]("sum_premium")
          ((policyId,policyCode,dayId),sumPremium)
        })
        .reduceByKey(_.add(_))
        .map(x => {
          (x._1._1,x._1._2,x._1._3,x._2)
        })
        .toDF("policy_id","policy_code","day_id","sum_premium")

    /**
      * 雇主基础数据表
      */
    val dwEmployerBaseinfoDetail =
      sqlContext.sql("select policy_id as policy_id_master,policy_code as policy_code_master,channel_id,channel_name,policy_start_date," +
        "case when policy_end_date is null then policy_start_date else policy_end_date end as policy_end_date,insure_company_name," +
        "insure_company_short_name,sku_charge_type from dwdb.dw_employer_baseinfo_detail")

    /**
      * 理赔表
      */
    val dwPolicyClaimDetail =
      sqlContext.sql("select policy_id,policy_code,case_no,risk_date,case_status,res_pay from dwdb.dw_policy_claim_detail")
      .where("risk_date is not null")

    val claimData = getClaimData(sqlContext,dwPolicyClaimDetail)
      .selectExpr("policy_id","policy_code","risk_date","case_no","cast(end_risk_premium as decimal(14,4)) as end_risk_premium",
        "cast(res_pay as decimal(14,4)) as res_pay")

    /**
      * 满期保费数据
      */
    val expireData = getExpirePremiumData(sqlContext,dwPolicyEverydayPremiumDetail,dwEmployerBaseinfoDetail)
      .selectExpr("policy_code_master", "expire_day_id", "expire_premium")

    /**
      * 在保人和已赚保费
      */
    val insuredAndPremium = dwPolicyCurrInsuredDetail.join(dwPolicyEverydayPremiumDetail,'policy_id_insured === 'policy_id_premium and 'day_id_insured==='day_id_premium ,"leftouter")
      .selectExpr(
        "policy_code_insured",
        "day_id_insured",
        "count as insured_count",
        "premium as charge_premium"
      )

    /**
      * 上述结果与与累计保费
      */
    val insuredAndPremiumAccPremium = insuredAndPremium.join(dwPolicyPremiumDetail,'policy_code_insured === 'policy_code and 'day_id_insured==='day_id ,"leftouter")
      .selectExpr(
        "policy_code_insured",
        "day_id_insured",
        "insured_count",
        "charge_premium",
        "sum_premium as acc_premium"
      )

    /**
      * 上述结果与理赔结果
      */
    val insuredAndPremiumAccPremiumClaim = insuredAndPremiumAccPremium.join(claimData,'policy_code_insured === 'policy_code and 'day_id_insured==='risk_date ,"leftouter")
      .selectExpr(
        "policy_code_insured",
        "day_id_insured",
        "insured_count",
        "charge_premium",
        "acc_premium",
        "case_no",
        "end_risk_premium",
        "res_pay"
      )

    /**
      * 上述结果与满期保费
      */
    val insuredAndPremiumAccPremiumClaimExpire = insuredAndPremiumAccPremiumClaim.join(expireData,'policy_code_insured === 'policy_code_master and 'day_id_insured==='expire_day_id ,"leftouter")
      .selectExpr(
        "policy_code_insured",
        "day_id_insured",
        "insured_count",
        "charge_premium",
        "acc_premium",
        "case_no",
        "end_risk_premium",
        "res_pay",
        "cast(expire_premium as decimal(14,4)) as expire_premium"
      )

    /**
      * 上述结果与基础数据信息
      */
    val res = insuredAndPremiumAccPremiumClaimExpire.join(dwEmployerBaseinfoDetail,'policy_code_insured === 'policy_code_master)
      .selectExpr(
        //"getUUID() as id",
        "channel_id",
        "channel_name",
        "policy_code_insured",
        "insure_company_name",
        "insure_company_short_name",
        "sku_charge_type",
        "day_id_insured",
        "if(insured_count is null ,0,insured_count) as insured_count",
        "if(charge_premium is null ,0,charge_premium) as charge_premium",
        "if(acc_premium is null ,0,acc_premium) as acc_premium",
        "if(case_no is null ,0,case_no) as case_no",
        "if(end_risk_premium is null ,0,end_risk_premium) as end_risk_premium",
        "if(res_pay is null ,0,res_pay) as res_pay",
        "if(expire_premium  is null ,0,expire_premium) as expire_premium"
        //"cast(getNow() as timestamp) as create_time",
        //"cast(getNow() as timestamp) as update_time"
      )
      .map(x => {
        val channelId = x.getAs[String]("channel_id")
        val channelName = x.getAs[String]("channel_name")
        val insureCompanyName = x.getAs[String]("insure_company_name")
        val insureCompanyShortName = x.getAs[String]("insure_company_short_name")
        val skuChargeType = x.getAs[String]("sku_charge_type")
        val dayIdInsured = x.getAs[String]("day_id_insured")
        val dayId = if(dayIdInsured != null){
          dayIdInsured.substring(0,4)+"-"+dayIdInsured.substring(4,6)+"-"+dayIdInsured.substring(6,8)
        }else{
          null
        }
        val insuredCount = x.getAs[Int]("insured_count")
        val chargePremium = x.getAs[java.math.BigDecimal]("charge_premium")
        val accPremium = x.getAs[java.math.BigDecimal]("acc_premium")
        val caseNo = x.getAs[Int]("case_no")
        val endRiskPremium = x.getAs[java.math.BigDecimal]("end_risk_premium")
        val resPay = x.getAs[java.math.BigDecimal]("res_pay")
        val expirePremium = x.getAs[java.math.BigDecimal]("expire_premium")
        ((channelId,channelName,insureCompanyName,insureCompanyShortName,skuChargeType,dayId),
          (insuredCount,chargePremium,accPremium,caseNo,endRiskPremium,resPay,expirePremium))
      })
      .reduceByKey((x1,x2) =>{
        val insuredCount = x1._1+x2._1
        val chargePremium = x1._2.add(x2._2)
        val accPremium = x1._2.add(x2._2)
        val caseNo = x1._4+x2._4
        val endRiskPremium = x1._5.add(x2._5)
        val resPay = x1._6.add(x2._6)
        val expirePremium = x1._7.add(x2._7)
        (insuredCount,chargePremium,accPremium,caseNo,endRiskPremium,resPay,expirePremium)
      })
      .map(x => {
        (x._1._1,x._1._2,x._1._3,x._1._4,x._1._5,x._1._6,x._2._1,
          x._2._2,
          x._2._3,
          x._2._4,
          x._2._5,
          x._2._6,
          x._2._7
        )
      })
      .toDF(
        "channel_id",
        "channel_name",
        "insurance_company",
        "insurance_company_short_name",
        "sku_charge_type",
        "day_id",
        "curr_insured",
        "charge_premium",
        "acc_premium",
        "case_num",
        "settled_claim_premium",
        "prepare_claim_premium",
        "expire_premium"
      )
      .selectExpr(
        "getUUID() as id",
        "channel_id",
        "clean(channel_name) as channel_name",
        "insurance_company",
        "insurance_company_short_name",
        "sku_charge_type",
        "cast(day_id as Date) as day_id",
        "curr_insured",
        "cast (charge_premium as decimal(14,4)) as charge_premium",
        "cast (acc_premium as decimal(14,4)) as acc_premium",
        "case_num",
        "cast (settled_claim_premium as decimal(14,4)) as settled_claim_premium",
        "cast (prepare_claim_premium as decimal(14,4)) as prepare_claim_premium",
        "cast (expire_premium as decimal(14,4)) as expire_premium",
        "cast(getNow() as timestamp ) as create_time",
        "cast(getNow() as timestamp ) as update_time"
      )
      .where("channel_name is not null")
    
    res
  }

  /**
    * 得到满期保费数据
    * @param sqlContext 上下文
    */
  def getExpirePremiumData(sqlContext:HiveContext,dwPolicyEverydayPremiumDetail:DataFrame,dwEmployerBaseinfoDetail:DataFrame): DataFrame = {
    /**
      * 筛选出基础数据
      */
    val dwEmployerBaseinfoOne =
      dwEmployerBaseinfoDetail.selectExpr("policy_id_master","policy_code_master",
        "regexp_replace(substr(cast(policy_end_date as string),1,10),'-','') as expire_day_id")

    /**
      * 满期保费
      */
    dwEmployerBaseinfoOne.join(dwPolicyEverydayPremiumDetail,dwEmployerBaseinfoOne("policy_id_master")===dwPolicyEverydayPremiumDetail("policy_id_premium"))
      .selectExpr(
        "policy_id_master",
        "policy_code_master",
        "expire_day_id",
        "premium"
      ).registerTempTable("expirePremium")

    /**
      * 对保单号和满期时间进行分组得到满期保费数据
      */
    val res = sqlContext.sql("select policy_code_master,expire_day_id,sum(case when premium is null then 0 else premium end) as expire_premium " +
      "from expirePremium group by policy_code_master,expire_day_id")

    res
  }

  /**
    * 处理理赔相关的数据
    * @param sqlContext 上下文
    * @param dwPolicyClaimDetail 理赔数据
    */
  def getClaimData(sqlContext:HiveContext,dwPolicyClaimDetail:DataFrame): DataFrame = {
    import sqlContext.implicits._

    val res = dwPolicyClaimDetail.map(x => {
      //policy_id,policy_code,case_no,risk_date,case_status,res_pay
      val policyId = x.getAs[String]("policy_id")
      val policyCode = x.getAs[String]("policy_code")
      val riskDate = getFormatTime(getBeginTime(x.getAs[String]("risk_date").replaceAll("/", "-").concat(" 00:00:00"))).substring(0,10).replaceAll("-","")
      val caseStatus = x.getAs[String]("case_status")
      //已决预估赔付
      val endRiskPremium = if(caseStatus == "结案"){
        x.getAs[java.math.BigDecimal]("res_pay")
      }else{
        java.math.BigDecimal.valueOf(0.0)
      }
      //预估赔付
      val resPay = x.getAs[java.math.BigDecimal]("res_pay")
                                       //案件数,结案赔付，预估赔付
      ((policyId,policyCode,riskDate),(1,endRiskPremium,resPay))
    })
      .reduceByKey((x1,x2)=>{
        val caseNo = x1._1+x2._1
        val endRiskPremium = x1._2.add(x2._2)
        val resPay = x1._3.add(x2._3)
        (caseNo,endRiskPremium,resPay)
      })
      .map(x =>{
        (x._1._1,x._1._2,x._1._3,x._2._1,x._2._2,x._2._3)
      })
      .toDF("policy_id","policy_code","risk_date","case_no","end_risk_premium","res_pay")

    res
  }
}
