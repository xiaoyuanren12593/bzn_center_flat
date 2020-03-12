package bzn.dm.aegis

import java.sql.Timestamp
import java.text.NumberFormat

import bzn.dm.util.SparkUtil
import bzn.job.common.{DataBaseUtil, Until}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * author:xiaoYuanRen
  * Date:2020/2/25
  * Time:12:02
  * describe: 理赔数据分析因子  数据提取
  **/
object DmClaimDataAnalyzeDetail extends SparkUtil with DataBaseUtil with Until{
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    val res = getClaimDataAnalyzeData(hiveContext)
    val user103 = "mysql.username.103"
    val pass103 = "mysql.password.103"
    val url103 = "mysql_url.103.dmdb"
    val driver = "mysql.driver"
    val tableName1 = "dm_claim_data_analyze_detail"
    val user106 = "mysql.username.106"
    val pass106 = "mysql.password.106"
    val url106 = "mysql.url.106.dmdb"
    saveASMysqlTable(res: DataFrame, tableName1: String, SaveMode.Overwrite,user106:String,pass106:String,driver:String,url106:String)

    sc.stop()
  }

  /**
    * 获取数据
    * @param sqlContext 上下文
    */
  def getClaimDataAnalyzeData(sqlContext:HiveContext) = {

    val resOne = sqlContext.sql(
      """
        |select a.*,
        |b.is_medical,
        |b.medical_amount,
        |b.medical_percent,
        |b.is_delay,
        |b.delay_amount,
        |b.delay_percent,
        |b.delay_days,
        |b.is_hospital,
        |b.hospital_amount,
        |b.hospital_days,
        |b.hospital_total_days,
        |b.disability_scale,
        |b.society_num_scale,
        |b.compensate_scale,
        |b.extend_24hour,
        |b.extend_hospital,
        |b.extend_job_Injury,
        |b.extend_overseas,
        |b.extend_self_charge_medicine,
        |b.extend_three,
        |b.extend_three_item,
        |b.sales_model,
        |b.sales_model_percent,
        |b.min_price,
        |b.profession_type,
        |b.join_social,
        |b.is_commission_discount,
        |b.extend_new_person,
        |b.is_month_replace,
        |b.plan_type,
        |c.one_work_rate,
        |c.two_work_rate,
        |c.three_work_rate,
        |c.four_work_rate,
        |c.five_work_rate,
        |c.six_work_rate,
        |c.man_rate,
        |c.woman_rate,
        |c.sixteen_dan,
        |c.twenty_five_dan,
        |c.thirty_six_dan,
        |c.forty_five_dan,
        |c.fivty_five_dan,
        |c.over_fivty_dan,
        |d.new_and_old,
        |e.first_policy,
        |f.case_no,
        |f.res_pay,
        |g.curr_insured,
        |g.person_count,
        |h.charge_premium,
        |date_format(now(),'yyy-MM-dd HH:mm:ss') as dw_create_time
        |from
        |(
        |select policy_id,policy_code, holder_name,insured_subject,channel_name,policy_start_date,policy_end_date,consumer_category,belongs_industry_name,num_of_preson_first_policy,sku_charge_type,case when sku_charge_type = '2' then sku_price/12 else sku_price end as sku_price,sku_ratio,sku_append,sku_coverage,big_policy,sale_name,team_name,
        |holder_province,holder_city,substr(cast(policy_start_date as string),1,7) as holder_month
        |from dwdb.dw_employer_baseinfo_detail
        |where insure_company_short_name = '国寿财' and product_code not in ('LGB000001','17000001')
        |) a
        |left join
        |(
        |select
        |policy_code,dead_amount,
        |is_medical,
        |medical_amount,
        |medical_percent,
        |is_delay,
        |delay_amount,
        |delay_percent,
        |delay_days,
        |is_hospital,
        |hospital_amount,
        |hospital_days,
        |hospital_total_days,
        |disability_scale,
        |society_num_scale,
        |compensate_scale,
        |extend_24hour,
        |extend_hospital,
        |extend_job_Injury,
        |extend_overseas,
        |extend_self_charge_medicine,
        |extend_three,
        |extend_three_item,
        |sales_model,
        |sales_model_percent,
        |min_price,
        |profession_type,
        |join_social,
        |is_commission_discount,
        |extend_new_person,
        |is_month_replace,
        |plan_type from odsdb.ods_work_grade_detail
        |) b
        |on a.policy_code = b.policy_code
        |left join --年龄结构 性别结构 工种结构
        |(
        |select policy_code,
        |sum(case when gs_work_risk = '1' then 1 else 0 end)/sum(case when gs_work_risk is not null and cast(gs_work_risk as int) >=0 then 1 else 0 end) as one_work_rate,
        |sum(case when gs_work_risk = '2' then 1 else 0 end)/sum(case when gs_work_risk is not null and cast(gs_work_risk as int) >=0 then 1 else 0 end) as two_work_rate,
        |sum(case when gs_work_risk = '3' then 1 else 0 end)/sum(case when gs_work_risk is not null and cast(gs_work_risk as int) >=0 then 1 else 0 end) as three_work_rate,
        |sum(case when gs_work_risk = '4' then 1 else 0 end)/sum(case when gs_work_risk is not null and cast(gs_work_risk as int) >=0 then 1 else 0 end) as four_work_rate,
        |sum(case when gs_work_risk = '5' then 1 else 0 end)/sum(case when gs_work_risk is not null and cast(gs_work_risk as int) >=0 then 1 else 0 end) as five_work_rate,
        |sum(case when gs_work_risk = '6' or gs_work_risk = '0' then 1 else 0 end)/sum(case when gs_work_risk is not null and cast(gs_work_risk as int) >=0 then 1 else 0 end) as six_work_rate,
        |sum(case when gender = 1 then 1 else 0 end)/sum(case when gender in (0,1) then 1 else 0 end) as man_rate,
        |sum(case when gender = 0 then 1 else 0 end)/sum(case when gender in (0,1) then 1 else 0 end) as woman_rate,
        |--当年龄<12为儿童，当12<=年龄<18为少年，当18<=年龄<28为青年，壮年 28-45  45<=年龄<60为中年，当60<=年龄 为老年
        |sum(case when age > 0 and age <16  then 1 else 0 end)/sum(case when insured_cert_no is not null then 1 else 0 end) as sixteen_dan,
        |sum(case when age >=16 and age <25 then 1 else 0 end)/sum(case when insured_cert_no is not null then 1 else 0 end) as twenty_five_dan,
        |sum(case when age >=25 and age <36 then 1 else 0 end)/sum(case when insured_cert_no is not null then 1 else 0 end) as thirty_six_dan,
        |sum(case when age >=36 and age <45 then 1 else 0 end)/sum(case when insured_cert_no is not null then 1 else 0 end) as forty_five_dan,
        |sum(case when age >=45 and age <55 then 1 else 0 end)/sum(case when insured_cert_no is not null then 1 else 0 end) as fivty_five_dan,
        |sum(case when age >=55 then 1 else 0 end)/sum(case when insured_cert_no is not null then 1 else 0 end) as over_fivty_dan
        |from dwdb.dw_work_type_matching_claim_detail
        |group by policy_code
        |) c
        |on a.policy_code = c.policy_code
        |left join
        |(
        |select channel_name,case when min(policy_start_date) >= '2020-01-01' then '新客' else '老客' end new_and_old
        |from dwdb.dw_employer_baseinfo_detail
        |group by channel_name
        |) d
        |on a.channel_name = d.channel_name
        |left join
        |(
        |select z.policy_code,'1' as first_policy from (
        |select policy_code,holder_name,policy_start_date,row_number() over(partition by holder_name order by policy_start_date asc) as rand1
        |from dwdb.dw_employer_baseinfo_detail
        |) z
        |where z.rand1=1
        |) e
        |on a.policy_code = e.policy_code
        |left join
        |(
        |select policy_code,count(case_no) as case_no,sum(res_pay) as res_pay from dwdb.dw_policy_claim_detail
        |group by policy_code
        |) f
        |on a.policy_code = f.policy_code
        |left join
        |(
        |select policy_code,sum(case when day_id = regexp_replace(substr(cast(now() as string),1,10),'-','') then count else 0 end) as curr_insured,
        |sum(count) as person_count
        |from dwdb.dw_policy_curr_insured_detail
        |where day_id <= regexp_replace(substr(cast(now() as string),1,10),'-','')
        |group by policy_code
        |) g
        |on a.policy_code = g.policy_code
        |left join
        |(
        |select policy_id,sum(premium) as charge_premium from dwdb.dw_policy_everyday_premium_detail
        |where day_id <= regexp_replace(substr(cast(now() as string),1,10),'-','')
        |group by policy_id
        |) h
        |on a.policy_id = h.policy_id
      """.stripMargin)

    val replenishPremiumData = getNewChargePremiumData(sqlContext:HiveContext)

    val res = resOne.join(replenishPremiumData,resOne("policy_id") === replenishPremiumData("policy_id_slave"),"leftouter")
      .drop("policy_id_slave")

    res
  }

  /**
    * 获取新已赚保费的差值
    * @param sqlContext
    */
  def getNewChargePremiumData(sqlContext:HiveContext): DataFrame = {
    import sqlContext.implicits._

    val user106 = "mysql.username.106"
    val pass106 = "mysql.password.106"
    val url106 = "mysql.url.106.odsdb"
    val driver = "mysql.driver"
    val tableName1 = "ods_gs_plan_avg_price_dimension"
    /**
      *
      */
    //读取方案类别表
    val odsWorkGradeDimension: DataFrame = sqlContext.sql("select policy_code as policy_code_temp,profession_type from odsdb.ods_work_grade_detail")
      .map(x => {
        val policyCodTemp = x.getAs[String]("policy_code_temp")
        val professionType = x.getAs[String]("profession_type")
        val result = if (professionType != null && professionType.length > 0) {
          val res = professionType.replaceAll("类", "")
          if (res == "5") {
            (1, 5)
          } else if(res == "4"){
            (1, 4)
          } else if(res == "6"){
            (1, 6)
          } else {
            val sp1 = res.split("-")(0).toInt
            val sp2 = res.split("-")(1).toInt
            (sp1, sp2)
          }
        } else {
          (-1, -1)
        }
        (policyCodTemp, professionType, result._1, result._2)
      })
      .toDF("policy_code_temp", "profession_type", "profession_type_slow", "profession_type_high")

    val workGradeBaseinfo = sqlContext.sql(
      """
        |select a.policy_start_date,a.policy_end_date,a.holder_name,a.channel_name,
        |concat(gs_profession_type,cast(b.sku_coverage as string),b.sku_ratio) as reffer_plan,
        |b.*
        |from dwdb.dw_employer_baseinfo_detail a
        |left join
        |(
        |select  policy_code,policy_id,cast(sku_coverage as int) as sku_coverage,sku_charge_type,sku_ratio,sku_price,
        |case when gs_work_risk is null then '-100' else gs_work_risk end as gs_work_risk,start_date,end_date,insured_cert_no,
        |case when gs_work_risk = '1' then '1-1类'
        |     when gs_work_risk = '2' then '1-2类'
        |     when gs_work_risk = '3' then '1-3类'
        |     when gs_work_risk = '4' then '1-4类'
        |     when gs_work_risk = '5' then '5类'
        |     when gs_work_risk = '6' then '6类'
        |     else 'S类' end as gs_profession_type,
        |work_type
        |from dwdb.dw_work_type_matching_claim_detail
        |) b
        |on a.policy_code = b.policy_code
        |where a.product_code not in ('17000001','LGB000001') and a.insure_company_short_name = '国寿财'
      """.stripMargin)

    val gradeDataAndPlanData = workGradeBaseinfo.join(odsWorkGradeDimension,'policy_code_temp==='policy_code,"leftouter")
      .selectExpr("profession_type", "profession_type_slow", "profession_type_high",
        "policy_code",
        "policy_id",
        "sku_coverage",
        "sku_charge_type",
        "sku_price",
        "gs_work_risk",
        "start_date",
        "end_date",
        "insured_cert_no",
        "gs_profession_type",
        "policy_start_date",
        "policy_end_date",
        "holder_name",
        "channel_name",
        "reffer_plan"
      )

    /**
      * 读取均价表
      */
    val odsGsPlanAvgPriceDimension =  readMysqlTable(sqlContext: SQLContext, tableName1: String,user106:String,pass106:String,driver:String,url106:String)
      .selectExpr(
        "plan_name",
        "axb_avg_price",
        "concat(profession_type,cast(cast(sku_coverage as int) as string),sku_ratio) as plan"
      )

    val gradeDataAndPlanAndRefferData = gradeDataAndPlanData.join(odsGsPlanAvgPriceDimension,'reffer_plan==='plan,"leftouter")
      .selectExpr(
        "profession_type_slow",
        "profession_type_high",
        "policy_code",
        "policy_id",
        "sku_coverage",
        "sku_charge_type",
        "sku_price",
        //如果工种级别超过方案最高级别标注1
        "case when cast(gs_work_risk as int) > profession_type_high then 1 else 0 end as reffer_gs_work_risk",
        "start_date as insured_start_date",
        "end_date as insured_end_date",
        "insured_cert_no",
        "gs_profession_type",
        "policy_start_date",
        "policy_end_date",
        "holder_name",
        "channel_name",
        "reffer_plan",
        "case when sku_charge_type = '2' then axb_avg_price*12 else axb_avg_price end as axb_avg_price"
      )
      .selectExpr(
        "profession_type_slow",
        "profession_type_high",
        "policy_code",
        "policy_id",
        "sku_coverage",
        "sku_charge_type",
        //如果工种级别超过方案最高级别标注1
        "reffer_gs_work_risk",
        "insured_start_date",
        "insured_end_date",
        "insured_cert_no",
        "gs_profession_type",
        "policy_start_date",
        "policy_end_date",
        "holder_name",
        "channel_name",
        "reffer_plan",
        "axb_avg_price",
        "case when reffer_gs_work_risk = 1 and axb_avg_price>sku_price then axb_avg_price-sku_price else 0 end as sku_price"
      )

    val yearData = yearPremium(sqlContext: HiveContext,gradeDataAndPlanAndRefferData:DataFrame)
    val monthData = monthPremium(sqlContext: HiveContext,gradeDataAndPlanAndRefferData:DataFrame)
    yearData.unionAll(monthData)
      .selectExpr(
        "policy_id","sku_day_price",
        "day_id"
      ).registerTempTable("temp_sku_price")

    val replenishPremiumData = sqlContext.sql("select policy_id as policy_id_slave,cast(sum(cast sku_day_price is null then 0 else sku_day_price end) as decimal(14,10)) as replenish_premium from temp_sku_price where day_id <= regexp_replace(subStr(cast(now() as string),1,10),'-','') group by policy_id")

    replenishPremiumData
  }

  /**
    * 年单保费计算方式
    * @param date //
    */
  def yearPremium(sqlContext: HiveContext,date:DataFrame): DataFrame = {
    import sqlContext.implicits._
    val res = date.where("sku_charge_type = '2'").mapPartitions(rdd => {
      // 创建一个数值格式化对象(对数字)
      val numberFormat = NumberFormat.getInstance
      // 设置精确到小数点后2位
      numberFormat.setMaximumFractionDigits(4)

      rdd.flatMap(x => {
        val holderName = x.getAs[String]("holder_name")

        val policyId = x.getAs[String]("policy_id")
        val skuPrice = x.getAs[java.math.BigDecimal]("sku_price")
        val insuredCertNo = x.getAs[String]("insured_cert_no")

        val startDate = x.getAs[Timestamp]("policy_start_date").toString.split(" ")(0).replaceAll("-", "").replaceAll("/", "")
        val endDate = x.getAs[Timestamp]("policy_end_date").toString.split(" ")(0).replaceAll("-", "").replaceAll("/", "")

        /**
          * 被保人的止期时间不得大于保单的止期时间
          */
        val insuredStartDateRes = if(x.getAs[Timestamp]("policy_start_date") != null){
          if(x.getAs[Timestamp]("policy_start_date").compareTo(x.getAs[Timestamp]("insured_start_date")) <= 0){
            x.getAs[Timestamp]("insured_start_date")
          }else{
            x.getAs[Timestamp]("policy_start_date")
          }
        }else{
          x.getAs[Timestamp]("insured_start_date")
        }

        val insuredEndDateRes = if(x.getAs[Timestamp]("policy_end_date") != null){
          if(x.getAs[Timestamp]("policy_end_date").compareTo(x.getAs[Timestamp]("insured_end_date")) >= 0){
            x.getAs[Timestamp]("insured_end_date")
          }else{
            x.getAs[Timestamp]("policy_end_date")
          }
        }else{
          x.getAs[Timestamp]("insured_end_date")
        }

        val insuredStartDate = insuredStartDateRes.toString.split(" ")(0).replaceAll("-", "").replaceAll("/", "")
        val insuredEndDate = insuredEndDateRes.toString.split(" ")(0).replaceAll("-", "").replaceAll("/", "")

        // sku_charge_type:1是月单，2是年单子
        //判断是年单还是月单
        //如果是月单，则计算的是我当月的平均保费使用的字段是:insured_start_date,insured_end_date
        //如果是年单，则计算的是我当年的平均保费使用的字段是:start_date,end_date

        //保单层面的循环天数
        val dateNumber = getBeg_End_one_two(startDate, endDate).size

        val res = getBeg_End_one_two(insuredStartDate, insuredEndDate).map(day_id => {
          val num = java.math.BigDecimal.valueOf(dateNumber)
          val skuDayPrice: java.math.BigDecimal = if(num.compareTo(java.math.BigDecimal.valueOf(0)) != 0){
            skuPrice.divide(num,4)
          }else{
            java.math.BigDecimal.valueOf(0.0)
          }
          (policyId,skuDayPrice,insuredCertNo,insuredStartDateRes,insuredEndDateRes,day_id,skuPrice,holderName)
        })
        res
      })
    }).toDF("policy_id","sku_day_price","insured_cert_no","insured_start_date","insured_end_date",
      "day_id","sku_price","holder_name")
    res
  }

  /**
    * 月单保费计算方式
    * @param date //
    */
  def monthPremium(sqlContext: HiveContext,date:DataFrame): DataFrame = {
    import sqlContext.implicits._
    val res = date.where("sku_charge_type = '1'").mapPartitions(rdd => {
      // 创建一个数值格式化对象(对数字)
      val numberFormat = NumberFormat.getInstance
      // 设置精确到小数点后2位
      numberFormat.setMaximumFractionDigits(4)

      rdd.flatMap(x => {
        val holderName = x.getAs[String]("holder_name")

        val policyId = x.getAs[String]("policy_id")
        val skuPrice = x.getAs[java.math.BigDecimal]("sku_price")
        val insuredCertNo = x.getAs[String]("insured_cert_no")

        /**
          * 被保人的止期时间不得大于保单的止期时间
          */
        val insuredStartDateRes = if(x.getAs[Timestamp]("policy_start_date") != null){
          if(x.getAs[Timestamp]("policy_start_date").compareTo(x.getAs[Timestamp]("insured_start_date")) <= 0){
            x.getAs[Timestamp]("insured_start_date")
          }else{
            x.getAs[Timestamp]("policy_start_date")
          }
        }else{
          x.getAs[Timestamp]("insured_start_date")
        }

        val insuredEndDateRes = if(x.getAs[Timestamp]("policy_end_date") != null){
          if(x.getAs[Timestamp]("policy_end_date").compareTo(x.getAs[Timestamp]("insured_end_date")) >= 0){
            x.getAs[Timestamp]("insured_end_date")
          }else{
            x.getAs[Timestamp]("policy_end_date")
          }
        }else{
          x.getAs[Timestamp]("insured_end_date")
        }

        val insuredStartDate = insuredStartDateRes.toString.split(" ")(0).replaceAll("-", "").replaceAll("/", "")
        val insuredEndDate = insuredEndDateRes.toString.split(" ")(0).replaceAll("-", "").replaceAll("/", "")

        // sku_charge_type:1是月单，2是年单子
        //判断是年单还是月单
        //如果是月单，则计算的是我当月的平均保费使用的字段是:insured_start_date,insured_end_date
        //如果是年单，则计算的是我当年的平均保费使用的字段是:start_date,end_date

        //保单层面的循环天数
        val dateNumber = getBeg_End_one_two(insuredStartDate, insuredEndDate).size

        val res =
          getBeg_End_one_two(insuredStartDate, insuredEndDate).map(day_id => {
            val num = java.math.BigDecimal.valueOf(dateNumber)
            val skuDayPrice: java.math.BigDecimal = if(num.compareTo(java.math.BigDecimal.valueOf(0)) != 0){
              skuPrice.divide(num,4)
            }else{
              java.math.BigDecimal.valueOf(0)
            }
            (policyId,skuDayPrice,insuredCertNo,insuredStartDateRes,insuredEndDateRes,day_id,skuPrice,holderName)
          })

        res
      })
    }).toDF("policy_id","sku_day_price","insured_cert_no","insured_end_date","insure_policy_status",
      "day_id","sku_price","holder_name")
    res
  }
}
