package bzn.ods.policy

import java.text.SimpleDateFormat
import java.util.Date

import bzn.job.common.{DataBaseUtil, Until}
import bzn.ods.util.SparkUtil
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * author:xiaoYuanRen
  * Date:2020/1/8
  * Time:14:41
  * describe: 工种类别明细表
  **/
object OdsWorkGradeDetail extends SparkUtil with Until with DataBaseUtil{
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName,"")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4

    /**
      * 抽取投保单表
      * 抽取方案表
      * 抽取bzn_business.dict_zh_plan
      * 抽取bzn_business.dict_china_life_plan
      * 抽取bzn_business.dict_china_life_plan_group
      */
    val res = getOdsWorkGradeInfo(hiveContext)
    hiveContext.sql("truncate table odsdb.ods_work_grade_detail")
    res.repartition(10).write.mode(SaveMode.Append).saveAsTable("odsdb.ods_work_grade_detail")
    sc.stop()
  }

  /**
    * 得到gsc和zh和泰康的方案信息
    * @param sqlContext sxw
    */
  def getOdsWorkGradeInfo(sqlContext:HiveContext): DataFrame = {
    import sqlContext.implicits._
    sqlContext.udf.register("clean", (str: String) => clean(str))
    sqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    sqlContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")//设置日期格式
      val date = df.format(new Date())// new Date()为获取当前系统时间
      date + ""
    })
    sqlContext.udf.register("getNull", (line:String) =>  {
      if (line == "" || line == null || line == "NULL") 9 else line.toInt
    })

    val urlFormatOfficial = "mysql.url.106"
    val userFormatOfficial = "mysql.username.106"
    val possWordFormatOfficial = "mysql.password.106"

    val driverFormat = "mysql.driver"
    val tableTProposalName = "t_proposal_bznbusi"
    val tableTProposalProductPlanName = "t_proposal_product_plan_bznbusi"
    val tableDictZhPlanBznbusiName = "dict_zh_plan_bznbusi"
    val tableDictChinaLifePlanBznbusiName = "dict_china_life_plan_bznbusi"
    val tableDictChinaLifePlanGroupBznbusiName = "dict_china_life_plan_group_bznbusi"
    val tablebPolicyzncenName = "b_policy_bzncen"
    val tablebPolicyProductPlanBzncenName = "b_policy_product_plan_bzncen"
    val tableDictChinalifeWybPlanBznbusiName = "dict_chinalife_wyb_plan_bznbusi"

    /**
      * 核心保单表
      */
    val policyzncen = readMysqlTable(sqlContext: SQLContext, tablebPolicyzncenName: String,userFormatOfficial:String,possWordFormatOfficial:String,driverFormat:String,urlFormatOfficial:String)
      .selectExpr("insurance_policy_no","case when proposal_no is null then getUUID() else proposal_no end as proposal_no","policy_no","product_code",
        "status as proposal_status","insurance_name","profession_type as profession_type_master")

    /**
      * 核心方案表
      */
    val policyProductPlanBzncen = readMysqlTable(sqlContext: SQLContext, tablebPolicyProductPlanBzncenName: String,userFormatOfficial:String,possWordFormatOfficial:String,driverFormat:String,urlFormatOfficial:String)
      .selectExpr("policy_no as policy_no_slave","plan_name","SUBSTRING_INDEX(plan_detail_code,',',-1) as plan_detail_code")

    val policyAndPlan = policyzncen.join(policyProductPlanBzncen,'policy_no==='policy_no_slave,"leftouter")
      .drop("policy_no_slave")

    /**
      * 投保单表
      */
    val TProposalBznbusi = readMysqlTable(sqlContext: SQLContext, tableTProposalName: String,userFormatOfficial:String,possWordFormatOfficial:String,driverFormat:String,urlFormatOfficial:String)
      .selectExpr("insurance_policy_no","proposal_no","policy_no","product_code","status as proposal_status","insurance_name","profession_type as profession_type_master")

    /**
      * 方案表
      */
    val TProposalProductPlanBznbusi = readMysqlTable(sqlContext: SQLContext, tableTProposalProductPlanName: String,userFormatOfficial:String,possWordFormatOfficial:String,driverFormat:String,urlFormatOfficial:String)
      .selectExpr("proposal_no as proposal_no_slave","plan_name","SUBSTRING_INDEX(plan_detail_code,',',-1) as plan_detail_code")

    /**
      * 中华的方案配置表
      */
    val tableDictZhPlanBznbusi = readMysqlTable(sqlContext: SQLContext, tableDictZhPlanBznbusiName: String,userFormatOfficial:String,possWordFormatOfficial:String,driverFormat:String,urlFormatOfficial:String)
      .withColumnRenamed("plan_code","plan_code_salve")
      .drop("plan_code")

    /**
      * 国寿财的方案表
      */
    val tableDictChinaLifePlanBznbusi = readMysqlTable(sqlContext: SQLContext, tableDictChinaLifePlanBznbusiName: String,userFormatOfficial:String,possWordFormatOfficial:String,driverFormat:String,urlFormatOfficial:String)
      .selectExpr("group_code","plan_code as plan_code_salve")

    /**
      * 国寿财的方案表2
      */
    val tabledictChinaLifePlanGroupBznbusi = readMysqlTable(sqlContext: SQLContext, tableDictChinaLifePlanGroupBznbusiName: String,userFormatOfficial:String,possWordFormatOfficial:String,driverFormat:String,urlFormatOfficial:String)
      .withColumnRenamed("group_code","group_code_slave")

    /**
      * 读取无忧保产品的方案
      * K1:1-2类 K2:1-3类 K3:1-4类 K4:1-5类 K5:1-6类'
      */
    val dictChinalifeWybPlanBznbusi = readMysqlTable(sqlContext: SQLContext, tableDictChinalifeWybPlanBznbusiName: String,userFormatOfficial:String,possWordFormatOfficial:String,driverFormat:String,urlFormatOfficial:String)
      .withColumnRenamed("plan_code","plan_code_salve")

    /**
      * 投保单表和方案表
      */
    val proposalAndPlan = TProposalBznbusi.join(TProposalProductPlanBznbusi,'proposal_no==='proposal_no_slave,"leftouter")
      .drop("proposal_no_slave")

    val proposalAndPolicy = proposalAndPlan.unionAll(policyAndPlan)
      .distinct()
      .map(x => {
        val insurancePolicyNo = x.getAs[String]("insurance_policy_no")
        val proposalNo = x.getAs[String]("proposal_no")
        val policyNo = x.getAs[String]("policy_no")
        val proposalStatus = x.getAs[Int]("proposal_status")
        val insuranceName = x.getAs[String]("insurance_name")
        val professionTypeMaster = x.getAs[String]("profession_type_master")
        val planName = x.getAs[String]("plan_name")
        val productCode = x.getAs[String]("product_code")
        val planDetailCode = x.getAs[String]("plan_detail_code")
        (proposalNo,(insurancePolicyNo,policyNo,proposalStatus,insuranceName,professionTypeMaster,planName,productCode,planDetailCode))
      })
      .reduceByKey((x1,x2) => {
        val insurancePolicyNo = if(x1._1 != null)  x1._1 else x2._1
        val policyNo = if(x1._2 != null)  x1._2 else x2._2
        val proposalStatus = if(x1._3 != null)  x1._3 else x2._3
        val insuranceName = if(x1._4 != null)  x1._4 else x2._4
        val professionTypeMaster = if(x1._5 != null)  x1._5 else x2._5
        val planName = if(x1._6 != null)  x1._6 else x2._6
        val productCode = if(x1._7 != null)  x1._7 else x2._7
        val planDetailCode = if(x1._8 != null)  x1._8 else x2._8
        (insurancePolicyNo,policyNo,proposalStatus,insuranceName,professionTypeMaster,planName,productCode,planDetailCode)
      })
      .map(x => {
        (x._2._1,x._1,x._2._2,x._2._3,x._2._4,x._2._5,x._2._6,x._2._7,x._2._8)
      })
      .toDF("insurance_policy_no","proposal_no","policy_no","proposal_status","insurance_name","profession_type_master","plan_name","product_code","plan_detail_code")

    /**
      * 无忧保方案数据
      */
    val wybData = proposalAndPolicy.join(dictChinalifeWybPlanBznbusi,'plan_detail_code==='plan_code_salve)
      .where("product_code = 'P00001800' and profession_type is not null")
      .selectExpr(
        "insurance_policy_no as policy_code","proposal_no","policy_no","proposal_status","insurance_name","plan_name",
        "policy_category as deadline_type",
        "service_charge",
        "dead_amount",
        "'' as is_medical",
        "medical_amount",
        "medical_percent",
        "'' as is_delay",
        "delay_amount",
        "delay_percent",
        "delay_days",
        "'' as is_hospital",
        "hospital_amount",
        "hospital_days",
        "hospital_total_days",
        "disability_scale",
        //        "is_social_insurance as society_scale",
        "min_num as society_num_scale",
        "'' as compensate_scale",
        "extend_24hour",
        "extend_hospital",
        "'' as extend_job_Injury",
        "extend_overseas",
        "extend_self_charge_medicine",
        "'' as extend_three",
        "extend_three_item",
        "sales_model",
        "sales_model_percent",
        "commission_percent",
        "min_price",
        "premium",
        "status",
        "create_time",
        "create_user_id",
        "create_user_name",
        "update_time",
        "case profession_type when 'K1' then '1-2类' when 'K2' then '1-3类' when 'K3' then '1-4类' when 'K4' then '1-5类' when 'K5' then '1-6类' else null end as profession_type",
        "is_social_insurance as join_social",
        "is_commission_discount",
        "extend_new_person",
        "is_month_replace",
        "plan_type"
      )

    /**
      * 上述数据和国寿财1关联
      */
    val proposalAndPlanAndGsc1 = proposalAndPolicy.join(tableDictChinaLifePlanBznbusi,'plan_detail_code==='plan_code_salve,"leftouter")
      .where("plan_code_salve is not null")

    /**
      * 上述数据和国寿财2关联
      */
    val gscDate = proposalAndPlanAndGsc1.join(tabledictChinaLifePlanGroupBznbusi,'group_code==='group_code_slave,"leftouter")
      .where("profession_type is not null or join_social is not null")
      .selectExpr(
        "insurance_policy_no as policy_code","proposal_no","policy_no","proposal_status","insurance_name","plan_name",
        "policy_category as deadline_type",
        "'' as service_charge",
        "amount as dead_amount",
        "'' as is_medical",
        "'' as medical_amount",
        "'' as medical_percent",
        "'' as is_delay",
        "'' as delay_amount",
        "'' as delay_percent",
        "'' as delay_days",
        "'' as is_hospital",
        "'' as hospital_amount",
        "'' as hospital_days",
        "'' as hospital_total_days",
        "compensate_scale as disability_scale",
        //        "'' as society_scale",
        "insure_num_type as society_num_scale",
        "'' as compensate_scale",
        "'' as extend_24hour",
        "'' as extend_hospital",
        "'' as extend_job_Injury",
        "'' as extend_overseas",
        "'' as extend_self_charge_medicine",
        "'' as extend_three",
        "'' as extend_three_item",
        "'' as sales_model",
        "'' as sales_model_percent",
        "commission_percent as commission_percent",
        "cast('' as decimal(14,4)) as min_price",
        "cast('' as decimal(14,4)) as premium",
        "status",
        "create_time",
        "create_user_id",
        "create_user_name",
        "update_time",
        "case profession_type when '1' then '1-3类' when '2' then '1-4类' when '3' then '5类' else '未知' end as profession_type",
        "case join_social when '1' then 'Y' when '0' then 'N' else null end as join_social",
        "is_commission as is_commission_discount",
        "'' as extend_new_person",
        "'' as is_month_replace",
        "type as plan_type"
      )

    /**
      * 泰康団意
      */
    val tkData = proposalAndPolicy.where("product_code = 'P00001728'")
      .selectExpr(
        "insurance_policy_no as policy_code","proposal_no","policy_no","proposal_status","insurance_name","plan_name",
        "'' as deadline_type",
        "'' as service_charge",
        "'' as dead_amount",
        "'' as is_medical",
        "'' as medical_amount",
        "'' as medical_percent",
        "'' as is_delay",
        "'' as delay_amount",
        "'' as delay_percent",
        "'' as delay_days",
        "'' as is_hospital",
        "'' as hospital_amount",
        "'' as hospital_days",
        "'' as hospital_total_days",
        "'' as disability_scale",
        //        "'' as society_scale",
        "'' as society_num_scale",
        "'' as compensate_scale",
        "'' as extend_24hour",
        "'' as extend_hospital",
        "'' as extend_job_Injury",
        "'' as extend_overseas",
        "'' as extend_self_charge_medicine",
        "'' as extend_three",
        "'' as extend_three_item",
        "'' as sales_model",
        "'' as sales_model_percent",
        "cast('' as decimal(14,4)) as commission_percent",
        "cast('' as decimal(14,4)) as min_price",
        "cast('' as decimal(14,4)) as premium",
        "cast('' as int) as status",
        "cast('' as timestamp) as create_time",
        "'' as create_user_id",
        "'' as create_user_name",
        "cast('' as timestamp) as update_time",
        "profession_type_master as profession_type",
        "'' as join_social",
        "'' is_commission_discount",
        "'' as extend_new_person",
        "'' as is_month_replace",
        "'' as plan_type"
      )

    /**
      * 投保单和方案数据与中华数据关联
      */
    proposalAndPolicy.join(tableDictZhPlanBznbusi,'plan_detail_code==='plan_code_salve,"leftouter")
      .where("profession_type is NOT NULL or society_scale is NOT NULL")
      .drop("id")
      .drop("product_code")
      .drop("profession_type_master")
      .drop("group_code")
      .drop("center_plan_code")
      .registerTempTable("zhDataTemp")

    val zhData = sqlContext.sql(
      """
        |select *,case profession_type when 'K1' then '1-2类'
        |when 'K2' then '1-3类' when 'K3' then '1-4类' when 'K4' then '5类' else null end as profession_type_new,society_scale as join_social,
        |'' as is_commission_discount,'' as extend_new_person,'' as is_month_replace, '' as plan_type
        |from zhDataTemp
      """.stripMargin)
      .drop("profession_type")
      .drop("society_scale")
      .withColumnRenamed("profession_type_new","profession_type")
      .withColumnRenamed("insurance_policy_no","policy_code")
      .drop("plan_detail_code")
      .drop("plan_code_salve")

    val res = zhData.unionAll(gscDate).unionAll(tkData).unionAll(wybData)
      .selectExpr(
        "getUUID() as id",
        "clean(policy_code) as policy_code","proposal_no","policy_no","proposal_status","plan_name","insurance_name",
        "clean(deadline_type) as deadline_type",
        "clean(service_charge) as service_charge",
        "clean(dead_amount) as dead_amount",
        "clean(is_medical) as is_medical",
        "clean(medical_amount) as medical_amount",
        "clean(medical_percent) as medical_percent",
        "clean(is_delay) as is_delay",
        "clean(delay_amount) as delay_amount",
        "clean(delay_percent) as delay_percent",
        "clean(delay_days) as delay_days",
        "clean(is_hospital) as is_hospital",
        "clean(hospital_amount) as hospital_amount",
        "clean(hospital_days) as hospital_days",
        "clean(hospital_total_days) as hospital_total_days",
        "clean(disability_scale) as disability_scale",
        "clean(society_num_scale) as society_num_scale",
        "clean(compensate_scale) as compensate_scale",
        "clean(extend_24hour) as extend_24hour",
        "clean(extend_hospital) as extend_hospital",
        "clean(extend_job_Injury) as extend_job_Injury",
        "clean(extend_overseas) as extend_overseas",
        "clean(extend_self_charge_medicine) as extend_self_charge_medicine",
        "clean(extend_three) as extend_three",
        "clean(extend_three_item) as extend_three_item",
        "clean(sales_model) as sales_model",
        "clean(sales_model_percent) as sales_model_percent",
        "commission_percent",
        "min_price",
        "premium",
        "status",
        "create_time",
        "clean(create_user_id) as create_user_id",
        "clean(create_user_name) as create_user_name",
        "update_time",
        "profession_type",
        "clean(join_social) as join_social",
        "clean(is_commission_discount) as is_commission_discount",
        "clean(extend_new_person) as extend_new_person",
        "clean(is_month_replace) as is_month_replace",
        "clean(plan_type) as plan_type",
        "getNow() as dw_create_time"
      )

    res
  }
}
