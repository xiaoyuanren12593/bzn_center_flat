package bzn.dw.premium

import java.text.SimpleDateFormat
import java.util.Date

import bzn.dw.premium.DwTypeOfWorkMatchingDetail.clean
import bzn.dw.util.SparkUtil
import bzn.job.common.Until
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.hive.HiveContext

/*
* @Author:liuxiang
* @Date：2019/9/17
* @Describe:工种匹配
*/
object DwTypeOfWorkMatchingTest extends SparkUtil with Until {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "local[*]")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    val res = DwTypeOfWorkMatching(hiveContext)
    res.printSchema()
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

    //读取保单明细表
    val odsPolicyDetailTemp: DataFrame = sqlContext.sql("select policy_id,policy_code,holder_name,insured_subject,product_code " +
      ",policy_status,sum_premium,insure_company_name from odsdb.ods_policy_detail")
      .where("policy_status in (1,0,-1)")

    //读取保险公司表,拿到保险公司简称
    val insuranceCompany = sqlContext.sql("select insurance_company,short_name from odsdb.ods_insurance_company_temp_dimension")

    //保单明细关联 保险公司表
    val odsPolicyDetail = odsPolicyDetailTemp.join(insuranceCompany, odsPolicyDetailTemp("insure_company_name") === insuranceCompany("insurance_company"), "leftouter")
      .selectExpr("policy_id", "policy_code",
       "sum_premium",
        "holder_name", "insured_subject","product_code", "insure_company_name", "short_name")


    //读取产品表
    val odsProductDetail: DataFrame = sqlContext.sql("select product_code as product_code_temp,product_name,one_level_pdt_cate from odsdb.ods_product_detail")

    //将明细表与产品表关联
    val ProductAndPolicy: DataFrame = odsPolicyDetail.join(odsProductDetail, odsPolicyDetail("product_code") === odsProductDetail("product_code_temp"), "leftouter")
      .selectExpr("policy_id", "product_code_temp as product_code", "policy_code", "product_name", "holder_name", "insured_subject","sum_premium","insure_company_name", "short_name","one_level_pdt_cate")

    //读取被保人明细表
    val odsPolicyInsuredDetail: DataFrame = sqlContext.sql(" select policy_id as id,insured_name,insured_cert_no,start_date,end_date,work_type,job_company,gender,age from odsdb.ods_policy_insured_detail")

    // 将上述结果与被保人表关联
    val policyInsure: DataFrame = ProductAndPolicy.join(odsPolicyInsuredDetail, ProductAndPolicy("policy_id") === odsPolicyInsuredDetail("id"), "leftouter")
      .selectExpr("policy_id", "policy_code", "product_code", "product_name", "holder_name", "insured_subject","sum_premium","insure_company_name", "short_name","insured_name",
        "insured_cert_no", "start_date","end_date","work_type", "job_company",
        "gender", "age", "one_level_pdt_cate")

    //读取企业联系人
    val odsEnterpriseDetail = sqlContext.sql("select ent_id,ent_name from odsdb.ods_enterprise_detail")

    //将上述结果与企业联系人关联
    val enterprise = policyInsure.join(odsEnterpriseDetail, policyInsure("holder_name") === odsEnterpriseDetail("ent_name"), "leftouter")
      .selectExpr("policy_id", "policy_code", "product_code", "product_name", "holder_name", "insured_subject","sum_premium",
        "insure_company_name", "short_name",
        "ent_name", "ent_id", "insured_name",
        "insured_cert_no","start_date","end_date", "work_type", "job_company","gender", "age", "one_level_pdt_cate")

    //读取客户归属销售表 拿到渠道id和名称
    val odsEntGuzhuDetail: DataFrame =
      sqlContext.sql("select ent_id as entid,ent_name as entname,channel_id, (case when channel_name='直客' then ent_name else channel_name end) as channel_name from odsdb.ods_ent_guzhu_salesman_detail")

    //将上述结果与客户归属销售表做关联
    val entAndGuzhuDetil = enterprise.join(odsEntGuzhuDetail, enterprise("ent_id") === odsEntGuzhuDetail("entid"), "leftouter")
      .selectExpr("policy_id", "policy_code", "product_code", "holder_name", "product_name",
        "channel_id", "channel_name", "insured_subject","sum_premium","insure_company_name", "short_name",
        "insured_name", "insured_cert_no","start_date","end_date",
        "work_type", "job_company", "gender", "age", "one_level_pdt_cate")

    //读取方案类别表
    val odsWorkGradeDimension: DataFrame = sqlContext.sql("select policy_code as policy_code_temp,profession_type from odsdb.ods_work_grade_dimension")
      .map(x => {
        val policyCodTemp = x.getAs[String]("policy_code_temp")
        val professionType = x.getAs[String]("profession_type")
        val result = if(professionType != null && professionType.length >0 ){
          val res = professionType.replaceAll("类","")
          if(res == "5"){
            (5,5)
          }else{
            val sp1 = res.split("-")(0).toInt
            val sp2 = res.split("-")(1).toInt
            (sp1,sp2)
          }
        }else{
          (-1,-1)
        }
        (policyCodTemp,professionType,result._1,result._2)
      })
      .toDF("policy_code_temp","profession_type","profession_type_slow","profession_type_high")

    //将上述结果与方案类别表关联
    val WorkGardeAndEnt = entAndGuzhuDetil.join(odsWorkGradeDimension, entAndGuzhuDetil("policy_code") === odsWorkGradeDimension("policy_code_temp"), "leftouter")
      .selectExpr("policy_id", "policy_code","holder_name",
        "product_code", "product_name", "profession_type", "channel_id", "channel_name",
        "insured_subject","sum_premium", "insure_company_name", "short_name","insured_name", "insured_cert_no",
        "start_date","end_date","work_type", "job_company",
        "gender", "age", "one_level_pdt_cate","profession_type_slow","profession_type_high")

    //读取bzn工种表
    val odsWorkMatching: DataFrame = sqlContext.sql("select primitive_work,work_name from odsdb.ods_work_matching_dimension")

    //将上述结果与bzn工种表关联
    val resAndOdsWorkMatch = WorkGardeAndEnt.join(odsWorkMatching, WorkGardeAndEnt("work_type") === odsWorkMatching("primitive_work"), "leftouter")
      .selectExpr("policy_id", "policy_code", "holder_name",
        "product_code", "product_name", "profession_type", "channel_id", "channel_name",
        "insured_subject","sum_premium", "insure_company_name", "short_name",
        "insured_name", "insured_cert_no","start_date","end_date", "work_type", "job_company",
        "primitive_work", "work_name",
        "gender", "age", "one_level_pdt_cate","profession_type_slow","profession_type_high")
      .where("one_level_pdt_cate = '蓝领外包' and product_code not in ('LGB000001','17000001')")
    // and product_code not in ('LGB000001','17000001')

    //读取方案信息表
    val odsPolicyProductPlanDetail: DataFrame = sqlContext.sql("select policy_code as policy_code_temp,sku_coverage,sku_append,sku_ratio,sku_price,sku_charge_type from odsdb.ods_policy_product_plan_detail")

     // 将上述结果与方案信息表关联
    val WorkAndPlan = resAndOdsWorkMatch.join(odsPolicyProductPlanDetail, resAndOdsWorkMatch("policy_code") === odsPolicyProductPlanDetail("policy_code_temp"))
      .selectExpr("policy_id", "policy_code", "holder_name",
        "product_code", "product_name", "profession_type", "channel_id", "channel_name",
        "insured_subject","sum_premium","insure_company_name", "short_name", "insured_name", "insured_cert_no","start_date",
        "end_date", "work_type", "job_company", "primitive_work", "work_name",
        "gender", "age", "one_level_pdt_cate","profession_type_slow",
        "profession_type_high", "sku_coverage", "sku_append", "sku_ratio", "sku_price", "sku_charge_type")

    //读取标准工种表 如果bzn_work_name 重复 拿最小的bzn_work_risk,取gs_work_risk的第一个,如果字段为空串,替换成-1

    val resTemp = sqlContext.sql("SELECT bzn_work_name,bzn_work_risk,gs_work_risk from odsdb.ods_work_risk_dimension")
      .map(x => {
        val bznWorkName = x.getAs[String]("bzn_work_name")
        val bznWorkRisk = x.getAs[String]("bzn_work_risk")
        val gsWrokRisk = x.getAs[String]("gs_work_risk")
        val res = if (gsWrokRisk == "" || gsWrokRisk.split(",")(0) == "S") -1 else {
          val restemp = gsWrokRisk.split(",")(0).toInt
          restemp
        }

        (bznWorkName, bznWorkRisk,res)

      }).toDF("bzn_work_name", "bzn_work_risk","gs_work_risk")

     resTemp.registerTempTable("ods_work_risk_dimension_Temp")

    val odsWorkRiskDimension = sqlContext.sql("select bzn_work_name as name, min(bzn_work_risk) as risk," +
      "min(gs_work_risk) as gs_work_risk from ods_work_risk_dimension_Temp group by bzn_work_name")

    //将上述结果与标准工种表关联
    var odsWorkMatch: DataFrame = WorkAndPlan.join(odsWorkRiskDimension,
      WorkAndPlan("work_name") === odsWorkRiskDimension("name"), "leftouter")
      .selectExpr("getUUID() as id", "policy_id ",
        "clean(policy_code) as policy_code",
        "sku_coverage",
        "clean(sku_append) as sku_append",
        "clean(sku_ratio) as sku_ratio",
        "case when product_code in ('LGB000001','17000001') then sum_premium else sku_price end as sku_price",
        "clean(sku_charge_type) as sku_charge_type",
        "clean(holder_name) as holder_name",
        "clean(product_code) as product_code",
        "clean(product_name) as product_name",
        "clean(profession_type) as profession_type",
        "clean(channel_id) as channel_id",
        "clean(channel_name) as channel_name ",
        "clean(insured_subject) as insured_subject ",
        "sum_premium",
        "clean(insure_company_name) as insure_company_name ", "clean(short_name) as short_name",
        "clean(insured_name) as insured_name",
        "clean(insured_cert_no) as insured_cert_no",
        "start_date","end_date",
        "clean(job_company) as job_company", //实际用工单位
        "gender",
        "age",
        "clean(work_type) as work_type",
        "clean(primitive_work) as primitive_work ", //原始工种
        "clean(name) as bzn_work_name ", //工种名称
        "clean(work_name) as work_name ", //标准工种
        "clean(risk) as bzn_work_risk", //级别
        "gs_work_risk",
        "case when work_type is null then '已匹配' when work_type is not null and primitive_work is not null then '已匹配'" +
          " when work_type is not null and primitive_work is null then '未匹配' end as recognition ",
        "case when work_type is not null and (work_name is not null and work_name !='未知') then 1 " +
          "when work_type is not null and work_name='未知' then 0 " +
          "when work_type is not null and work_name is null then 0  " +
          "when work_type is null then 2 end as whether_recognition",
        "case when profession_type is null then '未知' when cast(risk as int) >= profession_type_slow and  cast(risk as int) <= profession_type_high then '已知' else '未知' end as plan_recognition" ,//方案级别已匹配未匹配
        "case when profession_type is null then '未知' when  gs_work_risk >=profession_type_slow and gs_work_risk <= profession_type_high then '已知' else '未知' end as gs_plan_recognition"

      )

    val res = odsWorkMatch.selectExpr("id", "policy_id", "policy_code", "sku_coverage","sku_append","sku_ratio","sku_price","sku_charge_type",
      "holder_name",
      "product_code", "product_name", "profession_type", "channel_id", "channel_name",
      "insured_subject","sum_premium", "insure_company_name", "short_name",
      "insured_name", "insured_cert_no", "start_date","end_date","work_type","primitive_work","job_company", "gender", "age",
      "bzn_work_name","work_name","bzn_work_risk","cast(gs_work_risk as string) as gs_work_risk","recognition", "whether_recognition","plan_recognition","gs_plan_recognition")
    res

  }
}
