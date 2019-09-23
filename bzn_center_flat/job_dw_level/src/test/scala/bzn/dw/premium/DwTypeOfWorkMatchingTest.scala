package bzn.dw.premium

import java.text.SimpleDateFormat
import java.util.Date

import bzn.dw.util.SparkUtil
import bzn.job.common.Until
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.hive.HiveContext

/*
* @Author:liuxiang
* @Date：2019/9/17
* @Describe:工种匹配
*/ object DwTypeOfWorkMatchingTest extends SparkUtil with Until {
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
    val odsPolicyDetail: DataFrame = sqlContext.sql("select policy_id,policy_code,holder_name,insured_subject,product_code " +
      ",policy_status from odsdb.ods_policy_detail")
      .where("policy_status in (1,0,-1)")

    //读取产品表
    val odsProductDetail = sqlContext.sql("select product_code as product_code_temp,product_name,one_level_pdt_cate from odsdb.ods_product_detail")

    //将明细表与产品表关联
    val ProductAndPolicy: DataFrame = odsPolicyDetail.join(odsProductDetail, odsPolicyDetail("product_code") === odsProductDetail("product_code_temp"), "leftouter")
      .selectExpr("policy_id", "product_code_temp as product_code", "policy_code", "product_name", "holder_name", "insured_subject", "one_level_pdt_cate")

    //读取被保人明细表
    val odsPolicyInsuredDetail: DataFrame = sqlContext.sql(" select policy_id as id,insured_name,insured_cert_no,work_type,job_company,gender,age from odsdb.ods_policy_insured_detail")

    // 将上述结果与被保人表关联
    val policyInsure: DataFrame = ProductAndPolicy.join(odsPolicyInsuredDetail, ProductAndPolicy("policy_id") === odsPolicyInsuredDetail("id"), "leftouter")
      .selectExpr("policy_id", "policy_code", "product_code", "product_name", "holder_name", "insured_subject", "insured_name", "insured_cert_no", "work_type", "job_company",
        "gender", "age", "one_level_pdt_cate")
      .where("one_level_pdt_cate = '蓝领外包' and product_code not in ('LGB000001','17000001')")

    //读取企业联系人
    val odsEnterpriseDetail = sqlContext.sql("select ent_id,ent_name from odsdb.ods_enterprise_detail")

    //将上述结果与企业联系人关联
    val Enterprise = policyInsure.join(odsEnterpriseDetail, policyInsure("holder_name") === odsEnterpriseDetail("ent_name"), "leftouter")
      .selectExpr("policy_id", "policy_code", "product_code", "product_name", "holder_name", "insured_subject", "ent_name", "ent_id", "insured_name", "insured_cert_no", "work_type", "job_company",
        "gender", "age", "one_level_pdt_cate")

    //读取客户归属销售表 拿到渠道id和名称
    val odsEntGuzhuDetail: DataFrame =
      sqlContext.sql("select ent_id as entid,ent_name as entname,channel_id, (case when channel_name='直客' then ent_name else channel_name end) as channel_name from odsdb.ods_ent_guzhu_salesman_detail")

    //将上述结果与客户归属销售表做关联
    val entAndGuzhuDetil = Enterprise.join(odsEntGuzhuDetail, Enterprise("ent_id") === odsEntGuzhuDetail("entid"), "leftouter")
      .selectExpr("policy_id", "policy_code", "product_code", "holder_name", "product_name", "channel_id", "channel_name", "insured_subject", "insured_name", "insured_cert_no", "work_type", "job_company",
        "gender", "age", "one_level_pdt_cate")

    //读取方案类别表
    val odsWorkGradeDimension: DataFrame = sqlContext.sql("select policy_code as policy_code_temp,profession_type from odsdb.ods_work_grade_dimension")

    //将上述结果与方案类别表关联
    val WorkGardeAndEnt = entAndGuzhuDetil.join(odsWorkGradeDimension, entAndGuzhuDetil("policy_code") === odsWorkGradeDimension("policy_code_temp"), "leftouter")
      .selectExpr("policy_id", "policy_code", "holder_name",
        "product_code", "product_name", "profession_type", "channel_id", "channel_name",
        "insured_subject", "insured_name", "insured_cert_no", "work_type", "job_company",
        "gender", "age", "one_level_pdt_cate")
    //读取bzn工种表
    val odsWorkMatching: DataFrame = sqlContext.sql("select primitive_work,work_name from odsdb.ods_work_matching_dimension")

    //将上述结果与bzn工种表关联
    val resAndOdsWorkMatch = WorkGardeAndEnt.join(odsWorkMatching, WorkGardeAndEnt("work_type") === odsWorkMatching("primitive_work"), "leftouter")
      .selectExpr("policy_id", "policy_code", "holder_name",
        "product_code", "product_name", "profession_type", "channel_id", "channel_name",
        "insured_subject", "insured_name", "insured_cert_no", "work_type", "job_company", "primitive_work", "work_name",
        "gender", "age", "one_level_pdt_cate")

    //读取标准工种表
    val odsWorkRiskDimension: DataFrame =
      sqlContext.sql("SELECT bzn_work_name as name,min(bzn_work_risk) as risk  from odsdb.ods_work_risk_dimension GROUP BY bzn_work_name")

    //将上述结果与标准工种表关联
    var odsWorkMatch: DataFrame = resAndOdsWorkMatch.join(odsWorkRiskDimension, resAndOdsWorkMatch("work_name") === odsWorkRiskDimension("name"), "leftouter")
      .distinct()
      .selectExpr("getUUID() as id", "policy_id ",
        "clean(policy_code) as policy_code",
        "clean(holder_name) as holder_name",
        "clean(product_code) as product_code",
        "clean(product_name) as product_name",
        "clean(profession_type) as profession_type",
        "clean(channel_id) as channel_id",
        "clean(channel_name) as channel_name ",
        "clean(insured_subject) as insured_subject ",
        "clean(insured_name) as insured_name",
        "clean(insured_cert_no) as insured_cert_no",
        "clean(job_company) as job_company", //实际用工单位
        "clean(work_type) as work_type",
        "gender",
        "age",
        "clean(primitive_work) as primitive_work ", //原始工种
        "clean(name) as bzn_work_name ", //工种名称
        "clean(work_name) as work_name ", //标准工种
        "clean(risk) as bzn_work_risk", //级别
        "case when work_type is not null and (work_name is not null and work_name !='未知') then 1 " +
          "when work_type is not null and work_name='未知' then 0 " +
          "when work_type is not null and work_name is null then 0  " +
          "when work_type is null then 2 end as whether_recognition")

    val res = odsWorkMatch.selectExpr("id", "policy_id", "policy_code", "holder_name",
      "product_code", "product_name", "profession_type", "channel_id", "channel_name",
      "insured_subject", "insured_name", "insured_cert_no", "work_type", "job_company", "gender", "age",
      "work_name", "bzn_work_name", "bzn_work_risk", "whether_recognition"
    )
    res
  }

}