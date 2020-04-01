package bzn.dw.premium

import bzn.dw.util.SparkUtil
import bzn.job.common.{MysqlUntil, Until}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext

object DwRiskManagementOutputDeatil extends SparkUtil with Until with MysqlUntil {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName: String = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "")

    val sc: SparkContext = sparkConf._2
    val sqlContext = sparkConf._3
    val hiveContext: HiveContext = sparkConf._4
    val res = RiskManagementOutput(hiveContext)
    saveASMysqlTable(res, "dm_risk_management_input_detail",
      SaveMode.Overwrite, "mysql.username.103",
      "mysql.password.103", "mysql.driver", "mysql.url.103.dmdb")

    sc.stop()
  }


  /**
   *
   * @param hiveContext
   */

  def RiskManagementOutput(hiveContext: HiveContext): DataFrame = {
    import hiveContext.implicits._
    //读取保单表
    val odsPolicyDetail = hiveContext.sql("select policy_id,policy_code,holder_name,policy_status,product_code," +
      "preserve_policy_no,case when preserve_policy_no is not null then 1 else 0 end as renewal_policy,num_of_preson_first_policy," +
      "order_date,belongs_industry from odsdb.ods_policy_detail")
    //读取雇主销售表
    val odsGuzhuSaleManDetail = hiveContext.sql("select ent_name,salesman from odsdb.ods_ent_guzhu_salesman_detail")

    //读取产品表
    val odsProductDetail = hiveContext.sql("select product_code as product_code_salve,one_level_pdt_cate,two_level_pdt_cate,business_line from odsdb.ods_product_detail ")

    //读取dw层理赔表
    val dwPolicyCliamDetail = hiveContext.sql("select policy_code as policy_code_salve,count(case_no) as case_no_count,sum(res_pay) as res_pay from dwdb.dw_policy_claim_detail GROUP BY policy_code")

    //读取dw层每日已赚保费
    val dwPolicyEverydayPremiumDetail = hiveContext.sql("select policy_id as policy_id_salve,sum(premium) as charge_premium from dwdb.dw_policy_everyday_premium_detail where day_id <= regexp_replace(substr(cast(now() as STRING),1,10),'-','') GROUP BY policy_id")

    //读取企业信息表
    val odsEnterpriseDetail = hiveContext.sql("select ent_name,office_address,office_province,office_district from odsdb.ods_enterprise_detail")

    //读取产品方案表
    val odsPolicyPlanDetail = hiveContext.sql("select policy_code as policy_code_salve,sku_ratio,sku_price,sku_coverage from odsdb.ods_policy_product_plan_detail")

    //读取在保人数表
    val odsPoliocyInsureDetail = hiveContext.sql("select policy_id as policy_id_salve,count(insured_cert_no) as person_count " +
      "from odsdb.ods_policy_insured_detail GROUP BY policy_id")


    //读取工种表
    val dwWorkTypeMatching_one = hiveContext.sql("select policy_code as policy_code_work,profession_type," +
      "sum(case when bzn_work_risk = '1' then 1 else 0 end) as one_risk_count," +
      "sum(case when bzn_work_risk = '2' then 1 else 0 end) as two_risk_count," +
      "sum(case when bzn_work_risk = '3' then 1 else 0 end) as three_risk_count," +
      "sum(case when bzn_work_risk = '4' then 1 else 0 end) as four_risk_count," +
      "sum(case when bzn_work_risk = '5' then 1 else 0 end) as five_risk_count," +
      "sum(case when bzn_work_risk = '6' then 1 else 0 end) as six_risk_count," +
      "sum(case when bzn_work_risk not in ( '1','2','3','4','5','6') then 1 else 0 end) as other_risk_count " +
      "from dwdb.dw_work_type_matching_claim_detail group by policy_code,profession_type")


    //读取工种表_未知工种和其他工种个数
    val dwWorkTypeMatching_two = hiveContext.sql("SELECT " +
      "sum(case when work_name_check='未知' then 1 else 0 end) as work_risk_unknown,count(work_type) as work_counts,policy_code as policy_code_work " +
      "from dwdb.dw_work_type_matching_claim_detail GROUP BY policy_code")


    //保单表关联产品表
    val res1_salve = odsPolicyDetail.join(odsProductDetail, 'product_code === 'product_code_salve, "leftouter")
      .selectExpr("policy_id", "policy_code", "policy_status",
        "preserve_policy_no", "holder_name", "business_line", "product_code", "renewal_policy", "order_date", "belongs_industry",
        "num_of_preson_first_policy", "order_date", "belongs_industry")
      .where("policy_status in (0,1,-1) and business_line = '雇主' and product_code not in ('LGB000001','17000001')")

    //将上述结果关联销售表

    val res1 = res1_salve.join(odsGuzhuSaleManDetail, 'holder_name === 'ent_name, "leftouter")
      .selectExpr("policy_id", "policy_code", "policy_status", "salesman",
        "preserve_policy_no", "holder_name", "business_line", "product_code", "renewal_policy", "order_date", "belongs_industry",
        "num_of_preson_first_policy", "order_date", "belongs_industry")
    println(res1.count())
    //将上述结果关联产品方案表
    val res2 = res1.join(odsPolicyPlanDetail, 'policy_code === 'policy_code_salve, "leftouter")
      .selectExpr("policy_id", "policy_code", "holder_name","salesman",
        "preserve_policy_no", "renewal_policy", "belongs_industry",
        "num_of_preson_first_policy", "order_date", "sku_ratio", "sku_price", "sku_coverage")

    //将上述结果关联企业信息表
    val res3 = res2.join(odsEnterpriseDetail, 'holder_name === 'ent_name, "leftouter")
      .selectExpr("policy_id", "policy_code", "holder_name",
        "preserve_policy_no", "renewal_policy", "belongs_industry","salesman",
        "num_of_preson_first_policy", "order_date", "sku_ratio",
        "sku_price", "sku_coverage", "office_address", "office_province", "office_district")

    //将上述结果关联理赔表
    val res4 = res3.join(dwPolicyCliamDetail, 'policy_code === 'policy_code_salve, "leftouter")
      .selectExpr("policy_id", "policy_code", "holder_name",
        "preserve_policy_no", "renewal_policy", "belongs_industry","salesman",
        "num_of_preson_first_policy", "order_date", "sku_ratio",
        "sku_price", "sku_coverage", "office_address", "office_province", "office_district", "case_no_count", "res_pay")

    //将上述结果关联每日已赚保费表
    val res5 = res4.join(dwPolicyEverydayPremiumDetail, 'policy_id === 'policy_id_salve, "leftouter")
      .selectExpr("policy_id", "policy_code", "holder_name", "preserve_policy_no",
        "num_of_preson_first_policy", "order_date", "sku_ratio", "renewal_policy", "belongs_industry",
        "sku_price", "sku_coverage", "office_address", "office_province","salesman",
        "office_district", "case_no_count", "res_pay", "charge_premium")

    //将上述结果关联工种表
    val res6 = res5.join(dwWorkTypeMatching_one, 'policy_code === 'policy_code_work, "leftouter")
      .selectExpr("policy_id", "policy_code", "holder_name", "preserve_policy_no",
        "num_of_preson_first_policy", "order_date", "sku_ratio", "renewal_policy", "belongs_industry",
        "sku_price", "sku_coverage", "office_address", "office_province", "profession_type",
        "office_district", "case_no_count", "res_pay", "charge_premium", "one_risk_count","salesman",
        "two_risk_count", "three_risk_count", "four_risk_count", "five_risk_count", "six_risk_count")

    //上述结关联工种表
    val res7 = res6.join(dwWorkTypeMatching_two, 'policy_code === 'policy_code_work, "leftouter")
      .selectExpr("policy_id", "policy_code", "holder_name", "preserve_policy_no",
        "num_of_preson_first_policy", "order_date", "sku_ratio", "renewal_policy", "belongs_industry",
        "sku_price", "sku_coverage", "office_address", "office_province", "profession_type",
        "office_district", "case_no_count", "res_pay", "charge_premium", "one_risk_count",
        "two_risk_count", "three_risk_count", "four_risk_count", "five_risk_count","salesman",
        "six_risk_count", "work_risk_unknown","work_counts")

    //将上述结果关联被人表
    val res8 = res7.join(odsPoliocyInsureDetail, 'policy_id === 'policy_id_salve, "leftouter")
      .selectExpr("policy_id", "policy_code", "holder_name", "preserve_policy_no",
        "num_of_preson_first_policy", "order_date", "sku_ratio", "renewal_policy", "belongs_industry",
        "sku_price", "sku_coverage", "office_address", "office_province", "profession_type",
        "office_district", "case_no_count", "res_pay", "charge_premium", "person_count", "one_risk_count",
        "two_risk_count", "three_risk_count", "four_risk_count", "five_risk_count",
        "six_risk_count", "work_risk_unknown", "salesman","work_counts",
        "(work_counts-one_risk_count-two_risk_count-three_risk_count-four_risk_count-five_risk_count-six_risk_count-work_risk_unknown) as work_risk_other")


    //注册临时表
    res8.registerTempTable("tableTemp")

    //续投的明细信息:假如,B是通过A保单得来的,那么B就是A的续一保单,C通过B保单来的,那么C就是B的续一,C就是A的续二
    val res9 = hiveContext.sql(
      """
        |select a.*,b.case_no_count as one_case_no_count,b.charge_premium as one_charge_premium,
        |c.person_count as two_person_count,d.res_pay as three_res_pay
        |from tableTemp a
        |left join tableTemp b
        |on b.policy_code=a.preserve_policy_no
        |left join tableTemp c
        |on c.policy_code=b.preserve_policy_no
        |left join tableTemp d
        |on d.policy_code=c.preserve_policy_no
        |""".stripMargin)

    //父级三类和总工种:假如B是通过A保单来的,那么A就是B的父级保单.
    val res10 = hiveContext.sql(
      """
        |select a.policy_code as preserve_policy_no_salve,b.work_counts as max_work_counts,b.three_risk_count as max_three_risk_count
        |from tableTemp a
        |left join tableTemp b
        |on b.policy_code=a.preserve_policy_no
        """.stripMargin)

    //合并
    val res11 = res9.join(res10, 'policy_code === 'preserve_policy_no_salve, "leftouter")
      .selectExpr(
        "policy_id",
        "policy_code",
        "belongs_industry",
        "salesman",
        "num_of_preson_first_policy",
        "order_date",
        "sku_ratio",
        "sku_price",
        "sku_coverage",
        "profession_type",
        "renewal_policy",
        "office_address",
        "office_province",
        "case when office_district =0 then null else office_district end as office_district",
        "one_case_no_count",
        "one_charge_premium",
        "two_person_count",
        "three_res_pay",
        "three_risk_count",
        "four_risk_count",
        "five_risk_count",
        "work_risk_unknown",
        "work_risk_other",
        "work_counts",
        "max_work_counts",
        "max_three_risk_count")

    res11






    /* //续投的明细信息
     val res9 = hiveContext.sql(
       """
         |select a.*,b.case_no_count as one_case_no_count,b.charge_premium as one_charge_premium,
         |c.person_count as two_person_count,d.res_pay as three_res_pay
         |from tableTemp a
         |left join tableTemp b
         |on b.policy_code=a.preserve_policy_no
         |left join tableTemp c
         |on c.policy_code=b.preserve_policy_no
         |left join tableTemp d
         |on d.policy_code=c.preserve_policy_no
         |""".stripMargin)

     //父级三类和总工种
     val res10 = hiveContext.sql(
       """
         |select b.preserve_policy_no as preserve_policy_no_salve,b.work_counts as max_work_counts,b.three_risk_count as max_three_risk_count
         |from tableTemp a
         |left join tableTemp b
         |on a.policy_code=b.preserve_policy_no
         """.stripMargin)

    //合并
    val res11 = res9.join(res10, 'policy_code === 'preserve_policy_no, "leftouter")
      .selectExpr(
        "policy_code",
        "num_of_preson_first_policy",
        "policy_effect_date",
        "sku_ratio",
        "sku_price",
        "sku_coverage",
        "office_address",
        "office_province",
        "office_city",
        "one_case_no_count",
        "one_charge_premium",
        "two_person_count",
        "three_res_pay",
        "three_risk_count",
        "four_risk_count",
        "five_risk_count",
        "work_risk_unknown",
        "work_risk_other",
        "max_work_counts",
        "max_three_risk_count")*/


  }

}