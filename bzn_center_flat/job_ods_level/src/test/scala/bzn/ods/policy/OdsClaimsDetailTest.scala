package bzn.ods.policy

import bzn.job.common.{MysqlUntil, Until}
import bzn.util.SparkUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext

object OdsClaimsDetailTest extends SparkUtil with Until with MysqlUntil {


  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName: String = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "local[*]")

    val sc: SparkContext = sparkConf._2
    val sqlContext = sparkConf._3
    val hiveContext: HiveContext = sparkConf._4

    val res = ClaimsDetail(hiveContext)
    hiveContext.sql("truncate table odsdb.ods_claims_detail_v2")

    res.write.mode(SaveMode.Append).saveAsTable("odsdb.ods_claims_detail_v2")
    res.show(100)
   sc.stop()


  }

  def ClaimsDetail(hiveContext: HiveContext): DataFrame = {
    import hiveContext.implicits._
    hiveContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    val user_106 = "mysql.username.106"
    val ps_106 = "mysql.password.106"
    val driver_106 = "mysql.driver"
    val sourcce_url_106 = "mysql.url.106"

    //读取报案表
    val fnolPhoenixfnol = readMysqlTable(hiveContext, "fnol_phoenixfnol", user_106, ps_106, driver_106, sourcce_url_106)
      .selectExpr("fnol_no", "claim_no", "insurance_policy_no", "occurrence_scene", "injury_type", "status")

    //读取理赔表
    val claimPhoenixclaim = readMysqlTable(hiveContext, "claim_phoenixclaim", user_106, ps_106, driver_106, sourcce_url_106)
      .selectExpr("fnol_no as fnol_no_temp", "claim_no as claim_no_temp1", "holder_name", "loss_date", "fnol_date", "end_date",
        "estimated_compensation_amount", "compensation_amount", "disability_level", "apply_amount")

    //读取理赔出险人表
    val claimInsuredPhoenixclaim = readMysqlTable(hiveContext, "claim_insured_phoenixclaim", user_106, ps_106, driver_106, sourcce_url_106)
      .selectExpr("claim_no as claim_no_temp2", "name", "cert_no", "mobile")

    //读取理赔申请责任表,行转列
    val claimApplyLiabilityTemp = readMysqlTable(hiveContext, "claim_apply_liability_phoenixclaim", user_106, ps_106, driver_106, sourcce_url_106)
      .selectExpr("claim_no as claim_no_temp3",
        "case when liability_code='2000000224' then compensation_amount else 0 end as hos_benefits",
        "case when liability_code='2000000217' then compensation_amount else 0 end as medical_coverage",
        "case when liability_code='2000000229' then compensation_amount else 0 end as delay_payment",
        "case when liability_code='2000000209' then compensation_amount else 0 end as disable_death_payment_1",
        "case when liability_code='2000000213' then compensation_amount else 0 end as disable_death_payment_2"
      )
            //注册临时表
    claimApplyLiabilityTemp.registerTempTable("tableTemp")
    val claimApplyLiability = hiveContext.sql("select claim_no_temp3,sum(hos_benefits) as hos_benefits," +
      "sum(medical_coverage) as medical_coverage,sum(delay_payment) as delay_payment,sum(disable_death_payment_1+disable_death_payment_2)  from tableTemp group by claim_no_temp3")

       /* "liability_code", "liability_name", "compensation_amount")*/

    //理赔表关联理赔出险人表
    val res1 = claimPhoenixclaim.join(claimInsuredPhoenixclaim, 'claim_no_temp1 === 'claim_no_temp2, "leftouter")
      .selectExpr("fnol_no_temp", "claim_no_temp1", "holder_name", "loss_date", "fnol_date", "end_date", "estimated_compensation_amount",
        "compensation_amount", "disability_level", "name", "cert_no", "mobile", "apply_amount")

    //将上述结果关联责任申请表
    val res2 = res1.join(claimApplyLiability, 'claim_no_temp1 === 'claim_no_temp3, "leftouter")
      .selectExpr("fnol_no_temp", "claim_no_temp1", "holder_name", "loss_date", "fnol_date", "end_date", "estimated_compensation_amount",
        "compensation_amount", "disability_level", "name", "cert_no", "mobile", "hos_benefits", "medical_coverage", "delay_payment", "apply_amount")

    //报案表关联上述结果
    val res4 = fnolPhoenixfnol.join(res2, 'claim_no === 'claim_no_temp1, "leftouter")
      .selectExpr("fnol_no", "claim_no", "insurance_policy_no", "occurrence_scene", "injury_type",
        "status", "claim_no_temp1", "holder_name", "loss_date", "fnol_date",
        "end_date", "estimated_compensation_amount",
        "compensation_amount", "disability_level", "name",
        "cert_no", "mobile", "hos_benefits", "medical_coverage", "delay_payment", "apply_amount")


    val res = res4.selectExpr(
      "getUUID() as id",
      "fnol_no as case_no",
      "insurance_policy_no as policy_no",
      "loss_date as risk_date",
      "fnol_date as report_date",
      "name as risk_name",
      "cert_no as risk_cert_no",
      "mobile",
      "holder_name as insured_company",
      "estimated_compensation_amount as pre_com",
      "disability_level",
      "case when occurrence_scene = '1001'then'上下班路上' when occurrence_scene='1002' then '工作期间' when occurrence_scene ='1003' then '非工作期间' else '其他' end as scene",
      "case when injury_type='I001' then '创伤' when injury_type='I002' then '轻伤' " +
        "when injury_type='I003' then '骨折/骨裂' when injury_type='I004' then '烧伤/烫伤' " +
        "when injury_type='I005' then '死亡' when injury_type='I006' then '伤残' when injury_type='I007' then '其他' when injury_type='I008' then '生育' " +
        "when injury_type='I009' then '医疗' else '未知' end as case_type",
       "case when status=-1 then '已删除' when status=-1 then '已删除' when status=0 then '未完成' " +
        "when status=1 then '待审核' when status=2 then '审核未通过' when status=3 then '审核通过' " +
        "when status=4 then '撤案待审核' when status=5 then '撤案审核拒绝' when status=6 then '已撤案' " +
        "when status=7 then '关闭' when status=8 then '理赔中' else '未知' end as case_status",
      "end_date as case_close_date",
      "hos_benefits", "medical_coverage", "delay_payment",
      "apply_amount as disable_death_payment",
      "compensation_amount as final_payment"
    )

    res

  }


}
