package bzn.ods.policy

import bzn.job.common.{MysqlUntil, Until}
import bzn.ods.util.SparkUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext

/*
* @Author:liuxiang
* @Date：2019/12/17
* @Describe:
*/ object OdsHealthRenewalDetail extends SparkUtil with Until with MysqlUntil {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "")

    val sc = sparkConf._2

    val hiveContext = sparkConf._4
    val res = HealthPreserve(hiveContext)
    hiveContext.sql("truncate table odsdb.ods_health_installment_plan")
    res.write.mode(SaveMode.Append).saveAsTable("odsdb.ods_health_installment_plan")

    sc.stop()

  }


  def HealthPreserve(hiveContext: HiveContext): DataFrame = {
    import hiveContext.implicits._
    hiveContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    //读取保单表
    val bPolicyBzncen = readMysqlTable(hiveContext, "b_policy_bzncen", "mysql.username.106",
      "mysql.password.106", "mysql.driver", "mysql.url.106")
      .selectExpr("policy_no as policy_no_salve", "proposal_no", "holder_name", "sell_channel_name")


    //读取健康续期表

    val bPolicyInstallmentPlanBzncen = readMysqlTable(hiveContext, "b_policy_installment_plan_bzncen",
      "mysql.username.106", "mysql.password.106", "mysql.driver", "mysql.url.106")
      .selectExpr("policy_no", "premium", "status", "update_time")
      .where("status =2")

    val tProposalSubjectPersonMaster = readMysqlTable(hiveContext, "t_proposal_subject_person_master_bznrobot",
      "mysql.username.106", "mysql.password.106", "mysql.driver", "mysql.url.106")
      .selectExpr("proposal_no as proposal_no_salve", "name")

    //健康续期数据关联保单表
    val resTemp = bPolicyInstallmentPlanBzncen.join(bPolicyBzncen, 'policy_no === 'policy_no_salve, "leftouter")
      .selectExpr("policy_no", "proposal_no", "premium", "holder_name", "sell_channel_name", "update_time")

    val res = resTemp.join(tProposalSubjectPersonMaster, 'proposal_no === 'proposal_no_salve, "leftouter")
      .selectExpr(
        "getUUID() as id",
        "policy_no as insurance_policy_no",
        "concat(policy_no,'_',date_format(update_time,'yyyyMMddHHmmss')) as preserve_id",
        "premium as premium_total",
        "holder_name",
        "name as insurer_name",
        "sell_channel_name as channel_name",
        "update_time as policy_effective_time",
        "date_format(now(), 'yyyy-MM-dd HH:mm:ss') as create_time",
        "date_format(now(), 'yyyy-MM-dd HH:mm:ss') as update_time")
    res
  }

}