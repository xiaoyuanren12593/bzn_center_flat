package bzn.ods.policy

import bzn.job.common.{MysqlUntil, Until}
import bzn.util.SparkUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext

object OdsFnolPhoenixfnol extends SparkUtil with Until with MysqlUntil {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName: String = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "")

    val sc: SparkContext = sparkConf._2

    val hiveContext: HiveContext = sparkConf._4

    val res = FnolPhoenixfnol(hiveContext)
    saveASMysqlTable(res, "ods_claim_detail", SaveMode.Overwrite, "mysql.username", "mysql.password", "mysql.driver", "mysql.url")

    sc.stop()
  }

  def FnolPhoenixfnol(hiveContext: HiveContext): DataFrame = {
    import hiveContext.implicits._
    val user_106 = "mysql.username.106"
    val ps_106 = "mysql.password.106"
    val driver_106 = "mysql.driver"
    val sourcce_url_106 = "mysql.url.106"

    //读取报案表
    val fnolPhoenixfnol = readMysqlTable(hiveContext, "fnol_phoenixfnol", user_106, ps_106, driver_106, sourcce_url_106)
      .selectExpr("fnol_no",
        "claim_no",
        "insurance_policy_no",
        "occurrence_scene",
        "injury_type",
        "status",
        "fnol_date",
        "estimated_loss_amount",
        "adjuster_name",
        "latest_processing_time",
        "create_time",
        "update_time")

    //读取b报案出险人表
    val fnolInsuredPhoenixfnol = readMysqlTable(hiveContext, "fnol_insured_phoenixfnol", user_106, ps_106, driver_106, sourcce_url_106)
      .selectExpr("fnol_no as fnol_no_temp",
        "name",
        "cert_no",
        "mobile")

    //读取报案出险信息表
    val fnolLossPhoenixfnol = readMysqlTable(hiveContext, "fnol_loss_phoenixfnol", user_106, ps_106, driver_106, sourcce_url_106)
      .selectExpr("fnol_no as fnol_no_temp1",
        "loss_date",
        "loss_desc")

    //保案表关联报案出险人表
    val res1 = fnolPhoenixfnol.join(fnolInsuredPhoenixfnol, 'fnol_no === 'fnol_no_temp, "leftouter")
      .selectExpr("fnol_no",
        "claim_no",
        "insurance_policy_no",
        "occurrence_scene",
        "injury_type",
        "status",
        "estimated_loss_amount",
        "adjuster_name",
        "latest_processing_time",
        "create_time",
        "update_time",
        "fnol_date",
        "name",
        "cert_no",
        "mobile")

    //将上述结果关联报案出险信息表
    val res2 = res1.join(fnolLossPhoenixfnol, 'fnol_no === 'fnol_no_temp1, "leftouter")
      .selectExpr("fnol_no",
        "claim_no",
        "insurance_policy_no",
        "occurrence_scene",
        "injury_type",
        "status",
        "estimated_loss_amount",
        "adjuster_name",
        "latest_processing_time",
        "create_time",
        "update_time",
        "loss_date",
        "fnol_date",
        "name",
        "cert_no",
        "mobile",
        "loss_desc")

    //结果
    val res = res2.selectExpr(
      "fnol_no as case_no",
      "name as report_name",
      "cert_no",
      "case when occurrence_scene = '1001'then'上下班路上' when occurrence_scene='1002' then '工作期间' when occurrence_scene ='1003' then '非工作期间' else '其他' end as scene",
      "mobile",
      "insurance_policy_no as policy_no",
      "loss_date as risk_date",
      "fnol_date as report_date",
      "loss_desc as case_detail",
      "case when injury_type='I001' then '创伤' when injury_type='I002' then '轻伤' " +
        "when injury_type='I003' then '骨折/骨裂' when injury_type='I004' then '烧伤/烫伤' " +
        "when injury_type='I005' then '死亡' when injury_type='I006' then '伤残' when injury_type='I007' then '其他' when injury_type='I008' then '生育' " +
        "when injury_type='I009' then '医疗' else '未知' end as case_type",
      "case when status=-1 then '已删除' when status=0 then '未完成' " +
        "when status=1 then '待审核' when status=2 then '审核未通过' when status=3 then '审核通过' " +
        "when status=4 then '撤案待审核' when status=5 then '撤案审核拒绝' when status=6 then '撤案' " +
        "when status=7 then '关闭' when status=8 then '理赔中' else '未知' end as status",
      "case when estimated_loss_amount is null then 0 else estimated_loss_amount end as pre_com",
      "claim_no as CLAIM_NO",
      "adjuster_name as OPERATOR",
      "create_time as CREATE_TIME",
      "update_time as UPDATE_TIME",
      "latest_processing_time as sign_time"
    )

    res

  }


}
