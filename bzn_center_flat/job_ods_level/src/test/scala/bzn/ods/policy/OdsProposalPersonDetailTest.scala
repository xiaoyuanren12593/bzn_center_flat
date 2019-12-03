package bzn.ods.policy

import bzn.job.common.{MysqlUntil, Until}
import bzn.util.SparkUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext

/**
  * author:xiaoYuanRen
  * Date:2019/12/2
  * Time:15:50
  * describe: this is new class
  **/
object OdsProposalPersonDetailTest extends SparkUtil with MysqlUntil with Until{
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName,"local[*]")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4

    getPersonData(hiveContext)
    sc.stop()

  }
  val tableName  = "t_proposal_bznbusi"
  val tableName1  = "t_proposal_subject_person_master_bznbusi"

  //    val user103 = "mysql.username.103"
  //    val pass103 = "mysql.password.103"
  //    val url103 = "mysql_url.103.dmdb"
  val driver = "mysql.driver"
  val user106 = "mysql.username.106"
  val pass106 = "mysql.password.106"
  val url106 = "mysql.url.106"

 // select a.*,b.*,d.bzn_work_risk,d.gs_work_name,gs_work_risk,substring(gs_work_risk,1,1) as '截取第一位' from
 // (select proposal_no,insurance_policy_no,holder_name,`status`,insurance_name,sell_channel_name from t_proposal) a
 //   left join (select proposal_no,cert_no,work_type,start_date from t_proposal_subject_person_master) b
 //   on a.proposal_no = b.proposal_no
 // left join dm_work_matching_detail c
 //   on b.work_type = c.primitive_work
 // left join dm_work_risk_detail d
 //   on c.work_name_check = d.bzn_work_name
 // where a.insurance_name like '%中国人寿财%'and b.start_date >= '2019-11-25' and b.start_date <= '2019-12-01'
 // ;


  def getPersonData(sqlContext:HiveContext) = {
    val tProposalBznbusi: DataFrame = readMysqlTable(sqlContext: SQLContext, tableName: String,user106:String,pass106:String,driver:String,url106:String)
      .where("insurance_name like '%中国人寿财%'")
      .selectExpr("proposal_no","insurance_policy_no","holder_name","status","insurance_name","sell_channel_name")

    val tProposalSubjectPersonMasterBznbusi: DataFrame = readMysqlTable(sqlContext: SQLContext, tableName1: String,user106:String,pass106:String,driver:String,url106:String)
      .selectExpr("proposal_no as proposal_no_master","cert_no","work_type","start_date")
      .where("start_date >= '2019-11-25' and start_date <= '2019-12-01'")

    val odsWorkGradeDimension = sqlContext.sql("select policy_code as policy_code_profession,profession_type from odsdb.ods_work_grade_dimension")

    val odsWorkMatchingDimension = sqlContext.sql("select primitive_work,work_name_check from odsdb.ods_work_matching_dimension")

    val odsWorkRiskDimension = sqlContext.sql("select bzn_work_name,bzn_work_risk,gs_work_name,gs_work_risk,case when gs_work_risk is not null then substr(gs_work_risk,1,1) else null end as first from odsdb.ods_work_risk_dimension")

    val personRes = tProposalBznbusi.join(tProposalSubjectPersonMasterBznbusi,tProposalBznbusi("proposal_no")===tProposalSubjectPersonMasterBznbusi("proposal_no_master"))
      .selectExpr("proposal_no","insurance_policy_no","holder_name","status","insurance_name","sell_channel_name",
        "proposal_no_master","cert_no","work_type","start_date")

    val professionPersonRes = personRes.join(odsWorkGradeDimension,personRes("insurance_policy_no")===odsWorkGradeDimension("policy_code_profession"),"leftouter")
      .selectExpr("proposal_no","insurance_policy_no","holder_name","status","insurance_name","sell_channel_name",
        "proposal_no_master","cert_no","work_type","start_date","profession_type")

    val worktypeProfessionPersonRes = professionPersonRes.join(odsWorkMatchingDimension,professionPersonRes("work_type")===odsWorkMatchingDimension("primitive_work"),"leftouter")
      .selectExpr("proposal_no","insurance_policy_no","holder_name","status","insurance_name","sell_channel_name",
        "proposal_no_master","cert_no","work_type","start_date","profession_type","work_name_check")

    worktypeProfessionPersonRes.join(odsWorkRiskDimension,worktypeProfessionPersonRes("work_name_check")===odsWorkRiskDimension("bzn_work_name"),"leftouter")
      .selectExpr("insurance_policy_no","holder_name","status","insurance_name","sell_channel_name",
        "cert_no","work_type","start_date","bzn_work_name","bzn_work_risk","gs_work_name","gs_work_risk","first","profession_type")
      .registerTempTable("res")

    val temp = sqlContext.sql("select * from res")
    temp.show()
    temp.printSchema()

    //sqlContext.sql("select * from res").repartition(1).write.mode(SaveMode.Overwrite).save("/xing/data/OdsProposalPersonDetail/detail")


    val result = sqlContext.sql("select insurance_policy_no,insurance_name,sell_channel_name,work_type,bzn_work_name,bzn_work_risk," +
      "gs_work_name,gs_work_risk,first,count(cert_no) " +
      "from res group by insurance_policy_no,insurance_name,sell_channel_name,work_type,start_date,bzn_work_name,bzn_work_risk,gs_work_name,gs_work_risk,first")

    result.printSchema()

   // result.repartition(1).write.mode(SaveMode.Overwrite).save("/xing/data/OdsProposalPersonDetail/detail")
  }
}
