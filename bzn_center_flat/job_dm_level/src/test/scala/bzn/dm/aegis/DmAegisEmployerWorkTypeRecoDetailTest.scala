package bzn.dm.aegis

import bzn.dm.util.SparkUtil
import bzn.job.common.{MysqlUntil, Until}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * author:xiaoYuanRen
  * Date:2019/9/30
  * Time:16:10
  * describe: 雇主工种匹配查询
  **/
object DmAegisEmployerWorkTypeRecoDetailTest extends SparkUtil with Until with MysqlUntil{
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "local[*]")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    readHiveToMysql(hiveContext)
    sc.stop()
  }

  /**
    * 读取hive中的数据并且存入mysql
    */
  def readHiveToMysql(sqlContext:HiveContext) ={
    val user = "mysql.username.103"
    val pass = "mysql.password.103"
    val url = "mysql_url.103.dmdb"
    val driver = "mysql.driver"
    val tableName1 = "dm_ent_work_overview_detail"
    val tableName2 = "dm_ent_work_topn_detail"
    val tableName3 = "dm_ent_work_plan_topn_detail"

    /**
      * 雇主每个企业投保人数和赔付率表
      */
    val dmEntWorkOverviewDetail = sqlContext.sql("select holder_name,holder_person_count,holder_premium,holder_pre_estimate_compensate,holder_reco_count," +
      "holder_un_reco_count,holder_whether_reco_count,holder_un_whether_reco_count,create_time,update_time from dmdb.dm_ent_work_overview_detail")

    /**
      * 已匹配工种风险级别占比
      */
    val dmEntWorkTopNDetail = sqlContext.sql("select * from dmdb.dm_ent_work_topn_detail")
      .drop("id")

    /**
      * 每个方案下的匹配率
      */
    val dmEntWorkPlanTopnDetail = sqlContext.sql("select * from dmdb.dm_ent_work_plan_topn_detail")
      .drop("id")

    saveASMysqlTable(dmEntWorkOverviewDetail: DataFrame, tableName1: String, SaveMode.Overwrite,user:String,pass:String,driver:String,url:String)
    saveASMysqlTable(dmEntWorkTopNDetail: DataFrame, tableName2: String, SaveMode.Overwrite,user:String,pass:String,driver:String,url:String)
    saveASMysqlTable(dmEntWorkPlanTopnDetail: DataFrame, tableName3: String, SaveMode.Overwrite,user:String,pass:String,driver:String,url:String)
  }
}
