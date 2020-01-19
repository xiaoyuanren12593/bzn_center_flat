package bzn.ods.policy

import bzn.job.common.{DataBaseUtil, Until}
import bzn.ods.util.SparkUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext

/**
  * author:xiaoYuanRen
  * Date:2020/1/15
  * Time:16:12
  * describe: 雇主销售渠道数据
  **/
object OdsEntGuzhuSalesmenDetailTest extends SparkUtil with Until with DataBaseUtil{
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName,"local[*]")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4

    val one = getEntEmpSalemensData(hiveContext)

    sc.stop()
  }

  def getEntEmpSalemensData(sqlContext:HiveContext) = {
    import sqlContext.implicits._

    sqlContext.udf.register("getMD5",(str:String) => MD5(str))

    val tProposalBznbusiName = "t_proposal_bznbusi"
    val bsChannelBznmanaName = "bs_channel_bznmana"
    val empName = "ods_ent_guzhu_salesman_detail"
    val officialUrl = "mysql.url.106"
    val officialOdsdbUrl = "mysql.url.106.odsdb"
    val officialUser = "mysql.username.106"
    val officialPass = "mysql.password.106"
    val driver = "mysql.driver"

//    val empData = readMysqlTable(sqlContext: SQLContext, empName: String,officialUser:String,officialPass:String,driver:String,officialOdsdbUrl:String)
//      .selectExpr("ent_id as ent_id_salve","getMD5(ent_name) as ent_id_new","ent_name","channel_id",
//        "getMD5(case when channel_name = '直客' then ent_name else channel_name end) as channel_id_new",
//        "case when channel_name = '直客' then ent_name else channel_name end as channel_name")
//      .where("channel_id_new <> channel_id or ent_id_new <> ent_id")

    val empData = readMysqlTable(sqlContext: SQLContext, empName: String,officialUser:String,officialPass:String,driver:String,officialOdsdbUrl:String)
      .selectExpr("ent_id as ent_id_salve")

    /**
      * 读取投保单表
      */
    val tProposalBznbusi =  readMysqlTable(sqlContext: SQLContext, tProposalBznbusiName: String,officialUser:String,officialPass:String,driver:String,officialUrl:String)
      .where("business_type='2' and `status` = 5")
      .selectExpr("trim(holder_name) as holder_name","trim(sell_channel_name) as channel_name",
        "business_belong_user_name as salesman","sell_channel_code")
      .distinct()

    /**
      * 读取投保单表
      */
    val bsChannelBznmana =  readMysqlTable(sqlContext: SQLContext, bsChannelBznmanaName: String,officialUser:String,officialPass:String,driver:String,officialUrl:String)
      .where("business_type='2'")
      .selectExpr("channel_id",
        "channel_user_type")
      .distinct()

    val proposalAndChannelData = tProposalBznbusi.join(bsChannelBznmana,tProposalBznbusi("sell_channel_code")===bsChannelBznmana("channel_id"),"leftouter")
      .selectExpr("getMD5(holder_name) as ent_id","holder_name",
        "case when channel_user_type  = 1 then getMD5(holder_name) else getMD5(channel_name) end as channel_id",
        "case when channel_user_type  = 1 then '直客' else channel_name end as channel_name",
        "salesman")
      .cache()

    val res = proposalAndChannelData.join(empData,'ent_id==='ent_id_salve,"leftouter")
      .where("ent_id_salve is null")
      .selectExpr("ent_id","holder_name as ent_name", "channel_id", "channel_name", "salesman",
        "date_format(now(),'yyyy-MM-dd HH:mm:ss') as create_time",
        "date_format(now(),'yyyy-MM-dd HH:mm:ss') as update_time"
      )

    saveASMysqlTable(res: DataFrame, empName: String, SaveMode.Append,officialUser:String,officialPass:String,driver:String,officialOdsdbUrl:String)
//    res.foreach(println)
  }
}
