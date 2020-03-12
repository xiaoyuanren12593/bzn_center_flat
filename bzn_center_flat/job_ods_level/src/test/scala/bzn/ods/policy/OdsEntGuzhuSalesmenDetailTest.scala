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

    val res = getEntEmpSalemensData(hiveContext)


//    hiveContext.sql("truncate table odsdb.ods_ent_guzhu_salesman_detail")

//    res.repartition(10).write.mode(SaveMode.Append).saveAsTable("odsdb.ods_ent_guzhu_salesman_detail")

    sc.stop()
  }

  def getEntEmpSalemensData(sqlContext:HiveContext) = {
    import sqlContext.implicits._

    sqlContext.udf.register("getMD5",(str:String) => MD5(str))

    val tProposalBznbusiName = "t_proposal_bznbusi"
    val tProposalHolderCompanyBznbusiName = "t_proposal_holder_company_bznbusi"
    val bsChannelBznmanaName = "bs_channel_bznmana"
    val odsSaleDist2020Dimension = "ods_sale_dist_2020_dimension"
    val empName = "ods_ent_guzhu_salesman_detail"
    val officialUrl = "mysql.url.106"
    val officialUrlOdsdb = "mysql.url.106.odsdb"
    val officialUrlDmdb = "mysql.url.106.dmdb"
    val officialUser = "mysql.username.106"
    val officialPass = "mysql.password.106"
    val driver = "mysql.driver"

    val user103 = "mysql.username.103"
    val pass103 = "mysql.password.103"
    val url103 = "mysql_url.103.odsdb"

//    val empData = readMysqlTable(sqlContext: SQLContext, empName: String,officialUser:String,officialPass:String,driver:String,officialOdsdbUrl:String)
//      .selectExpr("ent_id as ent_id_salve","getMD5(ent_name) as ent_id_new","ent_name","channel_id",
//        "getMD5(case when channel_name = '直客' then ent_name else channel_name end) as channel_id_new",
//        "case when channel_name = '直客' then ent_name else channel_name end as channel_name")
//      .where("channel_id_new <> channel_id or ent_id_new <> ent_id")

    val empData = readMysqlTable(sqlContext: SQLContext, empName: String,officialUser:String,officialPass:String,driver:String,officialUrlOdsdb:String)

    val empDataTemp = empData.selectExpr("case when channel_name = '直客' then ent_name else channel_name end as channel_name_salve",
      "salesman as salesman_salve","biz_operator as biz_operator_salve")
      .distinct()

    /**
      * 读取2020年销售表
      */
    val odsSaleDist2020DimensionData = readMysqlTable(sqlContext: SQLContext, odsSaleDist2020Dimension: String,officialUser:String,officialPass:String,driver:String,officialUrlOdsdb:String)

    /**
      * 读取投保单表
      */
    val tProposalBznbusi =  readMysqlTable(sqlContext: SQLContext, tProposalBznbusiName: String,officialUser:String,officialPass:String,driver:String,officialUrl:String)
      .where("business_type='2' and `status` = 5")
      .selectExpr("proposal_no","trim(holder_name) as holder_name","trim(sell_channel_name) as channel_name",
        "business_belong_user_name as salesman","sell_channel_code")

    /**
      * 投保人企业信息
      */
    val tProposalHolderCompanyBznbusi = readMysqlTable(sqlContext: SQLContext, tProposalHolderCompanyBznbusiName: String,officialUser:String,officialPass:String,driver:String,officialUrl:String)
      .selectExpr("proposal_no as proposal_no_salve","province_name")

    val tProposalAndHolder = tProposalBznbusi.join(tProposalHolderCompanyBznbusi,tProposalBznbusi("proposal_no")===tProposalHolderCompanyBznbusi("proposal_no_salve"),"leftouter")
      .drop("proposal_no")
      .drop("proposal_no_salve")
      .distinct()

    /**
      * 读取投保单表
      */
    val bsChannelBznmana =  readMysqlTable(sqlContext: SQLContext, bsChannelBznmanaName: String,officialUser:String,officialPass:String,driver:String,officialUrl:String)
      .where("business_type='2'")
      .selectExpr("channel_id",
        "channel_user_type")
      .distinct()

    val proposalAndChannelData = tProposalAndHolder.join(bsChannelBznmana,tProposalBznbusi("sell_channel_code")===bsChannelBznmana("channel_id"),"leftouter")
      .selectExpr("getMD5(holder_name) as ent_id","holder_name",
        "case when channel_user_type  = 2 then getMD5(holder_name) else getMD5(channel_name) end as channel_id",
        "case when channel_user_type  = 2 then '直客' else channel_name end as channel_name","province_name",
        "salesman")
      .cache()

    /**
      * 新增的数据
      */
    val newData = proposalAndChannelData.join(empData.selectExpr("ent_id as ent_id_salve"),'ent_id==='ent_id_salve,"leftouter")
      .where("ent_id_salve is null")
      .selectExpr("ent_id","holder_name as ent_name", "channel_id", "channel_name", "salesman","province_name as province_name_master"
      )

    val resTemp = newData.join(odsSaleDist2020DimensionData,'province_name_master==='province,"leftouter")
      .selectExpr("ent_id","ent_name", "channel_id", "channel_name","case when channel_name = '直客' then ent_name else channel_name end as channel_name_master",
        "salesman","biz_operator")

    /**
      * 如果这个渠道是历史有的 用历史渠道的销售和运营
      */
    val res = resTemp.join(empDataTemp,'channel_name_master==='channel_name_salve,"leftouter")
      .selectExpr("ent_id","ent_name", "channel_id", "channel_name",
        "case when channel_name_salve is not null then salesman_salve else salesman end as salesman",
        "case when channel_name_salve is not null then biz_operator_salve else biz_operator end as biz_operator",
        "date_format(now(),'yyyy-MM-dd HH:mm:ss') as create_time",
        "date_format(now(),'yyyy-MM-dd HH:mm:ss') as update_time")

    res.show(1000)

    res.printSchema()

//    saveASMysqlTable(res: DataFrame, empName: String, SaveMode.Append,officialUser:String,officialPass:String,driver:String,officialUrlDmdb:String)

//    val resultToHive = readMysqlTable(sqlContext: SQLContext, empName: String,officialUser:String,officialPass:String,driver:String,officialUrlDmdb:String)
//    resultToHive.printSchema()
//    resultToHive
  }
}
