package bzn.dm.aegis

import bzn.dm.util.SparkUtil
import bzn.job.common.DataBaseUtil
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * author:xiaoYuanRen
  * Date:2020/2/13
  * Time:18:13
  * describe: this is new class
  **/
object DmInsuredDetail extends SparkUtil with DataBaseUtil{
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    val res = getInsuredDetail(hiveContext)
    val user103 = "mysql.username.103"
    val pass103 = "mysql.password.103"
    val url103 = "mysql_url.103.dmdb"
    val driver = "mysql.driver"
    val tableName1 = "dm_insured_detail"
    val tableName2 = ""
    val user106 = "mysql.username.106"
    val pass106 = "mysql.password.106"
    val url106 = "mysql.url.106.dmdb"

    saveASMysqlTable(res: DataFrame, tableName1: String, SaveMode.Overwrite,user106:String,pass106:String,driver:String,url106:String)

    sc.stop()
  }

  def getInsuredDetail(sqlContext:HiveContext) = {
    val res = sqlContext.sql(
      """
        |select a.holder_name,a.holder_province,a.holder_city,a.insured_subject,
        |b.policy_id,b.insured_cert_no,b.age,b.gender,b.start_date,b.end_date,if(b.job_company is null,a.insured_subject,b.job_company) as job_company,b.insured_province,b.insured_city,
        |case when d.channel_name = '直客' then d.ent_name else d.channel_name end as channel_name,
        |case when e.holder_province is null then a.holder_province else e.holder_province end as job_province,
        |case when e.holder_city is null then a.holder_city else e.holder_city end as job_city,
        |date_format(now(),'yyyy-MM-dd HH:mm:ss') as dw_create_time
        |from
        |(
        |select x.policy_id,x.product_code,x.policy_status,x.policy_code,x.holder_name,x.insured_subject,
        |y.holder_city,y.holder_province
        |from odsdb.ods_policy_detail x
        |left join
        |(
        |select x.holder_name,y.province as holder_province,case when y.province in ('北京市','天津市','重庆市','上海市') then y.province else y.short_name end as holder_city from (
        |select holder_name,row_number() over(partition by holder_name order by length(belongs_regional) desc) as rand1,belongs_regional
        |from odsdb.ods_policy_detail where belongs_regional is not null
        |) x
        |left join  odsdb.ods_area_info_dimension  y
        |on concat(substr(x.belongs_regional,1,4),'00') = y.code
        |where x.rand1 = 1
        |) y
        |on x.holder_name = y.holder_name
        |)a
        |left join
        |(
        |select x.policy_id,x.insured_cert_no,x.age,x.gender,x.start_date,x.end_date,x.job_company,
        |case when y.province in ('北京市','天津市','重庆市','上海市') then y.province else y.short_name end as insured_city,y.province as insured_province
        |from odsdb.ods_policy_insured_detail x
        |left join odsdb.ods_area_info_dimension  y
        |on concat(substr(x.insured_cert_no,1,4),'00') = y.code
        |) b
        |on a.policy_id = b.policy_id
        |left join odsdb.ods_product_detail c
        |on a.product_code = c.product_code
        |left join odsdb.ods_ent_guzhu_salesman_detail d
        |on a.holder_name = d.ent_name
        |left join
        |(
        |select holder_name,holder_city,holder_province from (
        |select x.holder_name,
        |case when y.province in ('北京市','天津市','重庆市','上海市') then y.province else y.short_name end as holder_city,
        |y.province as holder_province ,
        |row_number() over(partition by x.holder_name order by x.policy_start_date asc) as rand1
        |from odsdb.ods_policy_detail x
        |left join  odsdb.ods_area_info_dimension  y
        |on concat(substr(x.belongs_regional,1,4),'00') = y.code
        |) z
        |where z.rand1 =1
        |) e
        |on b.job_company = e.holder_name
        |where a.policy_status in (0,1,-1) and c.one_level_pdt_cate = '蓝领外包'
      """.stripMargin)
      .drop("policy_id")
    res
  }
}
