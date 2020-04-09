package bzn.dw.premium

import bzn.dw.util.SparkUtil
import bzn.job.common.Until
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

/**
  * author:xiaoYuanRen
  * Date:2020/4/9
  * Time:11:29
  * describe: 非雇主基础信息表
  **/
object DwUnEmployerBaseInfoDetailTest extends  SparkUtil with  Until{

  def main(args: Array[String]): Unit = {
    System.setProperty ("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo (appName, "local[*]")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    val res = getBaseinfoData (hiveContext)
    sc.stop ()
  }

  /**
    * 非雇主基础信息数据
    * @param sqlContext 上下文
    */
  def getBaseinfoData(sqlContext:HiveContext) = {
    sqlContext.udf.register("getUUID",()=>(java.util.UUID.randomUUID() + "").replace("-", ""))
    sqlContext.udf.register("getMD5", (str: String) => MD5(str))
    val res = sqlContext.sql(
      """
        |select getUUID() as id,a.policy_code,a.policy_no,a.policy_id,a.insure_company_name,g.short_name as insure_company_short_name,a.holder_name,a.holder_type,a.insured_subject,getMD5(a.channel_name) as channel_id,a.channel_name,
        |case when a.sales_name is null and b.business_line = '体育' then c.sales_name when a.sales_name is null and b.business_line = '健康' then '保准健康' when a.sales_name is null and b.business_line in ('员福','场景') then '公司' else a.sales_name end as sales_name,
        |d.team_name,d.department,d.group_name,
        |a.num_of_preson_first_policy,a.sum_premium,a.policy_start_date,a.policy_end_date,a.policy_effect_date,
        |b.one_level_pdt_cate,b.two_level_pdt_cate,b.business_line,a.product_code,b.product_name,b.product_desc,
        |f.commission_discount_rate,f.commission_rate,f.economic_rate,f.tech_service_rate,
        |date_format(now(),'yyyy-MM-dd HH:mm:ss') as dw_create_time
        |from odsdb.ods_policy_detail a
        |left join odsdb.ods_product_detail b
        |on a.product_code = b.product_code
        |left join (select * from odsdb.ods_sports_customers_dimension where type = 1) c
        |on a.channel_name = c.name
        |left join odsdb.ods_ent_sales_team_dimension d
        |on (case when a.sales_name is null and b.business_line = '体育' then c.sales_name when a.sales_name is null and b.business_line = '健康' then '保准健康' when a.sales_name is null and b.business_line in ('员福','场景') then '公司' else a.sales_name end) = d.sale_name
        |left join odsdb.ods_policy_product_plan_detail f
        |on a.policy_code = f.policy_code
        |left join odsdb.ods_insurance_company_temp_dimension g
        |on a.insure_company_name = g.insurance_company
        |where b.business_line <> '雇主' and business_line <> '接口' and a.policy_status in (0,1,-1)
      """.stripMargin)

    res.printSchema()
  }
}
