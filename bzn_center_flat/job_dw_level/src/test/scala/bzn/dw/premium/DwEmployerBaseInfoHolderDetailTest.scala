package bzn.dw.premium

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import bzn.dw.util.SparkUtil
import bzn.job.common.{MysqlUntil, Until, WareUntil}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

/*
* @Author:liuxiang
* @Date：2019/11/8
* @Describe:
*/ object DwEmployerBaseInfoHolderDetailTest extends SparkUtil with Until with MysqlUntil{

  /**
    * 获取配置信息
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "")

    val sc = sparkConf._2
    val hqlContext = sparkConf._4
    val res = EmployerBaseInfoHolder(hqlContext)


    sc.stop()

  }

  def  EmployerBaseInfoHolder(hqlContext:HiveContext): Unit ={

    import hqlContext.implicits._
    hqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    hqlContext.udf.register("clean", (str: String) => clean(str))
    hqlContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      //设置日期格式
      val date = df.format(new Date()) // new Date()为获取当前系统时间
      (date + "")
    })


    // 读取保单明细表
    val odsPolicyDetailTemp = hqlContext.sql("select policy_id ,product_code,holder_name,order_date," +
      "policy_status,insure_company_name,policy_start_date,concat(substring(belongs_regional,1,4),'00') as belongs_regional," +
      "policy_create_time,policy_update_time from odsdb.ods_policy_detail")


    //读取产品表
    val odsProduct = hqlContext.sql("select product_code as insure_code,two_level_pdt_cate from odsdb.ods_product_detail")

    //保单关联产品表 拿到雇主外包的产品

    val policyAndproduct = odsPolicyDetailTemp.join(odsProduct, odsPolicyDetailTemp("product_code") === odsProduct("insure_code"), "leftouter")
      .selectExpr("policy_id", "product_code", "holder_name", "order_date", "policy_status", "insure_company_name", "policy_start_date",
        "belongs_regional", "two_level_pdt_cate", "policy_create_time", "policy_update_time")
      .where("two_level_pdt_cate in ('外包雇主','骑士保','大货车') and policy_status in (0,1,-1)")



    //同一个公司 投保省份不同 拿到保单最近的时间的城市
    val odsPolicyDetailRes = policyAndproduct.selectExpr("holder_name", "belongs_regional", "policy_start_date", "insure_company_name")
    val odsPolicyDetail = policyAndproduct.selectExpr("holder_name", "belongs_regional", "policy_start_date", "insure_company_name")
      .map(x => {
        val holderName = x.getAs[String]("holder_name")
        val belongsRegional = x.getAs[String]("belongs_regional")
        val policyStartDate = x.getAs[Timestamp]("policy_start_date")

        ((holderName), (belongsRegional, policyStartDate))
      }).reduceByKey((x, y) => {
      val res = if (x._1 == null) {
        y
      } else if (y._1 == null) {
        x
      } else {
        if (x._2.compareTo(y._2) > 0) {
          x
        } else {
          y
        }
      }
      res
    }).map(x => {
      (x._1, x._2._1, x._2._2)
    }).toDF("holderName", "belongsRegional", "policyStartDate")

    val odsPolicyDetailSalve = odsPolicyDetail.join(odsPolicyDetailRes, odsPolicyDetail("holderName") === odsPolicyDetailRes("holder_name"), "leftouter")
      .selectExpr("holderName", "belongsRegional", "policyStartDate", "insure_company_name")

    odsPolicyDetailSalve.registerTempTable("policyAndSaleTemp")

    val PolicyTemp = hqlContext.sql("select holderName as holder_name ,belongsRegional as belongs_regional," +
      "min(policyStartDate) as start_date,insure_company_name from policyAndSaleTemp group by holderName,belongsRegional,insure_company_name")


    //读取客户归属信息表
    val odsEntGuzhuSalesman = hqlContext.sql("select channel_id,channel_name,ent_id as entId," +
      "ent_name as entName,salesman,biz_operator,consumer_category,business_source from odsdb.ods_ent_guzhu_salesman_detail")

    //读取销售团队表
    val odsEntSaleTeam = hqlContext.sql("select sale_name,team_name from odsdb.ods_ent_sales_team_dimension ")

    //客户归属信息表关联销售团队表,拿到team
    val saleAndTeamRes = odsEntGuzhuSalesman.join(odsEntSaleTeam, odsEntGuzhuSalesman("salesman") === odsEntSaleTeam("sale_name"), "leftouter")
      .selectExpr("channel_id", "channel_name", "entId", "entName", "salesman", "team_name", "biz_operator", "consumer_category", "business_source")


    val policyAndSale = PolicyTemp.join(saleAndTeamRes, PolicyTemp("holder_name") === saleAndTeamRes("entName"), "leftouter")
      .selectExpr("entId", "entName", "holder_name", "channel_id", "channel_name", "belongs_regional", "salesman", "team_name", "biz_operator",
        "consumer_category", "business_source", "start_date", "insure_company_name")

    //读取城市码表
    val odsArea = hqlContext.sql("select code,province, short_name from odsdb.ods_area_info_dimension")

    val dataFrame = policyAndSale.join(odsArea, policyAndSale("belongs_regional") === odsArea("code"), "leftouter")
      .selectExpr("holder_name as holder", "channel_id", "channel_name", "entId", "entName", "province", "short_name",
        "salesman", "team_name", "biz_operator", "consumer_category", "business_source", "insure_company_name as insure_company_name_temp")


    //保单分组后的表关联
    val res = PolicyTemp.join(dataFrame, 'holder_name === 'holder and 'insure_company_name === 'insure_company_name_temp, "leftouter")
      .selectExpr("getUUID() as id",
        "entId",
        "entName",
        "holder_name as holderName",
        "channel_id",
        "case when channel_name  ='直客' then entName else channel_name end as channel_name",
        "start_date",
        "province",
        "short_name as city",
        "salesman",
        "team_name",
        "biz_operator",
        "consumer_category",
        "business_source",
        "insure_company_name",
        "getNow() as create_time",
        "getNowT() as update_time")

    res

  }


}
