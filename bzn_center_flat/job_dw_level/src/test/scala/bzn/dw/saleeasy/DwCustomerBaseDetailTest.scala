package bzn.dw.saleeasy

import bzn.dw.saleeasy.DwSaleEasyDetailTest.sparkConfInfo
import bzn.dw.util.SparkUtil
import bzn.job.common.Until
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

/*
* @Author:liuxiang
* @Date：2019/10/16
* @Describe:
*/ object DwCustomerBaseDetailTest extends SparkUtil with  Until{


  /**
    *  获取配置信息
    * @param args
    */
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "local[*]")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    val res = CustomerBase(hiveContext)

  }

  /**
    *
    * @param hqlContext
    */


  def  CustomerBase(hqlContext:HiveContext): Unit ={

    // 读取企业信息表
    val odsEnterprise = hqlContext.sql("select ent_id,ent_name,office_province,office_city from odsdb.ods_enterprise_detail")

    //读取客户归属信息表
    val odsEntGuzhuSalesman = hqlContext.sql("select channel_id,channel_name,ent_id as entId,ent_name as entName,salesman,biz_operator,consumer_category,business_source from odsdb.ods_ent_guzhu_salesman_detail")

    //读取销售团队表,拿到team
    val odsEntSaleTeam = hqlContext.sql("select sale_name,team_name from odsdb.ods_ent_sales_team_dimension ")

    //客户归属信息表关联销售团队表
    val saleAndTeamRes = odsEntGuzhuSalesman.join(odsEntSaleTeam, odsEntGuzhuSalesman("salesman") === odsEntSaleTeam("sale_name"), "leftouter")
      .selectExpr("channel_id", "channel_name", "entId", "entName", "salesman", "team_name", "biz_operator", "consumer_category", "business_source")
    saleAndTeamRes.show(10)

    // 读取保单明细表
    val odsPolicyDetail = hqlContext.sql("select policy_id as id,product_code,holder_name,order_date,policy_status from odsdb.ods_policy_detail")

   // 读取在保人信息表

    val policyCurrInsured = hqlContext.sql("select policy_id,day_id,count from dwdb.dw_policy_curr_insured_detail")

    //读取产品表
    val odsProduct = hqlContext.sql("select product_code as insure_code,two_level_pdt_cate from odsdb.ods_product_detail")

    /**
      * 注册一张临时表
      */


     //再保人表关联保单明细表
    val res1 = policyCurrInsured.join(odsPolicyDetail, policyCurrInsured("policy_id") === policyCurrInsured("id"), "leftouter")
      .selectExpr( "policy_id","product_code", "holder_name", "policy_status", "day_id")


    //将结果关联产品明细表
    val res2 = res1.join(odsProduct, res1("product_code") === odsProduct("insure_code"), "leftouter")
      .selectExpr("policy_id", "product_code", "holder_name", "policy_status", "day_id", "two_level_pdt_cate")

    // 关联saleAndTeamRes
     res2.join(saleAndTeamRes, res2("holder_name") === saleAndTeamRes("entName"), "leftouter")
      .selectExpr("policy_id", "product_code", "holder_name", "policy_status", "day_id", "two_level_pdt_cate", "channel_name", "ent_name")
      .map(x=>{
        val policyID = x.getAs[String]("policy_id")
        x.getAs[String]("product_code")
        x.getAs[String]("holder_name")
        x.getAs[Int]("policy_status")
        x.getAs[String]("day_id")
        x.getAs[String]("two_level_pdt_cate")
        x.getAs[String]("channel_name")
        x.getAs[String]("ent_name")

        //获取当前时间
        val nowTime = getNowTime().substring(0,10).replaceAll("-","")

      })















  }


}
