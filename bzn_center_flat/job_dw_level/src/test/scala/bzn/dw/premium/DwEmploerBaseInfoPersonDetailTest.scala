package bzn.dw.premium

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import bzn.dw.premium.DwEmployerBaseInfoDetailTest.{DwEmployerBaseInfoDetail, clean, sparkConfInfo}
import bzn.dw.premium.DwTypeOfWorkClaimDetailTest.{getBeginTime, getFormatTime}
import bzn.dw.util.SparkUtil
import bzn.job.common.Until
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.hive.HiveContext

/*
* @Author:liuxiang
* @Date：2019/10/9
* @Describe:雇主基础信息人员明细表
*/ object DwEmploerBaseInfoPersonDetailTest extends  SparkUtil with  Until{

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "local[*]")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    val res = EmployerBaseInfoPersonDetail(hiveContext)
     res.printSchema()

    sc.stop()
  }


  /**
    *
    * @param sqlContext
    * @return
    */

  def EmployerBaseInfoPersonDetail(sqlContext: HiveContext) = {
    import sqlContext.implicits._
    sqlContext.udf.register("clean", (str: String) => clean(str))
    sqlContext.udf.register("getUUID",()=>(java.util.UUID.randomUUID() + "").replace("-", ""))
    sqlContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      //设置日期格式
      val date = df.format(new Date()) // new Date()为获取当前系统时间
      (date + "")
    })

    //读取保单明细表

    val odsPolicyDetailTemp: DataFrame = sqlContext.sql("select policy_id,policy_code,holder_name,insured_subject,product_code " +
      ",policy_status,policy_start_date,policy_end_date ,channel_id as channelId,channel_name as channelName,sales_name as salesName,insure_company_name from odsdb.ods_policy_detail")
      .where("policy_status in (1,0,-1)")

    //读取保险公司表
       val insuranceCompany = sqlContext.sql("select insurance_company,short_name from odsdb.ods_insurance_company_temp_dimension")

    //关联两个表
    val odsPolicyDetail = odsPolicyDetailTemp.join(insuranceCompany, odsPolicyDetailTemp("insure_company_name") === insuranceCompany("insurance_company"), "leftouter")
      .selectExpr("policy_id", "policy_code", "holder_name",
        "insured_subject", "product_code", "policy_status", "policy_start_date", "policy_end_date", "channelId", "channelName", "salesName", "insure_company_name", "short_name")

    //读取企业联系人
    val odsEnterpriseDetail = sqlContext.sql("select ent_id,ent_name from odsdb.ods_enterprise_detail")

    //读取客户归属销售表
    val odsEntGuzhuDetail: DataFrame =
      sqlContext.sql("select ent_id as entid,ent_name as entname,channel_id,(case when channel_name='直客' then ent_name else channel_name end) as channel_name ,salesman,biz_operator from odsdb.ods_ent_guzhu_salesman_detail")

    // 将企业联系人 与 客户归属销售表关联  拿到 渠道id和name 销售名称
    val enterperiseAndEntGuzhu = odsEnterpriseDetail.join(odsEntGuzhuDetail, odsEnterpriseDetail("ent_id") === odsEntGuzhuDetail("entid"), "leftouter")
      .selectExpr("ent_id", "ent_name", "channel_id", "channel_name","salesman","biz_operator")

    //读取销售团队表
    val odsSalesmanDetail = sqlContext.sql("select sale_name,team_name from odsdb.ods_salesman_detail")

    /**
      * 关联两个表，拿字段
      */
    val enterperiseAndSaleRes = enterperiseAndEntGuzhu.join(odsSalesmanDetail, enterperiseAndEntGuzhu("salesman") === odsSalesmanDetail("sale_name"), "leftouter")
      .selectExpr("ent_id", "ent_name", "channel_id", "channel_name", "salesman", "team_name","biz_operator")

    // 将关联结果与保单明细表关联
    val resDetail = odsPolicyDetail.join(enterperiseAndSaleRes, odsPolicyDetail("holder_name") === enterperiseAndSaleRes("ent_name"), "leftouter")
      .selectExpr("policy_id", "policy_code","policy_start_date","policy_end_date", "holder_name", "insured_subject", "product_code","policy_status","ent_id", "ent_name", "channel_id", "channelId","channel_name","channelName", "salesman","salesName", "insure_company_name","short_name","team_name","biz_operator")

    //读取产品表
    val odsProductDetail = sqlContext.sql("select product_code as product_code_temp,product_name,one_level_pdt_cate from odsdb.ods_product_detail")

    //将关联结果与产品表关联 拿到产品类别
    val resProductDetail = resDetail.join(odsProductDetail, resDetail("product_code") === odsProductDetail("product_code_temp"), "leftouter")

      .selectExpr("policy_id", "policy_code", "policy_start_date","policy_end_date","holder_name", "insured_subject", "product_code", "policy_status","one_level_pdt_cate","ent_id", "ent_name",
        "channel_id", "channelId","channel_name","channelName", "salesman","salesName","insure_company_name","short_name","team_name","biz_operator")
      .where("one_level_pdt_cate = '蓝领外包' and product_code not in ('LGB000001','17000001')")

    //读取被保人表
    val odsPolicyInsured = sqlContext.sql("select policy_id as policy_id_salve,insured_name,insured_cert_no,insured_mobile,start_date,end_date from odsdb.ods_policy_insured_detail")
      .where("policy_id_salve = '360472629502676992'")
    odsPolicyInsured.show()

    /**
      * 将上述结果与被保人表关联
      */
    val resProductAndInsuredDetail = resProductDetail.join(odsPolicyInsured,resProductDetail("policy_id")===odsPolicyInsured("policy_id_salve"),"leftouter")
      .selectExpr("policy_id", "policy_code", "policy_start_date","policy_end_date","holder_name", "insured_subject","insured_name", "insured_cert_no",
        "insured_mobile","start_date","end_date",
        "product_code","policy_status",
        "one_level_pdt_cate","ent_id", "ent_name",


        "channel_id", "channelId","channel_name","channelName", "salesman","salesName", "insure_company_name","short_name","team_name","biz_operator")

    val res1 = resProductAndInsuredDetail.selectExpr("policy_id","insured_cert_no","start_date","end_date")


    /**
      * 读取理赔表
      */
    val res2 = sqlContext.sql("SELECT policy_id as id,risk_cert_no,risk_date ,res_pay ,risk_cert_no,risk_date ,res_pay from dwdb.dw_policy_claim_detail")

    //将res1 关联 res2  判断理赔表中的 每个保单的每个人的理赔钱数

    val res3 = res1.join(res2, 'policy_id === 'id and 'insured_cert_no === 'risk_cert_no)
      .where("start_date is not null")
      .selectExpr("policy_id", "insured_cert_no", "start_date", "end_date", "risk_date", "res_pay")
      .map(x => {
        val policy_id = x.getAs[String]("policy_id")
        val insured_cert_no = x.getAs[String]("insured_cert_no")
        val start_date = x.getAs[Timestamp]("start_date")
        val end_date = x.getAs[Timestamp]("end_date")
        val riskDate = x.getAs[String]("risk_date")
        val riskDateRes = if (riskDate != null) {
          java.sql.Timestamp.valueOf(getFormatTime(getBeginTime(riskDate.replaceAll("/", "-").concat(" 00:00:00"))))
        } else {
          null
        }
        val res_pay = x.getAs[java.math.BigDecimal]("res_pay")
        (policy_id, insured_cert_no, start_date, end_date, riskDateRes, res_pay)
      }).toDF("id", "risk_cert_no", "start_date", "end_date", "risk_date", "res_pay")

    val res4 = res3.selectExpr("id", "risk_cert_no", " start_date ", "end_date", "risk_date","res_pay",
      "case when risk_date is null then 1 when risk_date >= start_date and risk_date <= end_date then 1 else 0 end as tem")
      .where("tem = 1")

    val res5 = res4.selectExpr("id", "risk_cert_no", "start_date as start_date_temp ", "end_date as end_date_temp", "risk_date", "res_pay",
      "tem")

    res5.registerTempTable("tempTable")
    val tenmpTbles = sqlContext.sql("select sum(res_pay) as res_pay,id,risk_cert_no,start_date_temp from tempTable group by id, risk_cert_no,start_date_temp")

    /**
      * 将上述结果与理赔表关联
      */
    val insuredAndClaimRes = resProductAndInsuredDetail.join(tenmpTbles, 'policy_id === 'id and 'insured_cert_no === 'risk_cert_no and 'start_date === 'start_date_temp,"leftouter")
      .selectExpr("policy_id", "policy_code", "policy_start_date","policy_end_date","holder_name", "insured_subject", "product_code","insured_name", "insured_cert_no",

      "insured_mobile","start_date","end_date","policy_status", "one_level_pdt_cate","ent_id", "ent_name", "channel_id", "channelId","channel_name","channelName","insure_company_name","short_name","salesman","salesName", "team_name", "biz_operator", "res_pay")

    //读取方案信息表
    val odsPolicyProductPlanDetail: DataFrame = sqlContext.sql("select policy_code as policy_code_temp,product_code as product_code_temp,sku_coverage,sku_append," +
      "sku_ratio,sku_price,sku_charge_type,tech_service_rate,economic_rate," +
      "commission_discount_rate,commission_rate from odsdb.ods_policy_product_plan_detail")

    //将上述结果与方案信息表关联
    val res = insuredAndClaimRes.join(odsPolicyProductPlanDetail, insuredAndClaimRes("policy_code") === odsPolicyProductPlanDetail("policy_code_temp"), "leftouter")
      .selectExpr("getUUID() as id","clean(policy_id) as policy_id", "clean(policy_code) as policy_code","policy_start_date","policy_end_date", "policy_status",
        " clean(holder_name) as holder_name",
        "clean(insured_subject) as insured_subject","clean(insured_name) as insured_name", "clean(insured_cert_no) as insured_cert_no",
        "clean(insured_mobile) as insured_mobile","start_date","end_date", "clean(product_code) as product_code",
        "clean(one_level_pdt_cate) as one_level_pdt_cate", "clean(ent_id) as ent_id ", "clean(ent_name) as ent_name",
        "clean(case when channel_id is null then channelId else channel_id end) as channel_id ",
        "clean(case when channel_name is null then channelName else channel_name end) as channel_name",
        "clean(insure_company_name) as insure_company_name ",
        "clean(short_name) as insure_company_short_name",
        "clean(case when salesman is null then salesName else salesman end) as sale_name",
        "clean(team_name) as team_name",
        "clean(biz_operator) as biz_operator",
        "sku_coverage", "clean(sku_append) as sku_append", "clean(sku_ratio) as sku_ratio", "sku_price",
        "clean(sku_charge_type) as sku_charge_type ",
        "tech_service_rate", "economic_rate", "commission_discount_rate", "commission_rate", "res_pay",
        "getNow() as dw_create_time")

    res.show()
    res



  }

}
