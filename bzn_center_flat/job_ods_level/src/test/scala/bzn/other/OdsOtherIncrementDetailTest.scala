package bzn.other

import java.text.SimpleDateFormat
import java.util.Date

import bzn.job.common.{MysqlUntil, Until}
import bzn.other.OdsOtherIncrementDetail.{clean, dateDelect, dateDelectOneMonth, readMysqlTable}
import bzn.util.SparkUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.hive.HiveContext

/*
* @Author:liuxiang
* @Date：2019/12/5
* @Describe:
*/ object OdsOtherIncrementDetailTest extends SparkUtil with Until with MysqlUntil {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName: String = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "local[*]")

    val sc: SparkContext = sparkConf._2
    val sqlContext = sparkConf._3
    val hiveContext: HiveContext = sparkConf._4
    //val res1 = OdsOtherToHive(hiveContext)
    /* val res2 = HiveDataPerson(hiveContext)
     val res3 = weddingData(hiveContext)
     val res4 = res1.unionAll(res2).unionAll(res3)
     res4.printSchema()*/

    /* val res5 = InterAMonthAgo(hiveContext)
     res5.printSchema()*/


    sc.stop()
  }



  /**
   * 上下文
   *
   * 接口15天内数据数据
   */
  def InterDataToHive(hiveContext: HiveContext): DataFrame = {
    import hiveContext.implicits._
    hiveContext.udf.register("dateDelect", (data_time: String) => dateDelect(data_time))
    hiveContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      //设置日期格式
      val date = df.format(new Date()) // new Date()为获取当前系统时间
      (date + "")
    })


    //拿到当前时间15天内的数据
    val data1 = readMysqlTable(hiveContext, "open_other_policy", "mysql.username.103",
      "mysql.password.103", "mysql.driver", "mysql.url.103.bzn_open_all")
      .where("cast(date_add(now(),-15) as string) <= cast(if(create_time is null,now(),create_time) as string)")
      .selectExpr("policy_id",
        "insured_name",
        "insured_cert_no",
        "insured_mobile",
        "policy_no",
        "start_date",
        "end_date",
        "create_time",
        "update_time",
        "product_code",
        "null as sku_price",
        "'inter' as business_line",
        "substring(cast(month as string),1,7) as months")

    // 读取接口当月数据
    val data2 = hiveContext.sql("select policy_id as policy_id_salve,years,business_line as business_line_salve from odsdb.ods_all_business_person_base_info_detail where business_line = 'inter' and cast(date_add(now(),-15) as string) <= cast(if(create_time is null,now(),create_time) as string)")

    //拿到当月数据的增量
    val data3 = data1.join(data2, 'policy_id === 'policy_id_salve, "leftouter")
      .selectExpr("policy_id", "policy_id_salve", "insured_name", "insured_cert_no", "insured_mobile", "policy_no", "start_date", "end_date", "create_time", "update_time", "product_code", "sku_price", "business_line", "months as years")
      .where("policy_id_salve is null")
    val res = data3.selectExpr(
      "insured_name",
      "insured_cert_no",
      "insured_mobile",
      "policy_no",
      "policy_id",
      "start_date",
      "end_date",
      "create_time",
      "update_time",
      "product_code",
      "sku_price",
      "business_line",
      "years")

    res

  }


  /**
   * 核心库数据
   *
   * @param hqlContext
   * @return
   */
  def OfficialDataToHive(hqlContext: HiveContext): DataFrame = {
    hqlContext.udf.register("clean", (str: String) => clean(str))
    import hqlContext.implicits._

    //读取保单明细表
    val odsPolicyDetail = hqlContext.sql("select policy_code,policy_id,policy_status from odsdb.ods_policy_detail")

    //读取被保人表
    val odsPolicyInsuredDetail = hqlContext.sql("select insured_name,insured_cert_no,insured_mobile,policy_code as policy_code_salve,start_date," +
      "end_date,create_time,update_time from odsdb.ods_policy_insured_detail")

    //读取产品方案表
    val odsProductPlanDetail = hqlContext.sql("select policy_code,product_code,sku_price from odsdb.ods_policy_product_plan_detail")

    //拿到保单在保退保终止的保单
    val odsPolicyAndInsured = odsPolicyInsuredDetail.join(odsPolicyDetail, 'policy_code_salve === 'policy_code, "leftouter")
      .selectExpr("insured_name", "policy_id", "insured_cert_no", "insured_mobile", "policy_code_salve", "start_date", "end_date", "policy_status", "create_time", "update_time")
      .where("policy_status in (0,1,-1)")

    //拿到产品
    val res = odsPolicyAndInsured.join(odsProductPlanDetail, 'policy_code_salve === 'policy_code, "leftouter")
      .selectExpr(
        "insured_name",
        "insured_cert_no",
        "insured_mobile",
        "policy_code_salve",
        "policy_id",
        "start_date",
        "end_date",
        "create_time",
        "update_time",
        "product_code",
        "sku_price",
        "'official' as business_line",
        "trim(substring(cast(if(start_date is null,if(end_date is null ,if(create_time is null," +
          "if(update_time is null,now(),update_time),create_time),end_date),start_date) as STRING),1,7)) as years")

    res

  }


  /**
   * 婚礼纪15天内数据
   *
   * @param hqlContext
   */

  def weddingDataToHive(hqlContext: HiveContext): DataFrame = {
    import hqlContext.implicits._

    //保单表
    val openPolicy = readMysqlTable(hqlContext, "open_policy_bznapi", "mysql.username.106",
      "mysql.password.106", "mysql.driver", "mysql.url.106")
      .selectExpr("policy_no", "proposal_no", "start_date", "end_date", "create_time", "update_time", "product_code", "premium")

    //被保人表保人表
    val openInsured = readMysqlTable(hqlContext, "open_insured_bznapi", "mysql.username.106",
      "mysql.password.106", "mysql.driver", "mysql.url.106")
      .selectExpr("proposal_no as proposal_no_salve", "name", "cert_no", "tel")

    //保单表关联被保人表
    val data1 = openPolicy.join(openInsured, 'proposal_no === 'proposal_no_salve, "leftouter")
      .selectExpr(
        "policy_no",
        "proposal_no as policy_id",
        "start_date",
        "end_date",
        "create_time",
        "update_time",
        "product_code",
        "premium",
        "name as insured_name",
        "cert_no as insured_cert_no",
        "tel as insured_mobile",
        "'wedding' as business_line",
        "substring(cast(case when create_time is null then now() else create_time end as string),1,7) as years")
      .where("cast(date_add(now(),-15) as string) <= cast(if(create_time is null,now(),create_time) as string)")

    // 读取hive的表中的数据
    val data2 = hqlContext.sql("select policy_id as policy_id_salve,business_line as business_id_salve from odsdb.ods_all_business_person_base_info_detail where business_line = 'wedding' and cast(date_add(now(),-15) as string) <= cast(if(create_time is null,now(),create_time) as string)")

    //判断增量数据
    val res = data1.join(data2, 'policy_id === 'policy_id_salve, "leftouter")
      .where("policy_id_salve is null")
      .selectExpr("insured_name",
        "insured_cert_no",
        "insured_mobile",
        "policy_no",
        "policy_id",
        "start_date",
        "end_date",
        "create_time",
        "update_time",
        "product_code",
        "premium as sku_price",
        "business_line",
        "years")
    res

  }



}


/*/**
 * 接口上个月数据
 */
def InterAMonthAgo(hiveContext: HiveContext): DataFrame ={
  import hiveContext.implicits._
  hiveContext.udf.register("dateDelectOneMonth",(data_time:String) =>dateDelectOneMonth(data_time))
  hiveContext.udf.register("getNow", () => {
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    //设置日期格式
    val date = df.format(new Date()) // new Date()为获取当前系统时间
    (date + "")
  })

  //拿到上个月接口Mysql的数据(时间原因导致的数据遗漏)
  val data1 = readMysqlTable(hiveContext, "open_other_policy", "mysql.username.103",
    "mysql.password.103", "mysql.driver", "mysql.url.103.bzn_open_all")
    .where("substring(cast(month as string),1,7) = substring(cast(dateDelectOneMonth(getNow()) as string),1,7)")
    .selectExpr("policy_id", "insured_name", "insured_cert_no", "insured_mobile", "policy_no", "start_date", "end_date", "create_time", "update_time",
      "product_code", "null as sku_price", "'inter' as business_line",
      "substring(cast(month as string),1,7) as months")


  // 读取hive上个月数据
  val data2 = hiveContext.sql("select policy_id as policy_id_salve,years,business_line as business_line_salve from odsdb.ods_all_business_person_base_info_detail where business_line = 'inter' and substring(cast(dateDelectOneMonth(getNow()) as string),1,7) = years")

  //拿到上个月数据的增量
  val data3 = data1.join(data2, 'policy_id === 'policy_id_salve, "leftouter")
    .selectExpr("policy_id", "policy_id_salve", "insured_name", "insured_cert_no", "insured_mobile", "policy_no", "start_date", "end_date", "create_time", "update_time", "product_code", "sku_price", "business_line", "months as years")
    .where("policy_id_salve is null")

  val res = data3.selectExpr("policy_id", "insured_name", "insured_cert_no", "insured_mobile", "policy_no", "start_date", "end_date", "create_time", "update_time", "product_code", "sku_price", "business_line", "years")

  res

}

/**
 * 上下文
 *
 *
 */
def OdsOtherToHive(hiveContext: HiveContext): DataFrame = {
  import hiveContext.implicits._
  hiveContext.udf.register("getNow", () => {
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    //设置日期格式
    val date = df.format(new Date()) // new Date()为获取当前系统时间
    (date + "")
  })

  /**
   * 获取mysql中接口的数据
   */
  //拿到当前时间所月份的数据
  val data1 = readMysqlTable(hiveContext, "open_other_policy", "mysql.username.103",
    "mysql.password.103", "mysql.driver", "mysql.url.103.bzn_open_all")
    .where("substring(cast(if(create_time is null,getNow(),create_time) as string),1,7) = substring(cast(getNow() as string),1,7)")
    .selectExpr("policy_id", "insured_name", "insured_cert_no", "insured_mobile", "policy_no", "start_date", "end_date", "create_time", "update_time",
      "product_code", "null as sku_price", "'inter' as business_line",
      "substring(cast(if(create_time is null,getNow(),create_time) as string),1,7) as months")

  // 读取接口当月数据
  val data2 = hiveContext.sql("select policy_id as policy_id_salve,years,business_line as business_line_salve from odsdb.ods_all_business_person_base_info_detail")
    .where("substring(cast(getNow() as string),1,7) = years and business_line_salve = 'inter'")

  //拿到当月数据的增量
  val data3 = data1.join(data2, 'policy_id === 'policy_id_salve, "leftouter")
    .selectExpr("policy_id", "policy_id_salve", "insured_name", "insured_cert_no", "insured_mobile", "policy_no", "start_date", "end_date", "create_time", "update_time", "product_code", "sku_price", "business_line", "months")
    .where("policy_id_salve is null")

  val res = data3.selectExpr("insured_name", "insured_cert_no", "insured_mobile", "policy_no", "policy_id", "start_date", "end_date", "create_time", "update_time", "product_code", "sku_price", "business_line", "months as years")

  res
}

*/