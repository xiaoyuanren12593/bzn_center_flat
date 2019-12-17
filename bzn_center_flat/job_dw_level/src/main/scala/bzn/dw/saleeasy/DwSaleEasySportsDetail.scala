package bzn.dw.saleeasy

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import bzn.dw.util.SparkUtil
import bzn.job.common.{ClickHouseUntil, DataBaseUtil, Until}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext

/*
* @Author:liuxiang
* @Date：2019/10/15
* @Describe:
*/
object DwSaleEasySportsDetail  extends  SparkUtil with Until with DataBaseUtil{
  case class DmbBatchingMonitoringDetail(id: String,project_name:String,warehouse_leve:String,house_name:String,table_name:String,
                                         status:Int,remark:String,create_time:Timestamp,update_time:Timestamp)
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName,"")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    val res = SaleEasy(hiveContext)
    import hiveContext.implicits._

    val tableMysqlName = "dm_batching_monitoring_detail"
    val updateColumns: Array[String] = Array("status","remark","update_time")
    val urlFormat = "mysql.url.103.dmdb"
    val userFormat = "mysql.username.103"
    val possWordFormat = "mysql.password.103"
    val driverFormat = "mysql.driver"
    val nowTime = getNowTime().substring(0,10)

    hiveContext.sql("truncate table dwdb.dw_saleeasy_sports_detail")
    res.repartition(10).write.mode(SaveMode.Append).saveAsTable("dwdb.dw_saleeasy_sports_detail")

    val tableName = "dm_saleeasy_sports_detail"
    val urlTest = "clickhouse.url.odsdb.test"
    val user = "clickhouse.username"
    val possWord = "clickhouse.password"
    val driver = "clickhouse.driver"

    writeClickHouseTable(res:DataFrame,tableName: String,SaveMode.Overwrite,urlTest:String,user:String,possWord:String,driver:String)

    val data = hiveContext.sql("select * from dwdb.dw_saleeasy_sports_detail limit 100").count()

    if(data > 0){
      val dataMonitor =
        Seq(
          DmbBatchingMonitoringDetail(nowTime+"dm_batching_monitoring_detail","销售易体育数据清洗","dm","dmdb","dm_batching_monitoring_detail",1,"销售易体育数据清洗-成功",new Timestamp(System.currentTimeMillis()),new Timestamp(System.currentTimeMillis()))
        ).toDF()

      insertOrUpdateDFtoDBUsePoolNew(tableMysqlName: String, dataMonitor: DataFrame, updateColumns: Array[String],
        urlFormat:String,userFormat:String,possWordFormat:String,driverFormat:String)
    }
    sc.stop()
  }

  /**
    * 读取数据
    * @param hqlContext
    */
  def SaleEasy(hqlContext:HiveContext): DataFrame ={

    import hqlContext.implicits._
    hqlContext.udf.register ("getUUID", () => (java.util.UUID.randomUUID () + "").replace ("-", ""))
    hqlContext.udf.register ("getNow", () => {
      val df = new SimpleDateFormat ("yyyy-MM-dd HH:mm:ss")
      //设置日期格式
      val date = df.format (new Date ()) // new Date()为获取当前系统时间
      date + ""
    })

    // 读取保单表
    val odsPolicyDetail = hqlContext.sql("select product_code ,policy_code,sum_premium,trim(holder_name)as holder_name,insure_company_name," +
      "channel_name,policy_create_time,policy_start_date,policy_end_date,order_date,policy_type,num_of_preson_first_policy,policy_status from odsdb.ods_policy_detail").
      where("(policy_create_time is not null or policy_start_date is not null) and insure_company_name is not null and policy_status in (0,1,-1)")
      .map(x => {
        val policyCode = x.getAs[String]("policy_code")
        val insureCode = x.getAs[String]("product_code")
        val premium = x.getAs[java.math.BigDecimal]("sum_premium")
        val holderName = x.getAs[String]("holder_name")
        val insureCompanyName = x.getAs[String]("insure_company_name")
        val channelName = x.getAs[String]("channel_name")
        val policyCreateTime = x.getAs[Timestamp]("policy_create_time")
        var startDate = x.getAs[Timestamp]("policy_start_date")
        val endDate = x.getAs[Timestamp]("policy_end_date")
        var orderDate = x.getAs[Timestamp]("order_date")
        val policyType = x.getAs[String]("policy_type")
        val policyTypeRes = if (policyType == "2") {
          "团单"
        } else if (policyType == "1") {
          "个单"
        } else {
          "团单"
        }
        //开始时间
        startDate = if (policyCreateTime != null) {
          if (startDate == null) {
            policyCreateTime
          } else if (startDate != null && policyCreateTime.compareTo(startDate) >= 0) {
            policyCreateTime
          } else {
            startDate
          }
        } else {
          startDate
        }
        //投保单时间
        orderDate = if (policyCreateTime != null) {
          if (orderDate == null) {
            policyCreateTime
          } else if (orderDate != null && policyCreateTime.compareTo(orderDate) >= 0) {
            policyCreateTime
          } else {
            orderDate
          }
        } else {
          orderDate
        }

        var numberOfPople: Int = x.getAs[Int]("num_of_preson_first_policy")
        numberOfPople = if (numberOfPople == 0) {
          1
        } else {
          numberOfPople
        }

        (policyCode, insureCode, premium, holderName, insureCompanyName, channelName, policyTypeRes, startDate, endDate, orderDate, numberOfPople)
      })
      .toDF("policy_code", "insure_code", "premium", "holder_name", "insure_company_name", "channel_name", "policy_type",
        "start_date", "end_date", "order_date", "num_pople")

    //读取产品表
    val odsProductDetail = hqlContext.sql("select product_code ,one_level_pdt_cate,product_name from odsdb.ods_product_detail")

    //体育渠道表
    val odsSportsCustomers = hqlContext.sql("select name,customer_type,sales_name,source,type from odsdb.ods_sports_customers_dimension")
      .where("type = 1")
      .map(x => {
        val name = x.getAs[String]("name")  //客户名称
        var customerType = x.getAs[String]("customer_type") //客户类型
        if(customerType == null){
          customerType = "日常营销"
        }
        var salesName = x.getAs[String]("sales_name") //销售姓名
        if(salesName == null){
          salesName = "保准体育"
        }
        var source = x.getAs[String]("source") //初始来源
        if(source == null){
          source = "线上业务"
        }
        (name,source,customerType,salesName)
      })
      .toDF("name","source","customerType","sales")


    // 保单表关联产品表
    val policyAndproduct = odsPolicyDetail.join(odsProductDetail, odsPolicyDetail("insure_code") === odsProductDetail("product_code"), "leftouter")
      .selectExpr("policy_code", "insure_code", "premium", "holder_name", "insure_company_name", "channel_name", "policy_type",
        "start_date", "end_date", "order_date", "product_name", "num_pople", "one_level_pdt_cate")

    // 将上述结果关联体育渠道表
    val res = policyAndproduct.join(odsSportsCustomers, policyAndproduct("channel_name") === odsSportsCustomers("name"), "leftouter")
      .where("one_level_pdt_cate ='体育'")
      .selectExpr("getUUID() as id", "policy_code", "cast(premium as decimal(14,4)) as premium", "holder_name", "insure_company_name",
        "channel_name", "start_date", "end_date", "order_date", "product_name", "policy_type", "source as type", "customerType as type_detail",
        "sales","num_pople","cast(date_format(getNow(),'yyyy-MM-dd') as date) as date", "cast(getNow() as timestamp) as dw_create_time")

   res
  }
}
