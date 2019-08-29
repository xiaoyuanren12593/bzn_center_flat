package bzn.ods.policy

import java.sql.{Connection, DriverManager, SQLException}
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import bzn.job.common.Until
import bzn.ods.util.SparkUtil
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import ru.yandex.clickhouse.{ClickHouseConnection, ClickHouseDataSource}
import ru.yandex.clickhouse.settings.ClickHouseProperties

import scala.io.Source
import scala.math.BigDecimal.RoundingMode

/**
  * author:xiaoYuanRen
  * Date:2019/8/28
  * Time:19:36
  * describe: this is new class
  **/
object jdbcConnectClickhouseTest extends SparkUtil with Until{
  def main (args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName,"")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    val sqlContext = sparkConf._3

//    val res = oneProductPlan(hiveContext)
    val conn :ClickHouseConnection =null
    val properties=new ClickHouseProperties()
    properties.setUser("default");
    properties.setPassword("iaisYuX4");
    properties.setDatabase("odsdb");
    try {

      val dataSource = new ClickHouseDataSource("jdbc:clickhouse://172.16.11.105:8123",properties); //default表示数据表 ,port原来为8123
      val connection =  dataSource.getConnection()
      if(connection !=null) {
        System.out.println("连接成功")
        println (connection.getMetaData)
      }

    } catch{
      case e: SQLException => println("连接异常")
    }

    readMysqlTable(sqlContext, "ods_policy_product_plan_detail").show()
    sc.stop()
  }

  /**
    * 产品方案表
    * @param sqlContext
    */
  def oneProductPlan(sqlContext:HiveContext) ={
    sqlContext.udf.register("clean", (str: String) => clean(str))
    sqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    sqlContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")//设置日期格式
      val date = df.format(new Date())// new Date()为获取当前系统时间
      (date + "")
    })

    import sqlContext.implicits._

    val onePlanRes = getOnePlanDetail(sqlContext)
    val twoPlanRes = getTwoPlanDetail(sqlContext)

    val unionRes = onePlanRes.unionAll(twoPlanRes)
      .selectExpr("policy_code","sku_id","product_code","sku_coverage",
        "sku_append","sku_ratio","sku_price","sku_charge_type","tech_service_rate","economic_rate","commission_rate")
      .distinct()

    /**
      * 读取历史方案配置表
      */
    val policy_product_plan_his = readMysqlTable(sqlContext,"policy_product_plan_his")
      .selectExpr("policy_code as policy_code_master","sku_coverage as sku_coverage_master","sku_append as sku_append_master",
        "sku_ratio as sku_ratio_master","sku_price as sku_price_master","sku_charge_type as sku_charge_type_master",
        "tech_service_rate","economic_rate")

    /**
      * 读取历史方案配置表 并计算出手续费率git
      */
    val policy_product_plan_his_rate = readMysqlTable(sqlContext,"policy_product_plan_his")
      .selectExpr("policy_code","tech_service_rate","economic_rate")
      .map(x => {
        val policyCode = x.getAs[String]("policy_code")
        val techServiceRate = x.getAs[java.math.BigDecimal]("tech_service_rate")
        val economicRate = x.getAs[java.math.BigDecimal]("economic_rate")
        var commissionRate = ""

        if(techServiceRate == null){
          if(economicRate == null){
            commissionRate = null
          }else{
            commissionRate = economicRate.toString
          }
        }else{
          if(economicRate == null){
            commissionRate = techServiceRate.toString
          }else{
            commissionRate = (techServiceRate.add(economicRate)).toString
          }
        }
        (policyCode,commissionRate)
      })
      .toDF("policy_code","commission_rate")

    val policy_product_plan_his_105 = policy_product_plan_his.join(policy_product_plan_his_rate,policy_product_plan_his("policy_code_master") === policy_product_plan_his_rate("policy_code"))
      .selectExpr("policy_code_master","sku_coverage_master","sku_append_master","sku_ratio_master","sku_price_master","sku_charge_type_master",
        "tech_service_rate as tech_service_rate_master","economic_rate as economic_rate_master","commission_rate as commission_rate_master")

    /**
      * 如果policy_code_master不是null：以policy_product_plan_his_105他的值为准
      */
    val resTemp = unionRes.join(policy_product_plan_his_105,unionRes("policy_code") === policy_product_plan_his_105("policy_code_master"),"leftouter")
      .selectExpr("getUUID() as id","policy_code","product_code",
        "case when policy_code_master is not null then sku_coverage_master else sku_coverage end as sku_coverage",
        "case when policy_code_master is not null then sku_append_master else sku_append end as sku_append",
        "case when policy_code_master is not null then sku_ratio_master else sku_ratio end as sku_ratio",
        "case when policy_code_master is not null then sku_price_master else sku_price end as sku_price",
        "case when policy_code_master is not null then sku_charge_type_master else sku_charge_type end as sku_charge_type",
        "case when policy_code_master is not null then tech_service_rate_master else tech_service_rate end as tech_service_rate",
        "case when policy_code_master is not null then economic_rate_master else economic_rate end as economic_rate",
        "case when policy_code_master is not null then commission_rate_master else commission_rate end as commission_rate",
        "getNow() as dw_create_time")
      .cache()

    /**
      * 众安和国寿财零工保
      */
    val firstRes = resTemp.where("product_code = 'LGB000001'")
      .selectExpr("id","policy_code","product_code","sku_coverage","sku_append", "sku_ratio", "sku_price", "sku_charge_type", "tech_service_rate",
        "economic_rate", "'0.1' as commission_rate", "dw_create_time")

    val threeRes = resTemp.where("product_code = '17000001'")
      .selectExpr("id","policy_code","product_code", "sku_coverage","sku_append","sku_ratio", "sku_price", "sku_charge_type", "tech_service_rate",
        "economic_rate", "'0.2' as commission_rate", "dw_create_time")

    /**
      * 其他产品的数据
      */
    val secondRes = resTemp.where("product_code <> 'LGB000001' and product_code <> '17000001'")

    val res = firstRes.unionAll(secondRes).unionAll(threeRes)
      .selectExpr(
        "id",
        "clean(policy_code) as policy_code",
        "clean(product_code) as product_code",
        "cast(clean(sku_coverage) as decimal(14,4)) as sku_coverage",
        "clean(sku_append) as sku_append",
        "clean(sku_ratio) as sku_ratio",
        "cast(clean(sku_price) as decimal(14,4)) as sku_price",
        "clean(sku_charge_type) as sku_charge_type",
        "cast(clean(tech_service_rate) as decimal(14,4)) as tech_service_rate ",
        "cast(clean(economic_rate) as decimal(14,4)) as economic_rate",
        "cast(clean(commission_rate) as decimal(14,4)) as commission_rate",
        "dw_create_time")
    res.limit(1)
  }

  /**
    * 1.0系统的方案信息
    */
  def getOnePlanDetail(sqlContext:HiveContext) = {
    import sqlContext.implicits._

    /**
      * 读取订单信息表
      */
    val odrOrderInfoBznprd: DataFrame = readMysqlTable(sqlContext,"odr_order_info_bznprd")
      .selectExpr("id as master_order_id","user_id")

    /**
      * 读取保单表和方案表作为临时表
      */
    val odrPolicyBznprd = readMysqlTable(sqlContext,"odr_policy_bznprd")
      .selectExpr("id","sku_id","policy_code","order_id","insure_code")

    val odrPolicyBznprdTemp = odrOrderInfoBznprd.join(odrPolicyBznprd,odrOrderInfoBznprd("master_order_id")===odrPolicyBznprd("order_id"),"leftouter")
      .selectExpr("id","sku_id","policy_code","master_order_id as order_id","user_id","insure_code")

    val onePlanRes = odrPolicyBznprdTemp
      .where("order_id not in ('934cec7f92f54be7812cfcfa23a093cb') and (user_id not in ('10100080492') or user_id is null) and insure_code not in ('15000001')")

    val twoPlanRes = odrPolicyBznprdTemp
      .where("insure_code in ('15000001') and (user_id not in ('10100080492') or user_id is null)")

    val unionPlanRes = onePlanRes.unionAll(twoPlanRes)
      .selectExpr("id","sku_id","policy_code","insure_code")
      .where("policy_code not in ('21010000889180002031','21010000889180002022','21010000889180002030')")

    /**
      * 读取产品方案表
      */
    val pdtProductSkuBznprd: DataFrame = readMysqlTable(sqlContext,"pdt_product_sku_bznprd")
      .selectExpr("id as sku_id_slave","term_one","term_three","price")

    /**
      * 从产品方案表中获取保费，特约，保费类型，伤残赔付比例
      */
    val policyRes = unionPlanRes.join(pdtProductSkuBznprd,unionPlanRes("sku_id") === pdtProductSkuBznprd("sku_id_slave"),"leftouter")
      .selectExpr("policy_code","sku_id","term_one","term_three","price","insure_code")
      .map(f = x => {
        val policyCode = x.getAs [String]("policy_code")
        val skuId = x.getAs [String]("sku_id")
        val insuredCode = x.getAs [String]("insure_code")
        val termOne = x.getAs [Int]("term_one")
        val termThree = x.getAs [Int]("term_three")
        var price = x.getAs [java.math.BigDecimal]("price")
        if( price != null ) {
          price = price.setScale (4, RoundingMode (3)).bigDecimal
        }

        var skuCoverage = "" //保费
        var skuAppend = "" //特约
        var sku_charge_type = "" //保费类型  年缴或者月缴
        var sku_ratio = "" //伤残赔付比例
        if( termThree != null && termThree.toString.length == 5 ) {
          skuCoverage = termThree.toString.substring (0, 2)
        } else {
          if( termOne != null ) {
            skuCoverage = termOne.toString
          } else {
            skuCoverage = null
          }
        }

        if( termThree != null && termThree.toString.length == 5 ) {
          skuAppend = termThree.toString.substring (2, 3)
        } else {
          skuAppend = null
        }

        if( termThree != null && termThree.toString.length == 5 ) {
          sku_charge_type = termThree.toString.substring (3, 4)
        } else {
          sku_charge_type = null
        }

        if( termThree != null && termThree.toString.length == 5 ) {
          sku_ratio = termThree.toString.substring (4, 5)
        } else {
          sku_ratio = null
        }
        var tech_service_rate = ""
        var economic_rate = ""
        var commission_rate = ""

        if( tech_service_rate == "" && economic_rate == "") {
          tech_service_rate = null
          economic_rate = null
          commission_rate = null
        }

        ( policyCode, skuId,insuredCode,skuCoverage, skuAppend, sku_ratio, price, sku_charge_type, tech_service_rate, economic_rate,commission_rate)
      })
      .toDF("policy_code","sku_id","product_code","sku_coverage","sku_append","sku_ratio","sku_price","sku_charge_type","tech_service_rate","economic_rate","commission_rate")

    policyRes.limit(1)
  }

  /**
    * 2.0系统数据信息
    */
  def getTwoPlanDetail(sqlContext:HiveContext) = {
    import sqlContext.implicits._

    /**
      * 读取保单表
      */
    val bPolicyBzncen: DataFrame = readMysqlTable(sqlContext,"b_policy_bzncen")
      .selectExpr("id as policy_id","policy_no as master_policy_no","insurance_policy_no as policy_code","product_code","premium_price")
      .cache()

    /*
     * 读取产品方案表
     */
    val bPolicyProductPlanBzncen = readMysqlTable(sqlContext,"b_policy_product_plan_bzncen")
      .selectExpr("policy_no","plan_amount","contain_trainee","payment_type","injure_percent","technology_fee","brokerage_fee")
      .map(x => {
        val policyNo = x.getAs[String]("policy_no")
        val planAmount = x.getAs[Double]("plan_amount") //方案保额
        val containTrainee = x.getAs[String]("contain_trainee") //2.0 1 2 3 null 特约（是否包含实习生）
        val paymentType = x.getAs[Int]("payment_type") //类型 年缴月缴
        val injurePercent = x.getAs[Double]("injure_percent")  //伤残比例
        var technologyFee = x.getAs[Double]("technology_fee") //技术服务费
        var brokerageFee = x.getAs[Double]("brokerage_fee") //经纪费
        var injurePercentRes = ""
        if(injurePercent == 0.05){
          injurePercentRes = "1"
        }else if (injurePercent == 0.10){
          injurePercentRes = "2"
        }else {
          injurePercentRes = null
        }

        var commissionRate = ""

        if(technologyFee != null){
          technologyFee = technologyFee/100
        }
        if(brokerageFee != null){
          brokerageFee = brokerageFee/100
        }

        if(technologyFee == null){
          if(brokerageFee == null){
            commissionRate = null
          }else{
            commissionRate = brokerageFee.toString
          }
        }else{
          if(brokerageFee == null){
            commissionRate = technologyFee.toString
          }else{
            commissionRate = (technologyFee+brokerageFee).toString
          }
        }

        (policyNo,(planAmount,containTrainee,injurePercentRes,paymentType,technologyFee,brokerageFee,commissionRate))
      })
      .reduceByKey((x1,x2)=>{
        val res = x1
        res
      })
      .map(x => {
        (x._1,x._2._1,x._2._2,x._2._3,x._2._4,x._2._5,x._2._6,x._2._7)
      })
      .toDF("policy_no_plan","sku_coverage","sku_append","sku_ratio","sku_charge_type","tech_service_rate","economic_rate","commission_rate")

    val planRes = bPolicyBzncen.join(bPolicyProductPlanBzncen,bPolicyBzncen("master_policy_no")===bPolicyProductPlanBzncen("policy_no_plan"),"leftouter")
      .selectExpr("master_policy_no as policy_code","product_code as sku_id","product_code","sku_coverage",
        "sku_append","sku_ratio","premium_price as sku_price","sku_charge_type","tech_service_rate","economic_rate","commission_rate")

    planRes.limit(1)
  }

  /**
    * 将DataFrame保存为Mysql表
    *
    * @param dataFrame 需要保存的dataFrame
    * @param tableName 保存的mysql 表名
    * @param saveMode  保存的模式 ：Append、Overwrite、ErrorIfExists、Ignore
    */
  def saveASMysqlTable(dataFrame: DataFrame, tableName: String, saveMode: SaveMode) = {
    var table = tableName
    val properties: Properties = getProPerties()
    val prop = new Properties //配置文件中的key 与 spark 中的 key 不同 所以 创建prop 按照spark 的格式 进行配置数据库
    prop.setProperty("user", properties.getProperty("mysql.username"))
    prop.setProperty("password", properties.getProperty("mysql.password"))
    prop.setProperty("driver", properties.getProperty("mysql.driver"))
    prop.setProperty("url", properties.getProperty("mysql.url"))

    if (saveMode == SaveMode.Overwrite) {
      var conn: Connection = null
      try {
          conn = DriverManager.getConnection(
          prop.getProperty("url"),
          prop.getProperty("user"),
          prop.getProperty("password")
        )
        val stmt = conn.createStatement
        table = table.toLowerCase
        stmt.execute(s"truncate table $table") //为了不删除表结构，先truncate 再Append
        conn.close()
      }
      catch {
        case e: Exception =>
          println("MySQL Error:")
          e.printStackTrace()
      }
    }
    dataFrame.write.mode(SaveMode.Append).jdbc(prop.getProperty("url"), table, prop)
  }
  /**
    * 获取 clickhouse表的数据
    *
    * @param sqlContext
    * @param tableName 读取Mysql表的名字
    * @return 返回 Mysql 表的 DataFrame
    */
  def readMysqlTable(sqlContext: SQLContext, tableName: String): DataFrame = {
    val properties: Properties = getProPerties()
    sqlContext
      .read
      .format("jdbc")
      .option("url", "jdbc:clickhouse://172.16.11.105:8123/odsdb")
      .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
      .option("user", "default")
      .option("password", "iaisYuX4")
      .option("dbtable", tableName)
      .load()
  }

  /**
    * 获取配置文件
    *
    * @return
    */
  def getProPerties() = {
    val lines_source = Source.fromURL(getClass.getResource("/config_scala.properties")).getLines.toSeq
    var properties: Properties = new Properties()
    for (elem <- lines_source) {
      val split = elem.split("==")
      val key = split(0)
      val value = split(1)
      properties.setProperty(key,value)
    }
    properties
  }
}
