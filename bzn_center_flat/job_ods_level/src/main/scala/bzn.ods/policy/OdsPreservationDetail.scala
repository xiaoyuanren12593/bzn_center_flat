package bzn.ods.policy

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import bzn.job.common.Until
import bzn.ods.util.SparkUtil
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * author:xiaoYuanRen
  * Date:2019/5/27
  * Time:15:47
  * describe: ods 层保全明细表
  **/
object OdsPreservationDetail extends SparkUtil with Until{
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName,"")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    val oneRes = onePreservationDetail(hiveContext)
    val twoRes = twoPreservationDetail(hiveContext)
    val res = oneRes.unionAll(twoRes)
    res.write.mode(SaveMode.Overwrite).saveAsTable("odsdb.ods_preservation_detail")
    sc.stop()
  }

  /**
    * 2.0系统保全信息
    * @param sqlContext
    */
  def twoPreservationDetail(sqlContext:HiveContext) ={
    import sqlContext.implicits._

    sqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))

    sqlContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")//设置日期格式
      val date = df.format(new Date())// new Date()为获取当前系统时间
      (date + "")
    })

    /**
      * 读取保全表
      */
    val bPolicyPreserveBznprdOne: DataFrame = readMysqlTable(sqlContext,"b_policy_preservation_bzncen")
      .selectExpr("id as preserve_id","policy_no","effective_date","inc_dec_order_no" ,"inc_revise_no as add_batch_code","inc_revise_premium as add_premium","inc_revise_sum as add_person_count","dec_revise_no as del_batch_code","dec_revise_premium as del_premium","dec_revise_sum as del_person_count","preservation_type as preserve_type","pay_status","create_time","update_time")

    /**
      * 增加两个关联字段
      */
    val bPolicyPreserveBznprd: DataFrame = bPolicyPreserveBznprdOne
      .withColumn("temp_policy_no",bPolicyPreserveBznprdOne("policy_no"))
      .withColumn("temp_inc_dec_order_no",bPolicyPreserveBznprdOne("inc_dec_order_no"))

    val bPolicyPreserveBznprdTemp = bPolicyPreserveBznprd
      .selectExpr("inc_dec_order_no as temp_inc_dec_order_no","effective_date","add_person_count","del_person_count","preserve_type")

    /**
      * 读取保单表
      */
    val bPolicyBzncen = readMysqlTable(sqlContext,"b_policy_bzncen")
      .selectExpr("id as policy_id","policy_no as b_policy_no","insurance_policy_no")

    /**
      * 读取被保人表
      */
    val bPolicyPreservationSubjectPersonMasterBzncenOne: DataFrame =
      sqlContext.sql("select * from sourcedb.b_policy_preservation_subject_person_master_bzncen")
      .selectExpr("inc_dec_order_no as master_inc_dec_order_no","policy_no as master_policy_no","start_date as pre_start_date","end_date as pre_end_date")

    /**
      * 增加两个字段 为后面关联用
      */
    val bPolicyPreservationSubjectPersonMasterBzncen = bPolicyPreservationSubjectPersonMasterBzncenOne
      .withColumn("temp_policy_no",bPolicyPreservationSubjectPersonMasterBzncenOne("master_policy_no"))
      .withColumn("temp_inc_dec_order_no",bPolicyPreservationSubjectPersonMasterBzncenOne("master_inc_dec_order_no"))

    /**
      * 保全与保单关联得到保单号
      */
    val bPolicyInfo = bPolicyPreserveBznprd.join(bPolicyBzncen,bPolicyPreserveBznprd("temp_policy_no") ===bPolicyBzncen("b_policy_no"),"leftouter")
      .selectExpr("preserve_id","policy_no","insurance_policy_no","temp_policy_no","policy_id","inc_dec_order_no","temp_inc_dec_order_no","add_batch_code","add_premium","add_person_count","del_batch_code","del_premium","del_person_count","preserve_type","pay_status","create_time","update_time")

    /**
      * 上结果与保全人员清单表关联  得到 保全生效的开始时间和结束时间
      */
    val bPolicyInfoMasterInfo = bPolicyInfo.join(bPolicyPreservationSubjectPersonMasterBzncen,Seq("temp_policy_no", "temp_inc_dec_order_no"),"leftouter")
      .selectExpr("preserve_id","policy_no","insurance_policy_no","policy_id","inc_dec_order_no" ,"add_batch_code","add_premium","add_person_count","del_batch_code","del_premium","del_person_count","pre_start_date","pre_end_date","preserve_type","pay_status","create_time","update_time")
      .distinct()
      .registerTempTable("bPolicyInfoMasterInfoTemp")

    val tep_four = sqlContext.sql("select inc_dec_order_no,pre_start_date,pre_end_date from bPolicyInfoMasterInfoTemp")
      .map(x=> {
        val preStartDate = x.getAs[Timestamp]("pre_start_date")
        val preEndDate = x.getAs[Timestamp]("pre_end_date")
        val one = if (preStartDate == "null" || preStartDate == null) {
          if(preEndDate !=null){
            currentTimeL(preEndDate.toString.substring(0, 19)).toDouble
          }else{
            0.0
          }
        } else {
          currentTimeL(preStartDate.toString.substring(0, 19)).toDouble
        }
        val two = if (preEndDate == "null" || preEndDate == null) {
          if(preStartDate != null){
            currentTimeL(preStartDate.toString.substring(0, 19)).toDouble
          }else{
            0.0
          }
        } else {
          currentTimeL(preEndDate.toString.substring(0, 19)).toDouble
        }
        (x.getAs[String]("inc_dec_order_no"), (one, two))
      })
      .reduceByKey((x1, x2) => {
        val one = if (x1._1 >= x2._1) x1._1 else x2._1
        val two = if (x1._2 <= x2._2) x1._2 else x2._2
        (one, two)
      })
      .map(x => {
        var one = ""
        var two = ""
        if(x._2._1.toLong > 0){
          one = get_current_date(x._2._1.toLong)
        }else{
          one = null
        }
        if(x._2._2.toLong > 0){
          two = get_current_date(x._2._2.toLong)
        }else{
          two = null
        }
        (x._1, one, two)
      })
      .toDF("temp_inc_dec_order_no", "pre_start_date", "pre_end_date")

    val tep_five = tep_four.join(bPolicyPreserveBznprdTemp,"temp_inc_dec_order_no").map(x => {
      val tempIncDecOrderNo = x.getAs[String]("temp_inc_dec_order_no")
      var preStartDate = x.getAs[String]("pre_start_date")
      var preEndDate = x.getAs[String]("pre_end_date")
      val effectiveDate = x.getAs[Long]("effective_date").toString
      val addPersonCount = x.getAs[Int]("add_person_count")
      val delPersonCount = x.getAs[Int]("del_person_count")
      val preserveType = x.getAs[Int]("preserve_type")

      var preserveEffectDate = ""
      // 生效日期：如果是纯减员  结束时间+1 去前十位  如果是增减员就得到开始时间的前十位，如果是退保使用时effect_date得到生效日期
      if(addPersonCount == 0 && delPersonCount > 0){
        if(preEndDate!=null && preEndDate.length >18){
          preEndDate = dateAddOneDay(preEndDate)
          preserveEffectDate = preEndDate.substring(0,10).replaceAll("-","")
        }else{
          preserveEffectDate = null
        }
      }else if(preserveType != 5){ //不是纯减员 并且不是退保情况 去开始时间为生效时间
        if(preStartDate!=null && preStartDate.length >18){
          preserveEffectDate = preStartDate.substring(0,10).replaceAll("-","")
        }else{
          preserveEffectDate = null
        }
      }else{//5 状态为退保
        if(effectiveDate!=null && effectiveDate.length >18){
          preserveEffectDate = effectiveDate.substring(0,10).replaceAll("-","")
        }else{
          preserveEffectDate = null
        }
      }

      (tempIncDecOrderNo,preserveEffectDate)
    })
    .toDF("temp_inc_dec_order_no", "preserve_effect_date")

    val resTemp = sqlContext.sql("select * from bPolicyInfoMasterInfoTemp")
      .selectExpr("preserve_id","insurance_policy_no as policy_code","policy_id","inc_dec_order_no" ,"add_batch_code","add_premium","add_person_count","del_batch_code","del_premium","del_person_count","preserve_type","pay_status","create_time","update_time")

    val res = resTemp.join(tep_five,resTemp("inc_dec_order_no") ===tep_five("temp_inc_dec_order_no"),"leftouter")
      .selectExpr("preserve_id","policy_id","policy_code","add_batch_code","add_premium","add_person_count","del_batch_code","del_premium",
          "del_person_count","preserve_effect_date","preserve_type","pay_status","create_time","update_time","getNow() as dw_create_time")
      .distinct()
      .selectExpr("getUUID() as id","preserve_id","policy_id","policy_code","'1' as preserve_status","add_batch_code","add_premium","add_person_count","del_batch_code","del_premium",
        "del_person_count","preserve_effect_date","case when preserve_type = 1 then 1 when preserve_type = 2 then 2 when preserve_type = 5 then 3 else -1 end as preserve_type",
        "case when pay_status = 1 then 1 when pay_status = 2 then 0 else -1 end pay_status","create_time","update_time","getNow() as dw_create_time")
    res
  }

  /**
    * 1.0系统保全信息
    * @param sqlContext
    */
  def onePreservationDetail(sqlContext:HiveContext) ={
    import sqlContext.implicits._

    sqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    sqlContext.udf.register("getDefault", () => {
      val str = ""
      str
    })
    sqlContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")//设置日期格式
      val date = df.format(new Date())// new Date()为获取当前系统时间
      (date + "")
    })


    /**
      * 读取保全表
      */
    val plcPolicyPreserveBznprd = readMysqlTable(sqlContext,"plc_policy_preserve_bznprd")
      .selectExpr("id as preserve_id","policy_id","policy_code","status","add_batch_code","add_premium","add_person_count","del_batch_code","del_premium","del_person_count","start_date","end_date","type as preserve_type","create_time","update_time")
      .registerTempTable("plcPolicyPreserveBznprdTemp")

    /**
      * 创建保全表临时表
      * 使用保全表的开始时间和结束时间以及增员人数和减员人数得到保全生效日期
      */
    val plcPolicyPreserveBznprdTemp = sqlContext.sql("select preserve_id,add_person_count,del_person_count,start_date,end_date from plcPolicyPreserveBznprdTemp")
      .map(x => {
        val preserveId = x.getAs[String]("preserve_id")
        val addPersonCount = x.getAs[Int]("add_person_count")
        val delPersonCount = x.getAs[Int]("del_person_count")
        val startDate = x.getAs[Timestamp]("start_date")
        val endDate = x.getAs[Timestamp]("end_date")
        var preserve_effect_date = ""
        if(addPersonCount == 0 && delPersonCount >0) {
          if(endDate != null && endDate.toString.length >0){
            preserve_effect_date = endDate.toString.substring(0,10).replaceAll("-","")
          }else if(startDate!=null  && startDate.toString.length>0){
            preserve_effect_date = startDate.toString.substring(0,10).replaceAll("-","")
          }else{
            preserve_effect_date = null
          }
        }else {
          if(startDate!=null  && startDate.toString.length>0){
            preserve_effect_date = startDate.toString.substring(0,10).replaceAll("-","")
          }else if(endDate != null && endDate.toString.length >0){
            preserve_effect_date = endDate.toString.substring(0,10).replaceAll("-","")
          }else{
            preserve_effect_date = null
          }
        }
        (preserveId,preserve_effect_date)
      })
      .toDF("preserve_id_temp","preserve_effect_date")

    val plcPolicyPreserveBznprdResTemp = sqlContext.sql("select * from plcPolicyPreserveBznprdTemp")
    val plcPolicyPreserveBznprdRes = plcPolicyPreserveBznprdResTemp.join(plcPolicyPreserveBznprdTemp,plcPolicyPreserveBznprdResTemp("preserve_id") ===plcPolicyPreserveBznprdTemp("preserve_id_temp"),"leftouter")
      .selectExpr("preserve_id","policy_id","policy_code","status ","add_batch_code","add_premium","add_person_count","del_batch_code","del_premium","del_person_count","preserve_effect_date","preserve_type","create_time","update_time")

    /**
      * 读取2.0保单表 如果1.0保单在2.0保单表中有数据，以2.0的保单id为准
      */
    val bPolicyBzncen = readMysqlTable(sqlContext,"b_policy_bzncen")
      .selectExpr("id","insurance_policy_no")

    val res = plcPolicyPreserveBznprdRes.join(bPolicyBzncen,plcPolicyPreserveBznprdRes("policy_code")===bPolicyBzncen("insurance_policy_no"),"leftouter")
      .distinct()
      .selectExpr("getUUID() as id","preserve_id","case when id is null then policy_id else id end as policy_id","policy_code",
        "case when status in (4,5) then 1 when status = 6 then 0 else -1 end as preserve_status",
        "add_batch_code","add_premium","add_person_count","del_batch_code","del_premium","del_person_count","preserve_effect_date",
        "case when preserve_type = 1 then 1 when preserve_type = 2 then 2 else -1 end as preserve_type",
        "case when getDefault() = '' then null end as pay_status","create_time","update_time","getNow() as dw_create_time")
    res
  }

  /**
    * 获取 Mysql 表的数据
    * @param sqlContext
    * @param tableName 读取Mysql表的名字
    * @return 返回 Mysql 表的 DataFrame
    */
  def readMysqlTable(sqlContext: SQLContext, tableName: String): DataFrame = {
    val properties: Properties = getProPerties()
    sqlContext
      .read
      .format("jdbc")
      .option("url", properties.getProperty("mysql.url.106"))
      .option("driver", properties.getProperty("mysql.driver"))
      .option("user", properties.getProperty("mysql.username.106"))
      .option("password", properties.getProperty("mysql.password.106"))
      .option("numPartitions","10")
      .option("partitionColumn","id")
      .option("lowerBound", "0")
      .option("upperBound","200")
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
