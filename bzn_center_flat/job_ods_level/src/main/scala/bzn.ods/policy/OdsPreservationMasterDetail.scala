package bzn.ods.policy

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import bzn.job.common.Until
import bzn.ods.util.SparkUtil
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * author:xiaoYuanRen
  * Date:2019/5/28
  * Time:10:24
  * describe: this is new class
  **/
object OdsPreservationMasterDetail extends SparkUtil with Until{
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName,"")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    val oneRes = onePreservetionMasterDetail(hiveContext)
    val twoRes = twoPreservetionMasterDetail(hiveContext)
    val res = oneRes.unionAll(twoRes)
    res.cache()

    hiveContext.sql("truncate table odsdb.ods_preservation_master_detail")
    res.repartition(10).write.mode(SaveMode.Append).saveAsTable("odsdb.ods_preservation_master_detail")
    res.repartition(1).write.mode(SaveMode.Overwrite).parquet("/dw_data/ods_data/OdsPreservationMasterDetail")

    sc.stop()
  }

  /**
    * 2.0保全人员清单明细表
    */
  def twoPreservetionMasterDetail(sqlContext:HiveContext): DataFrame ={
    import sqlContext.implicits._
    udfUtil(sqlContext)
    sqlContext.sql("select * from sourcedb.b_policy_preservation_subject_person_master_bzncen")
      .selectExpr("id","inc_dec_order_no","policy_no","name","sex as gender","cert_type as insured_cert_type","cert_no","birthday","industry_name as industry","work_type","company_name",
        "company_phone","status","start_date","end_date","create_time","update_time")
      .registerTempTable("bPolicyPreservationSubjectPersonMasterBzncenTemp")

    /**
      * 去掉工种和换行符中的空格
      */
    sqlContext.sql("select  regexp_replace(work_type,'\\n','') as work_type_new ,regexp_replace(cert_no,'\\n','') as insured_cert_no,* from bPolicyPreservationSubjectPersonMasterBzncenTemp")
      .drop("cert_no")
      .drop("work_type")
      .withColumnRenamed("work_type_new","work_type")
      .registerTempTable("bPolicyPreservationSubjectPersonMasterBzncenNew")

    /**
      * 更改在保人被保人状态0在职  1不在职
      */
    val bPolicyPreservationSubjectPersonMasterBzncenTwo: DataFrame = sqlContext.sql("select " +
      "*,CASE WHEN `status` = 1" +
      "    THEN 0" +
      "    ELSE 1" +
      "  END as insured_status from bPolicyPreservationSubjectPersonMasterBzncenNew")

    /**
      * 去掉工种和姓名中非中文字符
      */
    val fields_name = bPolicyPreservationSubjectPersonMasterBzncenTwo.schema.map(x => x.name) :+ "work_type_new" :+ "name_new"
    val bPolicyPreservationSubjectPersonMasterBzncenThree = bPolicyPreservationSubjectPersonMasterBzncenTwo.map(x => {
      val work = to_null(x.getAs[String]("work_type"))
      val name = to_null(x.getAs[String]("name"))
      (x.toSeq :+ work :+ name).map(x => if (x == null) null else x.toString)
    })
    /**
      * 重新dataframe
      */
    val value = bPolicyPreservationSubjectPersonMasterBzncenThree.map(r => Row(r: _*))
    val schema = StructType(fields_name.map(fieldName => StructField(fieldName, StringType, nullable = true)))
    val bPolicyPreservationSubjectPersonMasterBzncenFour = sqlContext.createDataFrame(value, schema)
    bPolicyPreservationSubjectPersonMasterBzncenFour
      .drop("work_type")
      .drop("insured_name")
      .registerTempTable("bPolicyPreservationSubjectPersonMasterBzncenFiveTemp")

    val bPolicyPreservationSubjectPersonMasterBzncen = sqlContext.sql("select * from bPolicyPreservationSubjectPersonMasterBzncenFiveTemp")
    /**
      * 读取保全表
      */
    val bPolicyPreservationBzncen = readMysqlTable(sqlContext,"b_policy_preservation_bzncen")
      .selectExpr("id as preserve_id","inc_dec_order_no","policy_no","preservation_type as preserve_type","inc_revise_sum as add_person_count","dec_revise_sum as del_person_count",
        "create_time as create_time_master")

    /**
      * 将保单表和保全表进行关联
      */
    bPolicyPreservationSubjectPersonMasterBzncen.join(bPolicyPreservationBzncen,Seq("inc_dec_order_no","policy_no"),"leftouter")
      .selectExpr("inc_dec_order_no","preserve_id","id as master_id","name as insured_name","gender","insured_cert_type","insured_cert_no","birthday",
        "industry","work_type_new as work_type","company_name","company_phone","preserve_type",
        "start_date","end_date","insured_status","case when insured_cert_type ='1' and start_date is not null then getAgeFromBirthTime(insured_cert_no,start_date) else null end as age",
        "create_time","update_time")
      .registerTempTable("res_temp")

    /**
      * 将每个增减员批单中开始时间选择最大，结束时间选择最小，如果开始时间是null  就把结束时间给它  结束时间亦然
      */
    val resTemp = sqlContext.sql("select inc_dec_order_no,start_date,end_date from res_temp")
      .map(x=> {
        val preStartDate = x.getAs[String]("start_date")
        val preEndDate = x.getAs[String]("end_date")
        val one = if (preStartDate == "" || preStartDate == null) {
          if(preEndDate !=null){
            currentTimeL(preEndDate.toString.substring(0, 19)).toDouble
          }else{
            0.0
          }
        } else {
          currentTimeL(preStartDate.toString.substring(0, 19)).toDouble
        }
        val two = if (preEndDate == "" || preEndDate == null) {
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

    val bPolicyPreserveTemp = bPolicyPreservationBzncen.selectExpr("inc_dec_order_no","preserve_type","add_person_count","del_person_count","create_time_master")
    val bPolicyPreserveTempRes = resTemp.join(bPolicyPreserveTemp,resTemp("temp_inc_dec_order_no")===bPolicyPreserveTemp("inc_dec_order_no"))
      .map(x => {
        val tempIncDecOrderNo = x.getAs[String]("temp_inc_dec_order_no")
        val preStartDate = x.getAs[String]("pre_start_date")
        var preStartDateRes = ""
        val preEndDate =  x.getAs[String]("pre_end_date")
        var preEndDateRes =  ""
        val addPersonCount = x.getAs[Int]("add_person_count")
        val delPersonCount = x.getAs[Int]("del_person_count")
        val createTimeMaster = x.getAs[java.sql.Timestamp]("create_time_master")
        val preserveType = x.getAs[Int]("preserve_type")

        if(preStartDate != null && preStartDate.length >18){
          preStartDateRes =  preStartDate
        }else{
          preStartDateRes = null
        }

        if(preEndDate != null && preEndDate.length >18){
          preEndDateRes =  preEndDate
        }else{
          preEndDateRes = null
        }

        // 生效日期：如果是纯减员  结束时间+1 去前十位  如果是增减员就得到开始时间的前十位，如果是退保使用时 创建时间 得到生效日期
        if(addPersonCount == 0 && delPersonCount > 0){
          preStartDateRes = null
          if(preEndDate!=null && preEndDate.length >18){
            preEndDateRes = dateAddOneDay(preEndDate)
          }else{
            preEndDateRes = null
          }
        }
        //退保情况
        if(preserveType == 5){
          if(createTimeMaster != null){
            preStartDateRes = createTimeMaster.toString
            preEndDateRes = createTimeMaster.toString
          }else{
            preStartDateRes = null
            preEndDateRes = null
          }
        }
        (tempIncDecOrderNo,preStartDateRes,preEndDateRes)
      })
      .toDF("temp_inc_dec_order_no","pre_start_date","pre_end_date")

    val resTempTwo = sqlContext.sql("select * from res_temp")
    val res = resTempTwo.join(bPolicyPreserveTempRes,resTempTwo("inc_dec_order_no")===bPolicyPreserveTempRes("temp_inc_dec_order_no"),"leftouter")
      .selectExpr("getUUID() as id","clean(master_id) as master_id","clean(cast(preserve_id as String)) as preserve_id","clean(insured_name) as insured_name",
        "case when gender = 2 then 0 else 1 end as gender","case when insured_cert_type = 1 then 1 else -1 end as insured_cert_type",
        "clean(insured_cert_no) as insured_cert_no","clean(birthday) as birthday","clean(industry) as industry","clean(work_type) as work_type","clean(company_name) as company_name",
        "clean(company_phone) as company_phone","case when preserve_type = 1 then 1 when preserve_type = 2 then 2 when preserve_type = 5 then 3 else -1 end as preserve_type",
        "pre_start_date","pre_end_date","cast(clean(insured_status) as int) as insured_status",
        "age","cast(getDate(create_time) as timestamp) as create_time","cast(getDate(update_time) as timestamp) as update_time","getNow() as dw_create_time")

    res
  }

  /**
    * 1.0 保全人员清单明细表
    * @param sqlContext 上下文
    */
  def onePreservetionMasterDetail(sqlContext:HiveContext) :DataFrame = {
    import sqlContext.implicits._
    udfUtil(sqlContext)
    /**
      * 读取被保人信息表
      */
    val plcPolicyPreserveInsuredBznprdOne =
      sqlContext.sql("select * from sourcedb.plc_policy_preserve_insured_bznprd")
        .where("remark != 'obsolete' or remark is null")
        .selectExpr("id as master_id","preserve_id","name as insured_name","gender","cert_type as insured_cert_type","cert_no","birthday","industry",
          "work_type","company_name","company_phone","status as insured_status","join_date","left_date","create_time","update_time")
    plcPolicyPreserveInsuredBznprdOne.registerTempTable("plcPolicyPreserveInsuredBznprdTemp")
    /**
      * 去掉工种和身份证号中的换行符
      */
    sqlContext.sql("select  regexp_replace(work_type,'\\n','') as work_type_new ,regexp_replace(cert_no,'\\n','') as insured_cert_no,* from plcPolicyPreserveInsuredBznprdTemp")
      .drop("cert_no")
      .drop("work_type")
      .withColumnRenamed("work_type_new", "work_type")
      .registerTempTable("plcPolicyPreserveInsuredBznprdNew")
    val plcPolicyPreserveInsuredBznprdTwo = sqlContext.sql("select * from plcPolicyPreserveInsuredBznprdNew")

    val fields_name = plcPolicyPreserveInsuredBznprdTwo.schema.map(x => x.name) :+ "work_type_new" :+ "name_new"
    val plcPolicyPreserveInsuredBznprdThree = plcPolicyPreserveInsuredBznprdTwo.map(x => {
      val work = to_null(x.getAs[String]("work_type"))
      val name = to_null(x.getAs[String]("insured_name"))
      (x.toSeq :+ work :+ name).map(x => if (x == null) null else x.toString)
    })
    val value = plcPolicyPreserveInsuredBznprdThree.map(r => Row(r: _*))
    val schema = StructType(fields_name.map(fieldName => StructField(fieldName, StringType, nullable = true)))
    val plcPolicyPreserveInsuredBznprdFour = sqlContext.createDataFrame(value, schema)

    plcPolicyPreserveInsuredBznprdFour
      .withColumn("name", plcPolicyPreserveInsuredBznprdFour("name_new"))
      .drop("work_type")
      .drop("insured_name")
      .registerTempTable("plcPolicyPreserveBznprdFiveTemp")

    val plcPolicyPreserveInsuredBznprd = sqlContext.sql("select * from plcPolicyPreserveBznprdFiveTemp")

    /**
      * 人员明细的最大开始时间和最小结束时间
      */
    val maxStartDateMinEndDate = sqlContext.sql("select preserve_id as preserve_id_insured,join_date,left_date from plcPolicyPreserveBznprdFiveTemp")
      .map(x => {
        val preserveIdInsured = x.getAs[String]("preserve_id_insured")
        val joinDate = x.getAs[String]("join_date")
        val leftDate = x.getAs[String]("left_date")
        var joinDateRes = "0"
        if(joinDate != null){
          joinDateRes = currentTimeL(joinDate.substring(0,19)).toString
        }else{
          if(leftDate != null){
            joinDateRes = currentTimeL(leftDate.substring(0,19)).toString
          }
        }
        var leftDateRes = "0"
        if(leftDate != null){
          leftDateRes = currentTimeL(leftDate.substring(0,19)).toString
        }else{
          if(joinDate != null){
            leftDateRes = currentTimeL(joinDate.substring(0,19)).toString
          }
        }
        (preserveIdInsured,(joinDateRes,leftDateRes))
      })
      .reduceByKey((x1,x2) => {
        val joinDateRes = x1._1+"\u0001"+x2._1
        val leftDateRes = x1._2+"\u0001"+x2._2
        (joinDateRes,leftDateRes)
      })
      .map(x => {
        //        get_current_date
        val joinDate: List[String] = x._2._1.split("\u0001").distinct.toList.sorted.reverse//降序
        var joinDateRes = ""
        if(joinDate.nonEmpty && joinDate.head !="0"){
          joinDateRes = get_current_date(joinDate.head.toLong)
        }else if (joinDate.isEmpty){
          joinDateRes = null
        }else{
          joinDateRes = null
        }

        val leftDate: List[String] = x._2._2.split("\u0001").distinct.toList.sorted//降序
        var leftDateRes = ""
        if(leftDate.nonEmpty && leftDate.head!="0"){
          leftDateRes = get_current_date(leftDate.head.toLong)
        }else if (leftDate.isEmpty){
          leftDateRes = null
        }else{
          leftDateRes = null
        }
        (x._1,joinDateRes,leftDateRes)
      })
      .toDF("preserve_id_insured","joinDateRes","leftDateRes")
      .distinct()

    /**
      * 读取保全表  如果保险开始时间为空就把结束时间赋值给他 如果结束时间为空  就把开始时间赋值给结束时间
      */
    val plcPolicyPreserveBznprd = readMysqlTable(sqlContext,"plc_policy_preserve_bznprd")
      .selectExpr("id","type as preserve_type","add_person_count","del_person_count")

    val temp = plcPolicyPreserveBznprd.join(maxStartDateMinEndDate,plcPolicyPreserveBznprd("id")===maxStartDateMinEndDate("preserve_id_insured"))
      .map(x=> {
        val preserveId = x.getAs[String]("id")
        val preserveType = x.getAs[Int]("preserve_type")
        val addPersonCount = x.getAs[Int]("add_person_count")
        val delPersonCount = x.getAs[Int]("del_person_count")
        val startDate = x.getAs[String]("joinDateRes")
        var endDate = x.getAs[String]("leftDateRes")
        /**
          * 纯减员情况下  结束时间 +1
          */
        if(addPersonCount ==0 && delPersonCount >0){
          if(endDate != null){
            endDate = dateAddOneDay(endDate.toString)
          }
        }
        (preserveId,preserveType,startDate,endDate)
      })
      .toDF("preserve_id_temp","preserve_type","pre_start_date","pre_end_date")

    val res = plcPolicyPreserveInsuredBznprd.join(temp,plcPolicyPreserveInsuredBznprd("preserve_id")===temp("preserve_id_temp") ,"leftouter")
      .selectExpr("getUUID() as id","clean(master_id) as master_id","clean(preserve_id) as preserve_id","clean(name) as insured_name","cast(clean(gender) as int) as gender",
        "case when insured_cert_type = 1 then 1 else -1 end as insured_cert_type","clean(insured_cert_no) as insured_cert_no","clean(birthday) as birthday","clean(industry) as industry",
        "clean(work_type_new) as work_type", "clean(company_name) as company_name","clean(company_phone) as company_phone","case when preserve_type = 1 then 1 when preserve_type = 2 then 2 else -1 end as preserve_type",
        "pre_start_date","pre_end_date","case when insured_status = 0 then 0 when insured_status = 1 then 1 else null end as insured_status",
        "case when insured_cert_type ='1' and pre_start_date is not null then getAgeFromBirthTime(insured_cert_no,pre_start_date) else null end as age",
        "cast(getDate(create_time) as timestamp) as create_time","cast(getDate(update_time) as timestamp) as update_time","getNow() as dw_create_time")

    res
  }

  /**
    * hive 自定义 udf函数
    * @param sqlContext 上下文
    * @return
    */
  def udfUtil(sqlContext:HiveContext): UserDefinedFunction = {
    sqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    sqlContext.udf.register("clean", (str: String) => clean(str))
    sqlContext.udf.register("getDate", (time:String) => timeSubstring(time))
    sqlContext.udf.register("getDefault", () => {
      val str = ""
      str
    })
    sqlContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")//设置日期格式
      val date = df.format(new Date())// new Date()为获取当前系统时间
      date + ""
    })

    sqlContext.udf.register("getAgeFromBirthTime", (cert_no: String, end: String) => getAgeFromBirthTime(cert_no, end))
  }

  /**
    * 获取 Mysql 表的数据
    * @param sqlContext 上下文
    * @param tableName 读取Mysql表的名字
    * @return 返回 Mysql 表的 DataFrame
    */
  def readMysqlTable(sqlContext: SQLContext, tableName: String): DataFrame = {
    val properties: Properties = getProperties
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
    */
  def getProperties : Properties = {
    val lines_source = Source.fromURL(getClass.getResource("/config_scala.properties")).getLines.toSeq
    val properties: Properties = new Properties()
    for (elem <- lines_source) {
      val split = elem.split("==")
      val key = split(0)
      val value = split(1)
      properties.setProperty(key,value)
    }
    properties
  }
}
