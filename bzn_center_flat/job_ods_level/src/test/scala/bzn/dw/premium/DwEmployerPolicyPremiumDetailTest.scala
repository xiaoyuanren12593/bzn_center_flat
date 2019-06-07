package bzn.dw.premium

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import bzn.ods.util.{SparkUtil, Until}
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * author:xiaoYuanRen
  * Date:2019/6/7
  * Time:10:40
  * describe: dw层  出单保费明细表
  **/
object DwEmployerPolicyPremiumDetailTest extends SparkUtil with Until{
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName,"")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    DwPolicyPremiumDetail(hiveContext)
//    res.write.mode(SaveMode.Overwrite).saveAsTable("odsdb.ods_preservation_detail")
    sc.stop()
  }

  /**
    * 出单保费
    * @param sqlContext
    */
  def DwPolicyPremiumDetail(sqlContext:HiveContext) ={
    import sqlContext.implicits._
    sqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    sqlContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")//设置日期格式
      val date = df.format(new Date())// new Date()为获取当前系统时间
      (date + "")
    })
    /**
      * 读取保单明细表
      */
    val odsPolicyDetail =
      sqlContext.sql("select policy_id,policy_code,product_code,policy_status,first_premium,preserve_policy_no," +
        "num_of_preson_first_policy,policy_start_date from odsdb.ods_policy_detail")
      .cache()

    /**
      * 读取保全明细表
      */
    val odsPreseveDetail =
      sqlContext.sql("select preserve_id,policy_id as policy_id_preserve,add_person_count,add_premium,del_person_count," +
        "del_premium,preserve_effect_date,preserve_status,preserve_type from odsdb.ods_preservation_detail")
      .cache()

    /**
      * 读取产品表
      */
    val odsProductDetail =
      sqlContext.sql("select product_code,one_level_pdt_cate from odsdb.ods_product_detail")
      .where("one_level_pdt_cate = '蓝领外包'")

    /**
      * 保单新投保费和续投保费
      */
    val newPolicy = odsPolicyDetail.join(odsProductDetail,odsPolicyDetail("product_code") ===odsProductDetail("product_code"))
      .where("policy_status in (0,1) and preserve_policy_no is null")
      .map(x => {
        val policyId = x.getAs[String]("policy_id")
        val policyCode = x.getAs[String]("policy_code")
        val firstPremium = x.getAs[Double]("first_premium")
        val preservePolicyNo = x.getAs[String]("preserve_policy_no")
        var policyStartDate = x.getAs[Timestamp]("policy_start_date")
        var policyStartDateRes = ""
        if(policyStartDate != null && policyStartDate.toString.length > 10){
          policyStartDateRes = policyStartDate.toString.substring(0,10).replaceAll("-","")
        }else{
          policyStartDateRes = null
        }
        val numOfPresonFirstPolicy = x.getAs[Int]("num_of_preson_first_policy")

        (policyId,policyCode,preservePolicyNo,4,firstPremium,numOfPresonFirstPolicy,0.0,0,policyStartDateRes)
      })
      .toDF("policy_id","policy_code","preserve_id","premium_type","add_premium","add_person_count","del_premium","del_person_count","day_id")

    /**
      * 续投
      */
    val renewPolicy = odsPolicyDetail.join(odsProductDetail,odsPolicyDetail("product_code") ===odsProductDetail("product_code"))
      .where("policy_status in (0,1) and length(preserve_policy_no) > 0")
      .map(x => {
        val policyId = x.getAs[String]("policy_id")
        val policyCode = x.getAs[String]("policy_code")
        val firstPremium = x.getAs[Double]("first_premium")
        val preservePolicyNo = x.getAs[String]("preserve_policy_no")
        val numOfPresonFirstPolicy = x.getAs[Int]("num_of_preson_first_policy")
        var policyStartDate = x.getAs[Timestamp]("policy_start_date")
        var policyStartDateRes = ""
        if(policyStartDate != null && policyStartDate.toString.length > 10){
          policyStartDateRes = policyStartDate.toString.substring(0,10).replaceAll("-","")
        }else{
          policyStartDateRes = null
        }
        (policyId,policyCode,preservePolicyNo,2,firstPremium,numOfPresonFirstPolicy,0.0,0,policyStartDateRes)
      })
      .toDF("policy_id","policy_code","preserve_id","premium_type","add_premium","add_person_count","del_premium","del_person_count","day_id")

    /**
      * 保单中的退保
      */
    val cancelPolicy = odsPolicyDetail.join(odsProductDetail,odsPolicyDetail("product_code") ===odsProductDetail("product_code"))
      .where("policy_status = -1")
      .map(x => {
        val policyId = x.getAs[String]("policy_id")
        val policyCode = x.getAs[String]("policy_code")
        var delPremium = x.getAs[Double]("first_premium")
        if(delPremium != null){
          delPremium = 0.0-delPremium
        }
        val preservePolicyNo = x.getAs[String]("preserve_policy_no")
        val numOfPresonFirstPolicy = x.getAs[Int]("num_of_preson_first_policy")
        var policyStartDate = x.getAs[Timestamp]("policy_start_date")
        var policyStartDateRes = ""
        if(policyStartDate != null && policyStartDate.toString.length > 10){
          policyStartDateRes = policyStartDate.toString.substring(0,10).replaceAll("-","")
        }else{
          policyStartDateRes = null
        }
        (policyId,policyCode,preservePolicyNo,3,0.0,0,delPremium,numOfPresonFirstPolicy,policyStartDateRes)
      })
      .toDF("policy_id","policy_code","preserve_id","premium_type","add_premium","add_person_count","del_premium","del_person_count","day_id")

    /**
      * 保全中的续投
      */
    val preserveReNewPremiumTemp = odsPolicyDetail.join(odsPreseveDetail,odsPolicyDetail("policy_id") ===odsPreseveDetail("policy_id_preserve"))
      .where("preserve_status = 1 and preserve_type = 2")
      .selectExpr("policy_id","policy_code","preserve_id","add_premium","add_person_count","del_premium","del_person_count",
        "product_code","preserve_effect_date","preserve_type")

    val preserveReNewPremium = preserveReNewPremiumTemp.join(odsProductDetail,preserveReNewPremiumTemp("product_code") ===odsProductDetail("product_code"))
      .selectExpr("policy_id","policy_code","preserve_id","preserve_type as premium_type","add_premium","add_person_count","del_premium",
        "del_person_count","preserve_effect_date as day_id")

    /**
      * 保全中的增减员
      */
    val preserveAddAndDelPremiumTemp = odsPolicyDetail.join(odsPreseveDetail,odsPolicyDetail("policy_id") ===odsPreseveDetail("policy_id_preserve"))
      .where("preserve_status = 1 and preserve_type = 1")
      .selectExpr("policy_id","policy_code","preserve_id","add_premium","add_person_count","del_premium","del_person_count",
        "product_code","preserve_effect_date","preserve_type")

    val preserveAddAndDelPremium = preserveAddAndDelPremiumTemp.join(odsProductDetail,preserveAddAndDelPremiumTemp("product_code") ===odsProductDetail("product_code"))
      .selectExpr("policy_id","policy_code","preserve_id","1 as premium_type","add_premium","add_person_count","del_premium",
        "del_person_count","preserve_effect_date as day_id")

    /**
      * 保全类型中的退保
      */
    val preserveCancelPremiumTemp = odsPolicyDetail.join(odsPreseveDetail,odsPolicyDetail("policy_id") ===odsPreseveDetail("policy_id_preserve"))
      .where("preserve_status = 1 and preserve_type = 3")
      .selectExpr("policy_id","policy_code","preserve_id","add_premium","add_person_count","del_premium","del_person_count",
        "product_code","preserve_effect_date","preserve_type")

    val preserveCancelPremium = preserveCancelPremiumTemp.join(odsProductDetail,preserveCancelPremiumTemp("product_code") ===odsProductDetail("product_code"))
      .selectExpr("policy_id","policy_code","preserve_id","preserve_type as premium_type","add_premium","add_person_count","del_premium",
        "del_person_count","preserve_effect_date as day_id")

   val res = newPolicy.unionAll(renewPolicy).unionAll(cancelPolicy).unionAll(preserveReNewPremium).unionAll(preserveAddAndDelPremium).unionAll(preserveCancelPremium)
      .selectExpr("getUUID() as id","policy_id","policy_code","preserve_id","premium_type","add_premium","add_person_count","del_premium",
        "del_person_count","day_id","getNow() as dw_create_time")
    res.printSchema()
   res
  }
}

