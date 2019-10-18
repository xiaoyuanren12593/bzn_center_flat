package bzn.dm.bclickthrough

import java.text.SimpleDateFormat
import java.util.Date

import bzn.dm.util.SparkUtil
import bzn.job.common.{MysqlUntil, Until}
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * author:xiaoYuanRen
  * Date:2019/10/10
  * Time:15:27
  * describe: 雇主续保功能
  **/
object DmEmployerPolicyContinueDetail extends SparkUtil with Until with MysqlUntil{
    def main (args: Array[String]): Unit = {
        System.setProperty ("HADOOP_USER_NAME", "hdfs")
        val appName = this.getClass.getName
        val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo (appName, "")

        val sc = sparkConf._2
        val hiveContext = sparkConf._4
        val res = DmEmployerPolicyContinueDetail(hiveContext)
        hiveContext.sql("truncate table dmdb.dm_b_clickthrouth_emp_policy_continue_Detail")
        res.repartition(10).write.mode(SaveMode.Append).saveAsTable("dmdb.dm_b_clickthrouth_emp_policy_continue_Detail")
        sc.stop ()
    }

    /**
      * 雇主续投保单明细数据清洗
      * @param sqlContext
      */
    def DmEmployerPolicyContinueDetail(sqlContext:HiveContext) = {
      import sqlContext.implicits._
      sqlContext.udf.register ("getUUID", () => (java.util.UUID.randomUUID () + "").replace ("-", ""))
      sqlContext.udf.register ("getNow", () => {
        val df = new SimpleDateFormat ("yyyy-MM-dd HH:mm:ss")
        //设置日期格式
        val date = df.format (new Date ()) // new Date()为获取当前系统时间
        date + ""
      })

      sqlContext.udf.register ("getDayId", () => {
        /**
          * 当前时间的day_id
          */
        val nowDayId = getNowTime().substring(0,10).replaceAll("-","")
        nowDayId
      })

      /**
        * 读取雇主基础数据人员明细表
        */
      val dwEmployerBaseinfoPersonDetail = sqlContext.sql("select * from dwdb.dw_employer_baseinfo_person_detail")
        .where("policy_status in (0,1) and sku_charge_type = '2'")

      /**
        * 读取时间维度数据
        */
      val odsTDayDimension =
        sqlContext.sql("select d_day,c_day_id,c_week_long_desc,c_month_long_desc,c_quarter_long_desc from odsdb.ods_t_day_dimension")

      /**
        * 读取当前在保人表
        */
      val dwolicyCurrInsuredDetail =
        sqlContext.sql("select policy_id,day_id,count from dwdb.dw_policy_curr_insured_detail")

      /**
        * 根据雇主基础数据人员明细数据，得到应续保日期，实际续保参照日期，应续保参照日期，当前在保的day_id，应续保参照日期的day_id
        */
      val dwEmployerBaseinfoPersonDetailLeftRes = dwEmployerBaseinfoPersonDetail
        .selectExpr("policy_id","holder_name","policy_start_date","policy_end_date","start_date","end_date","insured_cert_no")
        .map(x => {
          val policyId = x.getAs[String]("policy_id")
          val holderName = x.getAs[String]("holder_name")
          val policyStartDate = x.getAs[java.sql.Timestamp]("policy_start_date")
          val policyEndDate = x.getAs[java.sql.Timestamp]("policy_end_date")
          val startDate = x.getAs[java.sql.Timestamp]("start_date")
          val endDate = x.getAs[java.sql.Timestamp]("end_date")
          val insuredCertNo = x.getAs[String]("insured_cert_no")
          var shouldContinuePolicyDayId = "" //应续投时间的day_id
          var shouldContinuePolicyReferDayId = "" //应续投参照时间的day_id
          val nowDate = getNowTime ().substring (0, 10).replaceAll ("-", "")
          val shouldContinuePolicyDate = if(policyEndDate != null ){
            java.sql.Timestamp.valueOf(currTimeFuction(getFormatDate(policyEndDate),1)) //应续保时间 保单结束时间+1D
          }else{
            null
          }

          shouldContinuePolicyDayId = if(shouldContinuePolicyDate != null){
            shouldContinuePolicyDate.toString.substring(0,10).replaceAll("-","")
          }else{
            null
          }

          var shouldContinuePolicyReferDate = if(policyEndDate != null ){
            java.sql.Timestamp.valueOf(currTimeFuction(getFormatDate(policyEndDate),-30)) //应续保人数的参照时间 保单结束时间-30D
          }else{
            null
          }

          shouldContinuePolicyReferDayId = if(shouldContinuePolicyReferDate != null){
            shouldContinuePolicyReferDate.toString.substring(0,10).replaceAll("-","")
          }else{
            null
          }

          val realyContinuePolicyDate = if(policyEndDate != null ){
            java.sql.Timestamp.valueOf(currTimeFuction(getFormatDate(policyEndDate),30)) //实续参照时间为  保单时间+30d
          }else{
            null
          }

          if(shouldContinuePolicyReferDate != null ){
            if (policyStartDate != null){
              if(shouldContinuePolicyReferDate.compareTo(policyStartDate) < 0){
                shouldContinuePolicyReferDate = policyStartDate
              }
            }
          }
          (policyId,holderName,policyStartDate,policyEndDate,startDate,endDate,insuredCertNo,
            shouldContinuePolicyDayId,shouldContinuePolicyReferDayId,
            shouldContinuePolicyDate,shouldContinuePolicyReferDate,realyContinuePolicyDate,nowDate)
        })
        .toDF("policy_id","holder_name","policy_start_date","policy_end_date","start_date","end_date","insured_cert_no","should_continue_policy_day_id",
          "should_continue_policy_refer_day_id","should_continue_policy_date","should_continue_policy_refer_date","realy_continue_policy_date","now_date")

      /**
        * 将雇主基础数据人员信息作为右表
        */
      val dwEmployerBaseinfoPersonDetailRigthRes =
        dwEmployerBaseinfoPersonDetail.selectExpr("holder_name as holder_name_right","policy_start_date as policy_start_date_right","start_date as start_date_right",
        "end_date as end_date_right","insured_cert_no as insured_cert_no_right")

      /**
        *  以上两个结果表做关联，判断条件为：投保人相同，被保人证件号相同的前提下，如果右表的被保人的起止日期包括实际续保的参照日期，保存，反之 删掉
        */
      val dwEmployerBaseinfoPersonDetailRes = dwEmployerBaseinfoPersonDetailLeftRes
        .join(dwEmployerBaseinfoPersonDetailRigthRes,'holder_name==='holder_name_right and 'insured_cert_no==='insured_cert_no_right,"leftouter")
        .selectExpr("policy_id","holder_name","policy_start_date","policy_end_date","start_date","end_date","insured_cert_no",
          "should_continue_policy_date","should_continue_policy_refer_date","realy_continue_policy_date","holder_name_right",
          "policy_start_date_right","policy_start_date_right","start_date_right","end_date_right","insured_cert_no_right")
        .where("start_date_right <= realy_continue_policy_date and end_date_right >= realy_continue_policy_date " +
          "and start_date <= should_continue_policy_refer_date and end_date >= should_continue_policy_refer_date and policy_start_date_right >= policy_end_date ")

      /**
        * 注册成临时表
        */
      dwEmployerBaseinfoPersonDetailRes.registerTempTable("dwEmployerBaseinfoPersonDetailResTemp")

      /**
        * 对于保单进行分组，得到去重后的被保人交集个数，即为：同一个被保人在续保保单中实续的人
        */
      val resTemp1 = sqlContext.sql("select policy_id as policy_id_slave,count(distinct insured_cert_no_right) as realy_continue_person_count " +
        "from dwEmployerBaseinfoPersonDetailResTemp group by policy_id,holder_name")

      /**
        * 对雇主基础数据人员左表信息不包含人员信息的数据进行去重，保存唯一保单信息
        */
      val baseInfoRes = dwEmployerBaseinfoPersonDetailLeftRes.selectExpr("policy_id","holder_name","policy_start_date","policy_end_date",
        "should_continue_policy_day_id","should_continue_policy_refer_day_id","should_continue_policy_date","realy_continue_policy_date","now_date")
        .distinct()

      /**
        * 将雇主基础数据人员左表信息 和 resTemp1进行左连接，因为resTemp1 会丢掉很多没有续保的数据。
        */
      val resTemp2 = baseInfoRes.join(resTemp1,baseInfoRes("policy_id")===resTemp1("policy_id_slave"),"leftouter")
        .selectExpr("policy_id as policy_id_master","holder_name as holder_name_master","policy_start_date as policy_start_date_master",
          "policy_end_date as policy_end_date_master","should_continue_policy_day_id","should_continue_policy_refer_day_id",
          "should_continue_policy_date","realy_continue_policy_date","now_date","realy_continue_person_count")

      /**
        * resTemp2和时间维度数据关联，得到应续保的周月季维度  d_day,c_day_id,c_week_long_desc,c_month_long_desc,c_quarter_long_desc
        */
      val resTemp3 = resTemp2.join(odsTDayDimension,resTemp2("should_continue_policy_day_id")===odsTDayDimension("c_day_id"),"leftouter")
        .selectExpr(
          "policy_id_master",
          "holder_name_master",
          "policy_start_date_master",
          "policy_end_date_master",
          "should_continue_policy_day_id",
          "should_continue_policy_refer_day_id",
          "c_week_long_desc as c_week_id",
          "c_month_long_desc as c_month_id",
          "c_quarter_long_desc as c_quarter_id",
          "should_continue_policy_date",
          "realy_continue_policy_date",
          "now_date",
          "realy_continue_person_count"
        )

      /**
        * resTemp3 和当前在保人表进行关联，得到应续保参照时间的当前在保人
        */
      val resTemp4 = resTemp3.join(dwolicyCurrInsuredDetail,'policy_id_master==='policy_id and 'should_continue_policy_refer_day_id==='day_id ,"leftouter")
        .selectExpr(
          "policy_id_master",
          "holder_name_master",
          "policy_start_date_master",
          "policy_end_date_master",
          "should_continue_policy_day_id",
          "should_continue_policy_refer_day_id",
          "count as should_continue_policy_person_count",
          "c_week_id",
          "c_month_id",
          "c_quarter_id",
          "should_continue_policy_date",
          "realy_continue_policy_date",
          "now_date",
          "realy_continue_person_count"
        )

      /**
        * 获取基础数据的其他字段值  并去重 保证保单信息唯一
        */
      val baseInfo2 = dwEmployerBaseinfoPersonDetail.selectExpr(
        "policy_id",
        "policy_code",
        "insure_company_name",
        "insure_company_short_name",
        "ent_id",
        "channel_id",
        "channel_name",
        "sale_name",
        "biz_operator",
        "sku_coverage",
        "sku_price",
        "sku_charge_type"
      ).distinct()

      /**
        * resTemp4 和基础数据关联 得到基础数据信息
        */
      val resTemp5 = resTemp4.join(baseInfo2,resTemp4("policy_id_master")===baseInfo2("policy_id"),"leftouter")
        .selectExpr(
          "policy_id_master",
          "policy_code",
          "holder_name_master",
          "insure_company_name",
          "insure_company_short_name",
          "ent_id",
          "channel_id",
          "channel_name",
          "sale_name",
          "biz_operator",
          "sku_coverage",
          "sku_price",
          "sku_charge_type",
          "policy_start_date_master",
          "policy_end_date_master",
          "should_continue_policy_day_id",
          "should_continue_policy_refer_day_id",
          "should_continue_policy_person_count",
          "c_week_id",
          "c_month_id",
          "c_quarter_id",
          "should_continue_policy_date",
          "realy_continue_policy_date",
          "now_date",
          "realy_continue_person_count"
        )

      /**
        * resTemp5 和 当前在保人表进行关联，得到当前在保人
        * 主键ID、保单号、保险公司名称、企业ID、企业名称(投保企业)、渠道ID、渠道名称、销售人员、运营人员、方案保额、方案单价、年/月单、
        * 当前在保人数、生效日期、终止日期、应续保日期、应续保周(C_WEEK_ID)、应续保月份(C_MONTH_ID)、应续保季度(C_QUARTER_ID)、到期人数、实续人数、数仓创建时间
        */
      val res = resTemp5.join(dwolicyCurrInsuredDetail,'policy_id_master === 'policy_id and 'now_date === 'day_id , "leftouter")
        .selectExpr(
          "getUUID() as id",
          "policy_id_master as policy_id",
          "policy_code",
          "insure_company_name",
          "insure_company_short_name",
          "ent_id",
          "holder_name_master as holder_name",
          "channel_id",
          "channel_name",
          "sale_name",
          "biz_operator",
          "sku_coverage",
          "sku_price",
          "sku_charge_type",
          "case when count is null then 0 else count end as current_insured",
          "policy_start_date_master as policy_start_date",
          "policy_end_date_master as policy_end_date",
          "should_continue_policy_date",
          "c_week_id",
          "c_month_id",
          "c_quarter_id",
          "case when should_continue_policy_person_count is null then 0 else should_continue_policy_person_count end as should_continue_policy_person_count",
          "cast((case when realy_continue_person_count is null then 0 else realy_continue_person_count end) as int) as realy_continue_policy_person_count",
          "should_continue_policy_day_id",
          "should_continue_policy_refer_day_id",
          "realy_continue_policy_date",
          "getNow() as dw_create_time"
        )
      res
    }
}
