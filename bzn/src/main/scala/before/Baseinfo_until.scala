package before

import java.text.NumberFormat
import java.time.LocalDateTime
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import java.time.temporal.ChronoField

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import sun.util.calendar.CalendarUtils.mod

/**
  * Created by a2589 on 2018/4/3.
  */
trait Baseinfo_until extends until {
  //得到投保人投保了多少年(麻烦)
  def year_tb_one(before: String,
                  after: String)
  : String
  = {
    val formatter_before = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val qs_hs: DateTimeFormatter = new DateTimeFormatterBuilder().append(formatter_before)
      .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
      .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
      .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
      .toFormatter()

    val lod_before = LocalDateTime.parse(before, qs_hs)
    val lod_after = LocalDateTime.parse(after, qs_hs)
    //s1的年
    val s3_y = lod_before.getYear
    //s2的年
    val s4_y = lod_after.getYear

    var ss = s4_y - s3_y
    //s1的月
    val s5_m = lod_before.getMonth.getValue
    //s2的月
    val s6_m = lod_after.getMonth.getValue

    //s1的天
    val s7_d = lod_before.getDayOfMonth
    //s2的天
    val s8_d = lod_after.getDayOfMonth

    val result = if (s5_m < s6_m) {
      ss
    } else if (s5_m > s6_m) {
      ss - 1
    } else if (s5_m == s6_m) {
      val res = if (s7_d < s8_d) {
        ss
      } else if (s7_d > s8_d) {
        ss - 1
      } else if (s7_d == s8_d) {
        ss
      }
      res
    }
    s"$result"
  }

  /**
    * 得到企业产品的ID
    *
    * @param ods_policy_detail 保单级别信息总表	保单级别综合数据
    * @return RDD 返回企业ID，产品编码，编码别名
    **/
  def qy_cp(ods_policy_detail: DataFrame)
  : RDD[(String, String, String)]
  = {
    //企业产品ID :insure_code(产品代码)
    val qy_Producer = ods_policy_detail.where("policy_status in ('0','1','7','9','10')")
      .filter("length(insure_code) > 0").select("ent_id", "insure_code", "policy_status")

    val end: RDD[(String, String, String)] = qy_Producer.map(x => {
      (x.getString(0), (x.getString(1), x.getString(2)))
    }).groupByKey().map(x => {
      val result = x._2.map(x => x._1).toSet
      (x._1, result.mkString("|"), "ent_insure_code")
    })
    end
  }

  //该企业男女比例
  def qy_sex(ods_policy_insured_detail: DataFrame,
             ods_policy_detail_table_T: DataFrame)
  : RDD[(String, String, String)]
  = {
    /**
      * --  女生在该企业的占比,根据保单ID作join，对企业ID进行groupBy
      * -- 男生在该企业的占比
      * -- 往hbase里面存的话是以企业id为主键
      *
      * ods_policy_detail:保单明细表
      * * policy_id：保单id (是唯一的在该张表中)
      * * ent_id：企业id
      *
      * ods_policy_insured_detail:被保人明细表
      * * policy_id：保单id (是唯一的在该张表中)
      * * insured_name:被保人的名字
      * * insured_gender:身份证编号(倒数第二个数字/2 求膜 ==1是男 == 0是女) 对其表示为man,woman2个字段
      * *
      **/
    // 创建一个数值格式化对象(对数字)
    val numberFormat = NumberFormat.getInstance
    val ods_policy_insured_detail_table = ods_policy_insured_detail.filter("LENGTH(insured_cert_no)=18").select("policy_id", "insured_name", "insured_cert_no")
    val join_qy_mx = ods_policy_detail_table_T.join(ods_policy_insured_detail_table, "policy_id")
    //      |           policy_id|              ent_id|insured_name|   insured_cert_no|
    //      +--------------------+--------------------+------------+------------------+
    //      |6d04d7669ad34e488...|43294d3451c5411ab...|          文斌|110101198001138997|

    //该企业中男生，女生的百分比

    val man_woman_Percent_AgeAvg: RDD[(String, String, String)] = join_qy_mx.map(x =>
      //      |           ent_id |          policy_id    |insured_name|   insured_cert_no|
      (x.getString(1), (x.getString(0), x.getString(2), x.getString(3)))
    ).groupByKey().map(x => {
      /**
        * 计算该企业中女生和男生的占比
        **/
      val end_ID: String = x._1
      //该企业有多少人
      val counts: Int = x._2.map(_._3).size
      //企业的男生和女生0或者1
      val people_number = x._2.map(s => {
        val str2 = s._3.substring(s._3.length - 2, s._3.length - 1)
        //这里截取的信息就是e，倒数第二个字符
        val ss = str2.toInt
        //取模求余 ：0是女生  1是男生
        val aa = mod(ss, 2)
        aa
      })
      //企业的男生和女生0或者1的数量
      val man_Percentage = people_number.count(x => x == 1)
      val woman_Percentage = people_number.count(x => x == 0)
      // 设置精确到小数点后2位
      numberFormat.setMaximumFractionDigits(2)
      //男生百分比/女生百分比
      val result_man: String = numberFormat.format(man_Percentage.toFloat / counts.toFloat * 100)
      val result_woman: String = numberFormat.format(woman_Percentage.toFloat / counts.toFloat * 100)
      (end_ID, s"$result_man%,$result_woman%", "ent_man_woman_proportion")
    })
    man_woman_Percent_AgeAvg
  }

  //企业的人员规模
  def qy_gm(ent_sum_level: DataFrame)
  : RDD[(String, String, String)]
  = {
    //企业人员规模：end_id(企业id) ,ent_scale(人数)
    val end = ent_sum_level.where("length(ent_id)>0").select("ent_id", "ent_scale")
      .map(x => {
        (x.getString(0), x.getString(1), "ent_scale")
      })
    end
  }

  //企业品牌影响力
  def qy_pp(ent_sum_level: DataFrame)
  : RDD[(String, String, String)]
  = {
    //企业品牌影响力 ：end_id(企业id) ,ent_influence_level(企业的等级)
    val end = ent_sum_level.where("length(ent_id)>0").select("ent_id", "ent_influence_level")
      .map(x => {
        (x.getString(0), x.getString(1), "ent_influence_level")
      })
    end
  }

  //企业潜在人员规模
  def qy_qz(ods_policy_detail: DataFrame,
            ods_policy_insured_detail: DataFrame,
            ent_sum_level: DataFrame)
  : RDD[(String, String, String)]
  = {
    //ods_policy_insured_detail :被保人清单
    //ods_policy_detail：保单明细表
    //ent_sum_level:企业等级（包括企业的ID和总人数）

    //得到企业ID,police_id,police_status(保单状态，主要对应留学保保单状态(1：有效；0：无效；2：未完成；3：核保失败；4：预售成功；5：待支付；6：承保中；7：已承保；8：已取消；9：已终止; 10：已过期))

    val before = ods_policy_detail.where("length(ent_id)>0 and policy_status in('0','1', '7', '9', '10') ").select("ent_id", "policy_id", "policy_status")
    //得到被保人清单中的,id，和证件号,被保人投保状态，1为在保2为已减员3为失效

    val after = ods_policy_insured_detail.where("insure_policy_status='1'").selectExpr("policy_id", "insured_cert_no as cur_insured_persons", "insure_policy_status")

    //计算我该企业中有多少人投保了：返回的是企业ID，投保人数
    val j_after = before.join(after, "policy_id")
    //    |         policy_id|              ent_id|policy_status|cur_insured_persons|insure_policy_status|
    //    |122008268007673856|10e11fd0b1a7488db...|            0| 44132219951005661X|                   1|
    //ent_id|COUNT(DISTINCT pid.insured_cert_no) as cur_insured_persons
    val end = j_after.map(x => {
      //ent_id|police_id|policy_status|cur_insured_persons|insure_policy_status
      (x.getString(1), (x.getString(0), x.getString(2), x.getString(3), x.getString(4)))
    })
      .filter(_._2._3.length == 18)
      .map(x => (x._1, 1)).reduceByKey(_ + _)
    //      .groupByKey.map(x => {
    //      val s = x._2.map(x => x._3).toSet.size
    //      (x._1, s)
    //    })

    //得到ent_sum_level（企业等级）:ent_id|ent_scale(企业人员规模)
    val esl = ent_sum_level.select("ent_id", "ent_scale").filter("length(ent_id)>0 and  length(ent_scale)<10").map(x => (x.getString(0), x.getString(1)))

    val ents = end.join(esl).map(x => {
      val ent_scale = x._2._2.toInt
      val cur_insured_persons = x._2._1
      val ll = if (ent_scale > cur_insured_persons) Math.ceil(ent_scale - cur_insured_persons) else 0
      (x._1, ll.toInt + "")
    }).reduceByKey((x1, x2) => {
      if (x1 >= x2) x1 else x2
    }).map(x => {
      (x._1, x._2 + "", "ent_potential_scale")
    })

    //    //输出结果：企业潜在人员规模(ent_id,ent_scala,cur_insured_persons)
    //    val ents: RDD[(String, String, String)] = esl.join(end).map(x => {
    //      val ent_id: String = x._1 //ent_id
    //      val ent_scala = x._2._1.toDouble //ent_scale
    //      val cur_insured_persons = x._2._2.toDouble //cur_insured_persons(就是该企业总共有多少人)
    //      val ll = if (ent_scala > cur_insured_persons) Math.ceil(ent_scala - cur_insured_persons) else 0
    //      (ent_id, ll.toInt + "", "ent_potential_scale")
    //    })
    ents
  }

  //企业类型
  def ent_type(ent_enterprise_info: DataFrame,
               d_id_certificate: DataFrame,
               ods_policy_detail_table: DataFrame)
  : RDD[(String, String, String)]
  = {
    //以前的user_id已经不能通过他进行join了
    val tepOne = ent_enterprise_info.selectExpr("id", "triangle_status", "id as ent_id")
    val tepTwo = d_id_certificate.select("id_nub", "certificate").where("length(certificate)>0")
    val tepThree = tepOne.join(tepTwo, tepOne("triangle_status") === tepTwo("id_nub"))
    val end = ods_policy_detail_table.select("ent_id", "policy_status").join(tepThree, "ent_id").filter("policy_status in ('0','1','7','9','10')")
    val et: RDD[(String, String, String)] = end.map(x => {
      (x.getString(2), x.getString(5), "ent_type")
    })
    et
  }

}
