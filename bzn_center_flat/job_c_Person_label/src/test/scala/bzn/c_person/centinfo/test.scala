package bzn.c_person.centinfo

import java.sql.Timestamp
import java.text.SimpleDateFormat

import bzn.job.common.Until


/**
  * author:xiaoYuanRen
  * Date:2019/7/10
  * Time:18:33
  * describe:测试
  **/
object test extends Until{
  def main(args: Array[String]): Unit = {
    val str = "232425"
    println(str.substring(0, 2))
    println(str.substring(2, 4))
    println(str.substring(4, 6))
    println(getAgeFromBirthTime("232325199404143617","2019-07-10 00:00:00"))
    println(dateAddNintyDay("2019-07-10 00:00:00"))
    val first_policy_time_90_days = Timestamp.valueOf(getNowTime())
    println(System.currentTimeMillis())
    println(get_current_date(System.currentTimeMillis()))
    println(first_policy_time_90_days)
    println(getBeg_End_one_two_test("2018-06-05 00:00:00",get_current_date(System.currentTimeMillis()).toString.substring(0,19)))
  }


  //得到2个日期之间的所有天数
  def getBeg_End_one_two_test(mon3: String, day_time: String) = {

    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    //跨年不会出现问题
    //如果时间为：2016-03-18 11:59:59 和 2016-03-19 00:00:01的话差值为 0
    val fDate = sdf.parse(mon3)
    val oDate = sdf.parse(day_time)
    val days: Long = (oDate.getTime - fDate.getTime) / (1000 * 3600 * 24)
    days
  }
}
