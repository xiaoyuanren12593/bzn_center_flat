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
    println(getBeg_End_one_two_new("2018-09-18 00:00:00.0".substring(0,19),get_current_date(System.currentTimeMillis()).toString.substring(0,19)))
    println(dateDelNintyDay(get_current_date(System.currentTimeMillis()).toString.substring(0,19)))
  }
}
