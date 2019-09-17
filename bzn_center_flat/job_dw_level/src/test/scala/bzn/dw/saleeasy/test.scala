package bzn.dw.saleeasy

import bzn.job.common.Until

/**
  * author:xiaoYuanRen
  * Date:2019/9/17
  * Time:15:26
  * describe: this is new class
  **/
object test extends Until{
  def main (args: Array[String]): Unit = {
    val timePara = java.sql.Timestamp.valueOf("2017-11-03 00:00:00")
    val timePara1 = "20171103"
    println (timePara1.substring (0, 4).concat ("-"))
    println (timePara1.substring (4, 6).concat ("-"))
    println (timePara1.substring (6, 8))
    println (getTimeYearAndMonthAndDay(timePara1))
    println (getTimeYearAndMonth(timePara))

  }
}
