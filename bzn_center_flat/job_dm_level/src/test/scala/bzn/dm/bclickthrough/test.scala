package bzn.dm.bclickthrough

import bzn.job.common.Until

/**
  * author:xiaoYuanRen
  * Date:2019/10/15
  * Time:14:55
  * describe: this is new class
  **/
object test extends Until{
  def main (args: Array[String]): Unit = {
    val date = java.sql.Timestamp.valueOf("2019-10-15 23:59:59")
    println (getFormatDate (date))
  }
}
