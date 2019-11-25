package bzn.dm.aegis

import bzn.job.common.Until

/**
  * author:xiaoYuanRen
  * Date:2019/11/25
  * Time:10:05
  * describe: this is new class
  **/
object test extends Until{
  def main (args: Array[String]): Unit = {
    val date = java.sql.Timestamp.valueOf("2019-10-15 00:00:00")
    println (getFormatDate (date))
    println(java.math.BigDecimal.valueOf(0))
  }
}
