package bzn.dm.backup

import bzn.job.common.Until

/**
  * author:xiaoYuanRen
  * Date:2019/7/25
  * Time:14:28
  * describe: this is new class
  **/
object test extends Until{
  def main(args: Array[String]): Unit = {
    var i = 0
    while(i < 10){
      val str = scala.util.Random.nextInt(10).toString
      println(str)
      i = i+1
    }
    val nowTime = getNowTime()
    println("周几啊这是  "+getWeekOfDate("2019-08-22 00:00:00"))
    println(currTimeFuction(nowTime,-1))
  }
}
