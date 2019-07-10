package bzn.c_person.centinfo

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
  }
}
