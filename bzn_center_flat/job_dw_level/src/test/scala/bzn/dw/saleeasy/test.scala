package bzn.dw.saleeasy

import java.sql.Timestamp
import java.time.{YearMonth, ZoneId, ZonedDateTime}
import java.util.Date

import bzn.dw.premium.DwTypeOfWorkClaimDetailTest.{getBeginTime, getFormatTime}
import bzn.job.common.Until

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

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
    getBeg_End_one_two_month("2017-11-03 00:00:00","2017-12-03 00:00:00").foreach(println)
    println (getNowTime().substring(0,10).replaceAll("-",""))
    println (getNowTime().substring (0, 10))
    println (java.sql.Timestamp.valueOf ("2017-11-03 00:00:00"))

    println (getTimeYearAndMonth (currTimeFuction (java.sql.Timestamp.valueOf ("2017-11-30 00:00:00").toString, 1).substring(0,7).replaceAll("-","")))

    getBeg_End_one_two_month_day("2018-03-01 00:00:00","2019-02-28 23:59:59").foreach(println)

    import java.text.SimpleDateFormat
    var sdf = new SimpleDateFormat ("yyyy-MM-dd HH:mm:ss")
    val year = 2016
    val month = "03".toInt

    println (month)
    val beginTime = getBeginTime (year, month)
    println (beginTime)
    val endTime = getEndTime (year, month)
    println ("2018-03-01 00:00:00".compareTo ("2019-02-28 23:59:59"))
    println (java.sql.Timestamp.valueOf(currTimeFuction ("2019-02-28 23:59:59.0".toString, 1).substring(0,10).concat(" 00:00:00")))


    println(getBeginTime("2019-6-13 00:00:00").toString+"2346546")
    val sim = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date11: Timestamp = java.sql.Timestamp.valueOf(getFormatTime(getBeginTime("2019-6-17 00:00:00")))
    println(date11)
    println(java.sql.Timestamp.valueOf(getFormatTime(getBeginTime("2019/4/7".replaceAll("/", "-").concat(" 00:00:00")))))

  }

  /**
    * 求向量的模
    * @param vec
    * @return
    */
  def module(vec: Vector[Double]) = {
    math.sqrt(vec.map(math.pow(_, 2)).sum)
  }

  /**
    * 求两个向量的内积
    * @param v1
    * @param v2
    * @return
    */
  def innerProduct(v1: Vector[Double], v2: Vector[Double]) =
  {
    val listBuffer = ListBuffer[Double]()
    for (i <- 0 until v1.length; j <- 0 to v2.length; if i == j) {
      if (i == j)
        listBuffer.append(v1(i) * v2(j))
    }
    listBuffer.sum
  }

  /**
    * 求两个向量的余弦
    * @param v1
    * @param v2
    * @return
    */

  def cosvec(v1: Vector[Double], v2: Vector[Double]) = {
    val cos = innerProduct(v1, v2) / (module(v1) * module(v2))
    if (cos <= 1) cos else 1.0
  }

  /**
    * 余弦相似度
    * @param str1
    * @param str2
    * @return
    */
  def textCosine(str1: String, str2: String) = {
    val set = mutable.Set[Char]()
    // 不进行分词
    str1.foreach(set += _)
    str2.foreach(set += _)
    val ints1: Vector[Double] = set.toList.sorted.map(ch => {
      str1.count(s => s == ch).toDouble }).toVector
    val ints2: Vector[Double] = set.toList.sorted.map(ch => {
      str2.count(s => s == ch).toDouble
    }).toVector
    cosvec(ints1, ints2)
  }
}
