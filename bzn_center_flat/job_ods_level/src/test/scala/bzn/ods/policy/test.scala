package bzn.ods.policy

import java.security.MessageDigest
import java.sql.Timestamp
import java.text.{NumberFormat, SimpleDateFormat}
import java.util.{Calendar, Date}

import bzn.job.common.Until
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.math.BigDecimal.RoundingMode
import scala.math.BigDecimal.RoundingMode.RoundingMode

/**
  * author:xiaoYuanRen
  * Date:2019/5/21
  * Time:14:57
  * describe: this is new class
  **/
object test extends Until {
  def main(args: Array[String]): Unit = {
    if(null == null){
      println("q3")
    }else{
      println("123")
    }

    println(MD5("北京诠道科技有限公司"))
//    5efa43fccd26383273bd8ad97451a30a
//    5efa43fccd26383273bd8ad97451a30a
    println(MD5("println(MD5(\"\"\"\"))"))

  }


  def hashMD5(content: String): String = {
    val md5 = MessageDigest.getInstance("MD5")
    val encoded = md5.digest((content).getBytes)
    encoded.map("%02x".format(_)).mkString

  }


}