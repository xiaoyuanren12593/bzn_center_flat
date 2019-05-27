package bzn.ods.policy

import java.security.MessageDigest
import java.text.SimpleDateFormat
import java.util.Date

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

/**
  * author:xiaoYuanRen
  * Date:2019/5/21
  * Time:14:57
  * describe: this is new class
  **/
object test {
  def main(args: Array[String]): Unit = {
//    println(MD5("132")+"4654")
    println("123")

//    println(to_null("中sss国"))
//    val md5Str = MD5("北京德知航创科技有限责任公司")
//    val md5Str1 = MD5("北京德知航创科技有限责任公司")
//    println(md5Str)
//    println(md5Str1)
    var str = "1973-08-22 00:00:00"
    println(str.length)
    if(str != null && str.length == 19){
      str = str.substring(0,10)
    }
    println(str)

    println(currentTimeL("2019-05-29 00:11:11"))
    println(get_current_date(0))
  }

  def get_current_date(current: Long): String = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    //这个是你要转成后的时间的格式, 时间戳转换成时间
    val sd = sdf.format(new Date(current));
    sd
  }

  def to_null(s: String): String = {
    if (s == null) "null" else s.replaceAll("[^\u4E00-\u9FA5]", "")
  }

  def MD5(input: String): String = {
    var md5: MessageDigest = null
    try {
      md5 = MessageDigest.getInstance("MD5")
    } catch {
      case e: Exception => {
        e.printStackTrace
      }
    }
    val byteArray: Array[Byte] = input.trim.getBytes
    val md5Bytes: Array[Byte] = md5.digest(byteArray)
    var hexValue: String = ""
    var i: Integer = 0
    for (i <- 0 to md5Bytes.length - 1) {
      val str: Int = (md5Bytes(i).toInt) & 0xff
      if (str < 16) {
        hexValue = hexValue + "0"
      }
      hexValue = hexValue + (Integer.toHexString(str))
    }
    return hexValue.toString
  }

  //将时间转换为时间戳
  def currentTimeL(str: String): Long = {
    val format = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")

    val insured_create_time_curr_one: DateTime = DateTime.parse(str, format)
    val insured_create_time_curr_Long: Long = insured_create_time_curr_one.getMillis

    insured_create_time_curr_Long
  }
}

