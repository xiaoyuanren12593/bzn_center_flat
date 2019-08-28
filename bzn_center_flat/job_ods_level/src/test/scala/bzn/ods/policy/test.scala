package bzn.ods.policy

import java.security.MessageDigest
import java.sql.Timestamp
import java.text.{NumberFormat, SimpleDateFormat}
import java.util.{Calendar, Date}

import bzn.dw.premium.DwPolicyInsuredDayIdDetailTest.getBeg_End_one_two
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
object test {
  def main(args: Array[String]): Unit = {
    var z = 0
    var i = Integer.valueOf(0)
    if(i.equals(null)){
      z = Integer.valueOf(0).toInt
    }

    println(i)
//    println(MD5("132")+"4654")
    println("123")

//    println(to_null("中sss国"))
//    val md5Str = MD5("北京德知航创科技有限责任公司")
//    val md5Str1 = MD5("北京德知航创科技有限责任公司")
//    println(md5Str)
//    println(md5Str1)
    var str = "1970-00-00 00:00:00.0"
    println(str.length,"changdu"+str.substring(0,19))
    if(str != null && str.length == 19){
      str = str.substring(0,10)
    }
    println(str)

    if(str !=null && str !="null") {
      println(currentTimeL("2019-05-29 00:11:11"))
    }
    println(typeChange(28.5423))
    println(1.45645.toDouble/2.45455.toDouble)

    val d1 = 1
    val d2 = 20.2
    val d3: Double = 300.04
    val res = d1*d2*d3
    println(typeChange(res))
    println(timeSubstring("2019-05-29 00:11:1"))
    println(dateAddOneDay("2019-05-29 00:11:11.0"))
    var test: Timestamp = new Timestamp(0)
    test = null
    var preEndDateRes = println(test)
    println("^A")

    val arr = Array(1,2)
    val arr1 = Array(1,2,3)
    if(arr sameElements  arr1){
      println("12313132")
    }
    val l1 = Seq("1","2").sorted
    val l2 = Seq("2","1","-1").sorted

    println(l1.sameElements(l2))
    println((l2 diff l1).mkString(" "))

    println("80123".substring(2,4))
    val bigDecimal2 = BigDecimal(213131.0000000000)
    println(bigDecimal2.setScale(2,RoundingMode(3)))
    println(bigDecimal2.setScale(2,RoundingMode(4)))
    println(bigDecimal2.setScale(2,RoundingMode(5)))

    println(getDouble(12.45455))

    val numberFormat = NumberFormat.getInstance
    // 设置精确到小数点后4位
    numberFormat.setMaximumFractionDigits(4)
    val d = numberFormat.format(12.1534531)
    println(numberFormat.format(12.1534531))
    val ldate =  currentTimeL("2019-04-11 17:31:14")
    println(ldate)
    if(""<= "232"){
      println("132131")
    }
    val arrayBuffer: ArrayBuffer[String] = getBeg_End_one_two("20190411","20190415")
    arrayBuffer.foreach(println)
    val set = Set("`","`")
    val setRes = set.filter(x => !x.contains("`"))
    setRes.foreach(println)
    println("   153013021115602090480".length)
    println("   153013021115602090480".substring("   153013021115602090480".length-11))
    println("BZN_QJDC_1201_1231".contains("BZN_QJDC_001"))

    println("------------------------")
    val time: java.sql.Timestamp = java.sql.Timestamp.valueOf("2019-08-08 01:01:01")
    val strr: String = time.toString.split("\\.")(0)
    println(strr)

    println("------------------------")
    println("------------------------")
    val strs: String = "20.12"
    val strs1: String = null
    println(new java.math.BigDecimal(strs))
//    println(new java.math.BigDecimal(strs1))


  }

  def getNull(line: String): Int = {
    val res: Int = if (line == "" || line == null || line == "NULL") 9 else line.toInt
    res
  }

  def getDouble(d:Double): Double = {
    if (d != null) {
      val decimal = BigDecimal.apply(d)
      val res = decimal.setScale(4, BigDecimal.RoundingMode.HALF_UP).doubleValue()
      res
    } else {
      d
    }
  }
  def dateAddOneDay(date_time: String): String = {
    //    val date_time = "2017-06-06 03:39:09.0"
    val sim = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = sim.parse(date_time)
    val c = Calendar.getInstance
    c.setTime(date)
    c.add(Calendar.DATE, 1)
    val newDate = c.getTime
    sim.format(newDate)
  }


  def typeChange(dec:Double): BigDecimal = {
    if(dec != null){
      dec
    }else{
      dec
    }
  }

  def timeSubstring(str :String): String ={
    var date = ""
    if(str != null && str.length > 19){
      date = str.substring(0,19)
    }else if(str.length == 19){
      date = str
    }else{
      date = null
    }
    date
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

