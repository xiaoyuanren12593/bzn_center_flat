package bzn.job.common

import java.security.MessageDigest
import java.text.SimpleDateFormat
import java.util.regex.Pattern
import java.util.{Calendar, Date}

import com.alibaba.fastjson.JSONObject
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, Months}

import scala.collection.immutable
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * Created by a2589 on 2018/4/2.
  */
trait Until {
  /**
    *日期时间格式标准化
    * @param str
    * @return
    */
  def timeSubstring(str :String): String ={
    var date = ""
    if(str != null && str.length > 19){
      date = str.substring(0,19)
    }else if(str != null && str.length == 19){
      date = str
    }else{
      date = null
    }
    date
  }

  /**
    * mk5加密
    * @param input
    * @return
    */
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

  //将日期+8小时(24小时制)
  def eight_date(date_time: String): String = {
    //    val date_time = "2017-06-06 03:39:09.0"
    val sim = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = sim.parse(date_time)
    val c = Calendar.getInstance
    c.setTime(date)
    c.add(Calendar.HOUR_OF_DAY, +8)
    val newDate = c.getTime
    sim.format(newDate)
  }

   //当前日期+1天
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

  //当前日期+90天
  def dateAddNintyDay(date_time: String): String = {
    //    val date_time = "2017-06-06 03:39:09.0"
    val sim = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = sim.parse(date_time)
    val c = Calendar.getInstance
    c.setTime(date)
    c.add(Calendar.DATE, 90)
    val newDate = c.getTime
    sim.format(newDate)
  }

  //当前日期-90天
  def dateDelNintyDay(date_time: String): String = {
    //    val date_time = "2017-06-06 03:39:09.0"
    val sim = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = sim.parse(date_time)
    val c = Calendar.getInstance
    c.setTime(date)
    c.add(Calendar.DATE, -90)
    val newDate = c.getTime
    sim.format(newDate)
  }

  //当前日期前n天和后n天
  def currTimeFuction(date_time: String,d:Int): String = {
    val sim = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = sim.parse(date_time)
    val c = Calendar.getInstance
    c.setTime(date)
    c.add(Calendar.DATE, d)
    val newDate = c.getTime
    sim.format(newDate)
  }


  //将日期+8小时(24小时制)只有时间
  def eight_date_only_hour(date_time: String): String = {
    val sim = new SimpleDateFormat("HH:mm:ss")
    val date = sim.parse(date_time)
    val c = Calendar.getInstance
    c.setTime(date)
    c.add(Calendar.HOUR_OF_DAY, +8)
    val newDate = c.getTime
    sim.format(newDate)
  }

  //是否是数字
  def number_if_not(str: String): Boolean = {
    val pattern = Pattern.compile("^[-\\+]?[\\d]*$")
    pattern.matcher(str).matches

  }

  //得到当前的时间
  def getNowTime(): String = {
    //得到当前的日期
    val now: Date = new Date
    val dateFormatOne: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd 00:00:00")
    val now_Date: String = dateFormatOne.format(now)
    now_Date
  }

  //当前时间的前三个月是第几天
  def ThreeMonth(): String = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd 00:00:00")
    val date = new Date()
    val calendar = Calendar.getInstance()
    calendar.setTime(date)
    calendar.add(Calendar.MONTH, -3)
    val dates = calendar.getTime()
    val end: String = sdf.format(dates)
    end
  }

  //得到当前时间是周几
  def getWeekOfDate(strDate: String): Int = {

    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val date = sdf.parse(strDate)

    val weekDays = Array(7, 1, 2, 3, 4, 5, 6)
    val cal = Calendar.getInstance
    cal.setTime(date)
    var w = cal.get(Calendar.DAY_OF_WEEK) - 1
    if (w < 0) w = 0
    val res = weekDays(w)
    res
  }

  //得到现在是几几年
  def getNowYear(date: Date, dateFormat: SimpleDateFormat): Int = {
    //得到long类型当前时间//得到long类型当前时间
    dateFormat.format(date).toInt
  }

  //删除日期的小数点
  def deletePoint(strs: String): String = {
    var str = strs
    if (str.indexOf(".") > 0) {
      str = str.replaceAll("0+?$", ""); //去掉多余的0
      str = str.replaceAll("[.]$", ""); //如最后一位是.则去掉
    }
    str
  }

  //判断一个字符串是否含有数字
  def HasDigit(content: String): Boolean = {
    var flag = false
    val p = Pattern.compile(".*\\d+.*")
    val m = p.matcher(content)
    if (m.matches) flag = true
    flag
  }

  //计算一个日期，距离当前相隔几个月
  def getMonth(str: String): String = {
    val s = getNowTime()
    val formatter = DateTimeFormat.forPattern("yyyy-MM-dd")
    val start = formatter.parseDateTime(str)
    val end = formatter.parseDateTime(s.split(" ")(0))
    val months = Months.monthsBetween(start, end).getMonths
    months + ""
  }

  //算法统计相同
  def bruteForceStringMatch(source: String, pattern: String): Int = {
    val slen = source.length
    val plen = pattern.length
    val s = source.toCharArray
    val p = pattern.toCharArray
    var i = 0
    var j = 0
    //如果主串长度小于模式串，直接返回-1，匹配失败
    if (slen < plen) -1
    else {
      while ( {
        i < slen && j < plen
      }) if (s(i) == p(j)) { //如果i,j位置上的字符匹配成功就继续向后匹配
        i += 1
        j += 1
      }
      else {
        //i回溯到主串上一次开始匹配下一个位置的地方
        i = i - (j - 1)
        //j重置，模式串从开始再次进行匹配
        j = 0
      }
      //匹配成功
      if (j == plen) {
        i - j
      }
      //匹配失败
      else -1
    }
  }

  //出现的次数
  def count_num(arr: Array[String], num: String): Int = {
    var count = 0
    var i = 0

    while ( {
      i < arr.length
    }) {
      if (arr(i) == num) count += 1

      {
        i += 1;
        i - 1
      }
    }
    count
  }

  //得到2个日期之间的所有日期(当天和3个月之前的)
  def getBeg_End(): ArrayBuffer[String] = {
    val sdf = new SimpleDateFormat("yyyyMMdd")

    //得到过去第三个月的日期
    val c = Calendar.getInstance
    c.setTime(new Date)
    c.add(Calendar.MONTH, -3)
    val m3 = c.getTime
    val mon3 = sdf.format(m3)

    //得到今天的日期
    val cc = Calendar.getInstance
    cc.setTime(new Date)
    val day = cc.getTime
    val day_time = sdf.format(day)

    //得到他们相间的所有日期
    val arr: ArrayBuffer[String] = ArrayBuffer[String]()
    val date_start = sdf.parse(mon3)
    // val date_start = sdf.parse("20161007")
    val date_end = sdf.parse(day_time)
    // val date_end = sdf.parse("20161008")
    var date = date_start
    val cd = Calendar.getInstance //用Calendar 进行日期比较判断

    while (date.getTime <= date_end.getTime) {
      arr += sdf.format(date)
      cd.setTime(date)
      //增加一天 放入集合
      cd.add(Calendar.DATE, 1);
      date = cd.getTime
    }
    arr
  }

  //得到2个日期之间的所有天数
  def getBeg_End_one_two_new(mon3: String, day_time: String): Long = {

    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    //跨年不会出现问题
    //如果时间为：2016-03-18 11:59:59 和 2016-03-19 00:00:01的话差值为 0
    val fDate = sdf.parse(mon3)
    val oDate = sdf.parse(day_time)
    var days: Long = (oDate.getTime - fDate.getTime) / (1000 * 3600 * 24)
    if(days == 0){
      days = days+1
    }
    days
  }

  //得到2个日期之间的所有天数
  def getBeg_End_one_two(mon3: String, day_time: String): ArrayBuffer[String] = {
    val sdf = new SimpleDateFormat("yyyyMMdd")

    //得到过去第三个月的日期
    val c = Calendar.getInstance
    c.setTime(new Date)
    c.add(Calendar.MONTH, -3)
    val m3 = c.getTime

    //得到今天的日期
    val cc = Calendar.getInstance
    cc.setTime(new Date)
    val day = cc.getTime

    //得到他们相间的所有日期
    val arr: ArrayBuffer[String] = ArrayBuffer[String]()
    val date_start = sdf.parse(mon3)
    //    val date_start = sdf.parse("20161007")
    val date_end = sdf.parse(day_time)
    //    val date_end = sdf.parse("20161008")
    var date = date_start
    val cd = Calendar.getInstance //用Calendar 进行日期比较判断

    while (date.getTime <= date_end.getTime) {
      arr += sdf.format(date)
      cd.setTime(date)
      cd.add(Calendar.DATE, 1); //增加一天 放入集合
      date = cd.getTime
    }
    arr
  }

  //得到2个日期之间的所有月份
  def getBeg_End_one_two_month(mon3: String, day_time: String): ArrayBuffer[String] = {
    val sdf = new SimpleDateFormat("yyyyMM")

    //得到今天的日期
    val cc = Calendar.getInstance
    cc.setTime(new Date)
    val day = cc.getTime

    //得到他们相间的所有日期
    val arr: ArrayBuffer[String] = ArrayBuffer[String]()
    val date_start = sdf.parse(mon3)
    //    val date_start = sdf.parse("20161007")
    val date_end = sdf.parse(day_time)
    //    val date_end = sdf.parse("20161008")
    var date = date_start
    val cd = Calendar.getInstance //用Calendar 进行日期比较判断

    while (date.getTime <= date_end.getTime) {
      arr += sdf.format(date)
      cd.setTime(date)
      cd.add(Calendar.MONTH, 1); //增加一天 放入集合
      date = cd.getTime
    }
    arr
  }


  //精准的获取年龄
  def getAgeFromBirthTime(cert_no: String, time: String): Int = {
    if (
      cert_no.length == 18 &&
        (cert_no.substring(6, 10).toInt >= 1919) &&
        (cert_no.substring(10, 12).toInt >= 1 && cert_no.substring(10, 12).toInt <= 12) &&
        (cert_no.substring(12, 14).toInt >= 1 && cert_no.substring(12, 14).toInt <= 31)
    ) {
      if (time == null || "".equals(time) || "null".equals(time)) {
        0
      } else {
        val formatter = DateTimeFormat.forPattern("yyyyMMdd")
        var time_new = time.substring(0, 19)
        val formatter1 = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
        val birthTime = formatter.parseLocalDate(cert_no.substring(6, 14))
        val selectYear = birthTime.getYear.toInt
        val selectMonth = birthTime.getMonthOfYear.toInt
        val selectDay = birthTime.getDayOfMonth.toInt

        // 得到当前时间的年、月、日
        val localDate = formatter1.parseLocalDate(time_new)
        val yearNow = localDate.getYear.toInt
        val monthNow = localDate.getMonthOfYear.toInt
        val dayNow = localDate.getDayOfMonth.toInt

        // 用当前年月日减去生日年月日
        val yearMinus = yearNow - selectYear
        val monthMinus = monthNow - selectMonth
        val dayMinus = dayNow - selectDay
        var age = yearMinus // 先大致赋值
        if (yearMinus < 0) { // 选了未来的年份
          age = 0
        }
        else if (yearMinus == 0) { // 同年的，要么为1，要么为0
          if (monthMinus < 0) { // 选了未来的月份
            age = 0
          }
          else if (monthMinus == 0) { // 同月份的
            if (dayMinus < 0) { // 选了未来的日期
              age = 0
            }
            else if (dayMinus >= 0) age = 1
          }
          else if (monthMinus > 0) age = 1
        }
        else if (yearMinus > 0) if (monthMinus < 0) {
          // 当前月>生日月
        }
        else if (monthMinus == 0) { // 同月份的，再根据日期计算年龄
          if (dayMinus < 0) {
          }
          else if (dayMinus >= 0) age = age + 1
        }
        else if (monthMinus > 0) age = age + 1
        age
      }
    } else
      0
  }

  def to_null(s: String): String = {
    if (s == null) "null" else s.replaceAll("[^\u4E00-\u9FA5]", "")
  }

  //将时间转换为时间戳
  def currentTimeL(str: String): Long = {
    val format = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")

    val insured_create_time_curr_one: DateTime = DateTime.parse(str, format)
    val insured_create_time_curr_Long: Long = insured_create_time_curr_one.getMillis

    insured_create_time_curr_Long
  }

  //时间戳转换为日期
  def get_current_date(current: Long): String = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    //这个是你要转成后的时间的格式, 时间戳转换成时间
    val sd = sdf.format(new Date(current))
    sd
  }

  /**
    * 获得骑行天数
    * @param list
    * @return
    */
  def rideDays(list: mutable.ListBuffer[(String, String, String, String, String)]): String = {
    val set: mutable.Set[String] = mutable.Set[String]()
    for (l <- list) {
      set.add(l._2)
    }
    if (set.isEmpty) null else set.size.toString
  }

  /**
    * 获得最多骑行时间段
    * @param list
    * @return
    */
  def rideTimeStep(list: mutable.ListBuffer[(String, String, String, String, String)]): String = {
    val lists: ListBuffer[String] = mutable.ListBuffer[String]()
    for (l <- list) {
      lists += (if (l._3.toInt >= 1 && l._3.toInt < 3) "1-3时"
      else if (l._3.toInt >= 3 && l._3.toInt < 5) "3-5时"
      else if (l._3.toInt >= 5 && l._3.toInt < 7) "5-7时"
      else if (l._3.toInt >= 7 && l._3.toInt < 9) "7-9时"
      else if (l._3.toInt >= 9 && l._3.toInt < 11) "9-11时"
      else if (l._3.toInt >= 11 && l._3.toInt < 13) "11-13时"
      else if (l._3.toInt >= 13 && l._3.toInt < 15) "13-15时"
      else if (l._3.toInt >= 15 && l._3.toInt < 17) "15-17时"
      else if (l._3.toInt >= 17 && l._3.toInt < 19) "17-19时"
      else if (l._3.toInt >= 19 && l._3.toInt < 21) "19-21时"
      else if (l._3.toInt >= 21 && l._3.toInt < 23) "21-23时"
      else "23-1时")
    }
    if (lists.isEmpty) null else lists.max
  }

  /**
    * 获得骑行时间段
    * @param list
    * @return
    */
  def allRideTimeStep(list: mutable.ListBuffer[(String, String, String, String, String)]): String = {
    val lists: ListBuffer[String] = mutable.ListBuffer[String]()
    for (l <- list) {
      lists += (if (l._3.toInt >= 1 && l._3.toInt < 3) "1-3时"
      else if (l._3.toInt >= 3 && l._3.toInt < 5) "3-5时"
      else if (l._3.toInt >= 5 && l._3.toInt < 7) "5-7时"
      else if (l._3.toInt >= 7 && l._3.toInt < 9) "7-9时"
      else if (l._3.toInt >= 9 && l._3.toInt < 11) "9-11时"
      else if (l._3.toInt >= 11 && l._3.toInt < 13) "11-13时"
      else if (l._3.toInt >= 13 && l._3.toInt < 15) "13-15时"
      else if (l._3.toInt >= 15 && l._3.toInt < 17) "15-17时"
      else if (l._3.toInt >= 17 && l._3.toInt < 19) "17-19时"
      else if (l._3.toInt >= 19 && l._3.toInt < 21) "19-21时"
      else if (l._3.toInt >= 21 && l._3.toInt < 23) "21-23时"
      else "23-1时")
    }
//    分组聚合
    val map: Map[String, String] = lists.groupBy(_.toString).mapValues(_.size.toString)
//    创建JSON
    val JSON: JSONObject = new JSONObject()
    for (m <- map) {
      JSON.put(m._1, m._2)
    }
//    返回JSON的String
    JSON.toString

  }

  /**
    * 获得最多骑行品牌
    * @param list
    * @return
    */
  def rideBrand(list: mutable.ListBuffer[(String, String, String, String, String)]): String = {
    val lists: ListBuffer[String] = mutable.ListBuffer[String]()
    for (l <- list) {
      lists += l._1
    }
    if (lists.isEmpty) null else lists.max
  }

  /**
    * 获得骑行品牌
    * @param list
    * @return
    */
  def allRideBrand(list: mutable.ListBuffer[(String, String, String, String, String)]): String = {
    val lists: ListBuffer[String] = mutable.ListBuffer[String]()
    for (l <- list) {
      lists += l._1
    }
//    分组聚合
    val map: Map[String, String] = lists.groupBy(_.toString).mapValues(_.size.toString)
//    循环放入JSON
    val JSON: JSONObject = new JSONObject()
    for (m <- map) {
      JSON.put(m._1, m._2)
    }
//    返回JSON的String
    JSON.toString

  }

  /**
    * 获得最多骑行日期
    * @param list
    * @return
    */
  def rideDate(list: mutable.ListBuffer[(String, String, String, String, String)]): String = {
    val lists: ListBuffer[String] = mutable.ListBuffer[String]()
    for (l <- list) {
      lists += l._4
    }
    if (lists.isEmpty) null else lists.max
  }

  /**
    * 获得骑行日期
    * @param list
    * @return
    */
  def allRideDate(list: mutable.ListBuffer[(String, String, String, String, String)]): String = {
    val lists: ListBuffer[String] = mutable.ListBuffer[String]()
    for (l <- list) {
      lists += l._4
    }
//    分组聚合
    val map: Map[String, String] = lists.groupBy(_.toString).mapValues(_.size.toString)
//    创建JSON
    val JSON: JSONObject = new JSONObject()
    for (m <- map) {
      JSON.put(m._1, m._2)
    }
//    返回字符串
    JSON.toString

  }

  /**
    * 获得近90天骑行频率
    * @param list
    * @return
    */
  def tripRate(list: mutable.ListBuffer[(String, String, String, String, String)]): String = {
    val lists: ListBuffer[String] = mutable.ListBuffer[String]()
    for (l <- list) {
      if (l._2 > l._5) lists += l._2
    }
    if (lists.isEmpty) 0.toString else lists.size.toString
  }

  /**
    * 获得生物钟
    * @param list
    * @return
    */
  def internalClock(list: mutable.ListBuffer[(String, String, String, String, String)]): String = {
    val time: String = rideTimeStep(list)
    val internal: String = if (time == "5-7时") "早期族"
    else if (time == "23-1时" || time == "1-3时" || time == "3-5时") "夜猫子"
    else null
    internal
  }

  /**
    * 将空字符串、空值转换为NULL
    * @param Temp
    * @return
    */
  def dropEmpty(Temp: String): String = {
    if (Temp == "" || Temp == "NULL" || Temp == null) null else Temp
  }

  /**
    * 身份证匹配
    * @param Temp
    * @return
    */
  def dropSpecial(Temp: String): Boolean = {
    if (Temp != null) {
      val pattern = Pattern.compile("^[\\d]{17}[\\dxX]{1}$")
      pattern.matcher(Temp).matches
    } else false
  }

  /**
    * 根据生日月日获取星座id
    * @param month
    * @param day
    * @return
    */
  def getConstellation(month: String, day: String): String = {
    val dayArr = Array[Int](20, 19, 21, 20, 21, 22, 23, 23, 23, 24, 23, 22)
    val constellationArr = Array[Int](10, 11, 12, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    return if (day.toInt < dayArr(month.toInt - 1)) {
      constellationArr(month.toInt - 1).toString
    } else {
      constellationArr(month.toInt).toString
    }
  }

  /**
    * 自定义函数是否上学
    * @param age
    * @return
    */
  def isAttendSchool(age: String): String = {
    val ageInt: Int = age.toInt
    var isAttendSch: String = null
    if (ageInt >= 6 && ageInt <= 15) {
      isAttendSch =  "上学"
    } else {
      isAttendSch =  "未上学"
    }
    isAttendSch
  }

}
