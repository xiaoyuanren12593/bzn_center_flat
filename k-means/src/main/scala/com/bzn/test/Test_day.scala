package com.bzn.test

import java.text.SimpleDateFormat
import java.util.Date

import com.google.common.base.Strings
import org.joda.time.format.DateTimeFormat

object Test_day {
  def main(args: Array[String]): Unit = {
    var res = getAge("232325199304143617","")
    var s = "5"*1
    boolContain换行("黄婉虹\\n50周岁")

    //获取年龄
    var birth: Int = getAgeFromBirthTime("232325199304143617","2018-08-01 00:00:00.1")
    var time = "24"
    if(time == null  || "".equals(time) || "null".equals(time)){
      println("字符串不符合规定")
    }else
      println("字符串符合规定")
    println(birth)
  }

  def boolContain换行(str :String): Unit = {
    if(str == null && Strings.isNullOrEmpty(str)){
      return
    }

    val bool = str.contains("\\n")

    if(bool) println("有换行符") else println("没有换行符")
  }

  //精准的获取年龄
  def getAgeFromBirthTime(cert_no: String,time:String): Int = { // 先截取到字符串中的年、月、日
    if (cert_no.length == 18) {
      if (time == null || "".equals(time) || "null".equals(time)) {
        0
      } else {
        val formatter = DateTimeFormat.forPattern("YYYYMMdd")
        var time_new  = time.substring(0,19)
        val formatter1 = DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss")
        val birthTime = formatter.parseLocalDate(cert_no.substring(6, 14))
        val selectYear = birthTime.getYear.toInt
        val selectMonth = birthTime.getMonthOfYear.toInt
        val selectDay = birthTime.getDayOfMonth.toInt
        println(selectYear + "" + selectMonth + "" + selectDay)

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
        println(yearNow + "" + monthNow + "" + dayNow)
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

  //获取年龄
  def getAge(cert_no :String, end :String): String ={
    import java.text.SimpleDateFormat
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")

    if(cert_no.length == 18){
      //跨年的情况会出现问题哦
      //如果时间为：2016-03-18 11:59:59 和 2016-03-19 00:00:01的话差值为 1
      if(end != null  && !"".equals(end) && !"null".equals(end)){
        val fDate = sdf.parse(cert_no.substring(6,14))
        println(cert_no.substring(6,14))
        val oDate = sdf.parse(end)
        println(oDate.getTime.toString)
        val days = (((oDate.getTime - fDate.getTime) / (1000 * 3600 * 24))/365).toInt
        println(days)
        days.toString
      }else
        "-1"
    } else
      "-1"
  }
}
