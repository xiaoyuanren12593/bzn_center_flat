package c_person.highinfo

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.regex.Pattern
import java.util.{Calendar, Date}

import bzn.job.common.Until

object test extends Until {

  def main(args: Array[String]): Unit = {

    println(dateDelNintyDay("2019-07-18 00:00:00"))

    if (getNintyDaysAgo().compareTo(Timestamp.valueOf("2019-05-18 18:23:11")) < 0) {
      print("1234567")
    }

    println(dropSpecial("110105199711248912"))
    println(dropSpecial("11010519971124891X"))
    println(dropSpecial("11010519971124891x"))
    println(dropSpecial("110105**********12"))


  }

  def getNintyDaysAgo(): Timestamp = {
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dateStr: String = sdf.format(new Date())
    val date: Date = sdf.parse(dateStr)
    val c: Calendar = Calendar.getInstance
    c.setTime(date)
    c.add(Calendar.DATE, -90)
    val newDate: Date = c.getTime
    Timestamp.valueOf(sdf.format(newDate))
  }

}
