package bzn.c_person.highinfo

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

object test {

  def main(args: Array[String]): Unit = {

    println(getNintyDaysAgo())

  }

  /**
    * 获得当前时间九十天前的时间戳
    * @return
    */
  def getNintyDaysAgo(): Timestamp = {
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dateStr: String = sdf.format(new Date())
    val date: Date = sdf.parse(dateStr)
    val c: Calendar = Calendar.getInstance
    c.setTime(date)
    c.add(Calendar.DATE, -90)
    val newDate: Date = c.getTime
    println(sdf.format(newDate))
    Timestamp.valueOf(sdf.format(newDate))
  }

}
