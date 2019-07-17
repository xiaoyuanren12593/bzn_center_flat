package c_person.highinfo

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import bzn.job.common.Until

object test {

  def main(args: Array[String]): Unit = {



    if (getNintyDaysAgo().compareTo(Timestamp.valueOf("2019-05-18 18:23:11")) < 0) {
      print("1234567")
    }


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
