package bzn.c_person.interfaceinfo

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util
import java.util.{Calendar, Collection, Date}

import bzn.job.common.Until

import scala.collection.mutable

object test extends Until{

  def main(args: Array[String]): Unit = {

    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dateStr: String = sdf.format(new Date())

    val d = dateDelNintyDay(dateStr)
    print(d.split(" ")(0))

    val list = List[String]("a", "b", "c", "c")
    print(list.max)

    print("02".toInt)

    val arrayList: util.ArrayList[(String, String, String, String)] = new util.ArrayList[(String, String, String, String)]
    arrayList.add(("a", "b", "c", "d"))
    print(arrayList.toString)

    val res: String = rideTimeStep(mutable.ListBuffer[(String, String, String, String, String)](("1", "1", "01", "1", "1")))
    println("res" + res)

    println("------------------------")
    val sdf2: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dat = sdf2.format(getNintyDaysAgo())
    println(dat)

  }

  def getNintyDaysAgo(): Timestamp = {
    val date_time = "2019-07-18 09:54:05"
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = sdf.parse(date_time)
    val c: Calendar = Calendar.getInstance
    c.setTime(date)
    c.add(Calendar.DATE, -90)
    val newDate: Date = c.getTime
    Timestamp.valueOf(sdf.format(newDate))
  }

}
