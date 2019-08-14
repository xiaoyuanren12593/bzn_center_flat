package bzn.c_person.interfaceinfo

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util
import java.util.{Calendar, Collection, Date}

import bzn.c_person.util.SparkUtil
import bzn.job.common.Until
import com.alibaba.fastjson.JSONObject
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object test extends SparkUtil with Until {

  def main(args: Array[String]): Unit = {

    val sdf1: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dateStr: String = sdf1.format(new Date())

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


    println("------------------------")
    println("------------------------")
    println("------------------------")
    val set = mutable.Set[String]()
    set.add("1")
    set.add("1")
    set.add("2")
    set.add("3")

    println(set.mkString(":"))

    val jsons = new JSONObject(true)


    println("------------------------")
    val lists = mutable.ListBuffer[(String, String)](("1", "2018-01-01"), ("1", "2019-01-01"), ("1", "2019-07-01"), ("2", "2019-03-01"), ("3", "2019-08-01"), ("7", "2018-01-01"))
    val res2: Seq[(String, String)] = lists.groupBy(_._1).map(value => value._2.max).toSeq.sortBy(_._2).reverse

    for (r <- res2) {
      jsons.put(r._1, r._2)
    }

    println(jsons.toString)
    println(jsons.keySet().getClass.getName)

    println("------------------------")
    println("------------------------")
    println("------------------------")

    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val currDate = sdf.format(new Date()).split(" ")(0) + " 00:00:00"
    val sevenDate = currTimeFuction(currDate, -7)
    println(currDate)
    println(sevenDate)

    val currMonth = currDate.substring(0, 8) + "01"
    val lastMonth = currMonth.substring(0, 6) + (currMonth.substring(6, 7).toInt - 1).toString + currMonth.substring(7)

    println(currMonth)
    println(lastMonth)

    val table = "select * from open_other_policy where month = '" + lastMonth + "' or month = '" + currMonth + "'"
    println(table)

    val condition = "create_time >= '" + sevenDate + "' and create_time < '" + currDate + "'"
    println(condition)

    println("------------------------")
    println("------------------------")
    println("------------------------")

    val listsss = mutable.ListBuffer[String]()
    listsss += "1-3"
    listsss += "1-3"
    listsss += "3-5"
    listsss += "1-3"
    listsss += "5-7"

    val map = listsss.groupBy(_.toString).mapValues(_.size.toString)
    for (m <- map) {
      print(m._1)
      print(m._2)
      println()
    }

    println("------------------------")
    println("------------------------")
    println("------------------------")

    val str1 = "sjq"
    val str2 = "3"

    println(flat(str1, str2))


    println("1990/01/01 00:00:00.000000".split("\\.")(0).replaceAll("/", "-"))
    println("1990/01/01 00:00:00.000000".contains("."))

    println("------------------------")
    println("------------------------")
    println("------------------------")

    val list5: ListBuffer[(String, String)] = new ListBuffer[(String, String)]
    list5 += (("1-3", "1"))
    list5 += (("1-3", "1"))
    list5 += (("3-5", "1"))

    println(list5.map(v => (v._1, v._2.toInt)).groupBy(_._1).mapValues(x => x.map(_._2).reduce(_ + _)))

    println("------------------------")
    println("------------------------")
    println("------------------------")

    val list6: ListBuffer[String] = new ListBuffer[String]
    list6 += "1-3"
    list6 += "1-3"
    list6 += "3-5"

    println(list6.map((_, 1)).groupBy(_._1).mapValues(_.size.toString))













  }

  def flat(str1: String, str2: String): ListBuffer[String] = {
    var list: ListBuffer[String] = new ListBuffer[String]
    for (s <- 1 to str2.toInt) {
      list += str1
    }
    list
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
