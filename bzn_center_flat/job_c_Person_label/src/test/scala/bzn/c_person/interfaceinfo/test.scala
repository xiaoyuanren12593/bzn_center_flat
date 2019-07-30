package bzn.c_person.interfaceinfo

import java.text.SimpleDateFormat
import java.util
import java.util.Date
import java.util.Collection

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

  }

}
