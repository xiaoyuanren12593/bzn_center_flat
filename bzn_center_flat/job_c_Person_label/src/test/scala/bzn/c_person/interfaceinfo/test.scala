package bzn.c_person.interfaceinfo

import java.text.SimpleDateFormat
import java.util.Date

import bzn.job.common.Until

object test extends Until{

  def main(args: Array[String]): Unit = {

    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dateStr: String = sdf.format(new Date())

    val d = dateDelNintyDay(dateStr)
    print(d.split(" ")(0))

    val list = List[String]("a", "b", "c", "c")
    print(list.max)

    print("02".toInt)

  }

}
