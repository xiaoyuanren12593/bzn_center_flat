package bzn.c_person.highinfo

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.regex.Pattern
import java.util.{Calendar, Date}

import c_person.util.SparkUtil

object test extends SparkUtil {

  def main(args: Array[String]): Unit = {

    //    初始化设置
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf = sparkConfInfo(appName, "local[*]")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4

    import hiveContext.implicits._
    hiveContext.udf.register("dropSpecial", (line: String) => dropSpecial(line))

    val df = Seq(("110105199711248912", "brr"),("11010519971124891X", "hrr"),("110105********8912", "xxr")).toDF("id","name")

    df.filter("dropSpecial(id) as id").show()


  }

  def dropSpecial(Temp: String): Boolean = {
    val pattern = Pattern.compile("^[\\d]{17}[\\dxX]{1}$")
    pattern.matcher(Temp).matches()
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
