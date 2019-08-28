package bzn.c_person.highinfo

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util
import java.util.regex.Pattern
import java.util.{Calendar, Date}

import bzn.job.common.Until
import c_person.util.SparkUtil

object test extends SparkUtil with Until{


  def main(args: Array[String]): Unit = {

    val str = "我说一个30岁拥有100万的男人"
    val reg = "[^\u4e00-\u9fa5]"
    val reg1 = "[^0-9]"
    println (reg1.replaceAll (reg, "-"))
    //    初始化设置
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf = sparkConfInfo(appName, "local[*]")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4

    import hiveContext.implicits._

    println( getBeg_End_one_two_new("2018-05-02 07:41:54.0","2019-07-23 09:41:54.0"))

//    val days = getBeg_End_one_two_new("2018-05-02 07:41:54.0","2019-07-23 09:41:54.0")
    var cusType = "3"
    var lossTime = ""
    var days = 0L
    if("2018-05-02 07:41:54.0" != null){
      days = getBeg_End_one_two_new("2018-05-02 07:41:54.0","2019-07-23 09:41:54.0")//保险止期和当前日期所差天数
      lossTime = currTimeFuction("2018-05-02 07:41:54.0",60)
    }
    if((cusType == "1" || cusType == "3") && days > 60){
      println(cusType)
      cusType = "4"
      val becomeCurrCusTime = timeSubstring(lossTime)
      println(becomeCurrCusTime)
    }
    println(currTimeFuction("2018-08-31 23:59:59.0", 30))

    val arrayList = new util.ArrayList[(String, String)]
    if(arrayList == null){
      println("null  "+arrayList)
    }else{
      println("not null  "+arrayList)
    }

    if(arrayList.isEmpty){
      println("isEmpty")
    }

    val columnFamily1 = "base_info"
    val column = "cus_type"
    val stagingFolder = s"/hbasehfile/$columnFamily1/$column"
    println(stagingFolder)

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