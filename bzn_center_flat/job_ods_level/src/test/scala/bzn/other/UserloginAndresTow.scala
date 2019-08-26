package bzn.other


import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}



object UserloginAndresTow {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("UserKPI").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //指定读取数据目录
    val lines: RDD[String] = sc.textFile("D:\\testdata\\log\\undertow-access-logs\\*.log")
    
    // 过滤数据中包含注册信息和登陆信息
    val filetered: RDD[String] = lines.filter(lines=>lines.contains("/users/user/login")
      ||lines.contains("/users/user/register"))

    // 将数据切分，保留时间和地址
    val dateAndurl: RDD[(String, String)] = filetered.mapPartitions(it => {
      //SimpleDateFormat线程不安全，所以使用mapPartitions，一个分区用单独用一个SimpleDateFormat实例
      val dateFormat1 = new SimpleDateFormat("[dd/MMM/yyyy:hh:mm:ss", Locale.UK)
      val dateFormat2 = new SimpleDateFormat("yyyy-MM-dd", Locale.CHINA)

      it.map(line => {

        //切分数据
        val field: Array[String] = line.split(" ")
        //获取时间字段
        val dateStr: String = field(3)
        //获取访问url
        val url: String = field(6)
        // 格式化日期
        val date: Date = dateFormat1.parse(dateStr)
        val logDate: String = dateFormat2.format(date)

        (logDate, url)

      })




    })
   // 将数据缓存
    dateAndurl.cache()
    // 过滤出有注册数据
       val dataAndreg: RDD[(String, String)] = dateAndurl.filter(_._2.equals("/users/user/register"))
   // dataAndreg.foreach(println)
    //按照日期分组聚合数据
    val dataAndregreduced: RDD[(String, Int)] = dataAndreg.map(t=>(t._1,1)).reduceByKey(_+_)

    val registercounts = dataAndregreduced.map(_._2).sum().toInt
    //输出按日期的注册人数
   dataAndregreduced.foreach(t=>println("每天的注册信息统计： " + t._1 +":"+t._2))
     // 输出所有的注册信息

    println("总注册人数=》"+ registercounts)

    //过滤出登陆的人数
       val dataAndlogIn: RDD[(String, String)] = dateAndurl.filter(_._2.equals("/users/user/login"))

    // 按照日期对登陆的人数进行统计和
      val dataAndlogInReduced: RDD[(String, Int)] = dataAndlogIn.map(t=>(t._1,1)).reduceByKey(_+_)

     dataAndlogInReduced.foreach(t=>println("每天的登陆人数:"+t._1+"："+t._2))

    // 总注册人数

    val logInCounts: Int = dataAndlogInReduced.map(_._2).sum().toInt

    println("总登陆人数："+logInCounts)













    }
}

