package bzn.other

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.StringOps
import scala.util.matching.Regex

object UserKpi {
  def main(args: Array[String]): Unit = {
    //读取数据
     val conf = new SparkConf().setAppName("UserKpi").setMaster("local[*]")
     val sc: SparkContext = new SparkContext(conf)
     val data: RDD[String] = sc.textFile("D:\\testdata\\log\\baozhunniu-victory-user-log\\*.gz")
    //过滤出注册用户和登陆新的日志
    //"/users/user/login" "/users/user/register"
    val filtered: RDD[String] = data.filter(dt=>dt.contains("/users/user/register")
      ||dt.contains("/users/user/login"))


    //切个文件，提取字段
    val dataAndurl: RDD[( String, String)] = filtered.map(t => {
      val strings: Array[String] = t.split(" ")
      val data: String = strings(0)
      val lines: String = strings(10)


      (data, lines)
    })
   //  dataAndurl.foreach(println)



    // 过滤出用户登陆
    val filterdataAndlogin: RDD[(String, String)] = dataAndurl.filter(_._2.contains("/users/user/login"))
    val dataAndlogin = filterdataAndlogin.map(t => {
      val str: String = t._2.substring(3, 20)
      (t._1, str)
    })
    //filterdataAndlogin.foreach(println)

    //按日期分组聚合
      val dataAndloginreduce: RDD[(String, Int)] = dataAndlogin
        .map(t=>(t._1,1)).reduceByKey(_+_)
     dataAndloginreduce.foreach(t=>println("每天的总登陆人数:"+t._1+":"+t._2))
    //总登陆人数
    val lonincounts: Int = dataAndloginreduce.map(_._2).sum().toInt
    println("总登陆人数："+lonincounts)

    //过滤出注册人数
    val dataAndregfilter: RDD[(String, String)] = dataAndurl.filter(_._2.contains("/users/user/register"))
    val dataAndreg = dataAndregfilter.map(t => {
      val str: String = t._2.substring(3, 23)
      (t._1, str)
    })
    //dataAndreg.foreach(println)

    //按照日期分组聚合
    val dataAndregreduce: RDD[(String, Int)] = dataAndreg.map(t=>(t._1,1)).reduceByKey(_+_)
    dataAndregreduce.foreach(t=>println("每天注册的总人数："+t._1+":"+t._2))
    // 总注册人数
    val rescounts: Int = dataAndregreduce.map(_._2).sum().toInt
    println("总注册人数："+rescounts)



  }

}
