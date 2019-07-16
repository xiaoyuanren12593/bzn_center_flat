package bzn.c_person.centinfo

import java.sql.Timestamp
import java.util

import bzn.job.common.Until
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature


/**
  * author:xiaoYuanRen
  * Date:2019/7/10
  * Time:18:33
  * describe:测试
  **/
object test extends Until{
  def main(args: Array[String]): Unit = {
    val str = "232425"
    println(str.substring(0, 2))
    println(str.substring(2, 4))
    println(str.substring(4, 6))
    println(getAgeFromBirthTime("232325199404143617","2019-07-10 00:00:00"))
    println(dateAddNintyDay("2019-07-10 00:00:00"))
    val first_policy_time_90_days = Timestamp.valueOf(getNowTime())
    println(System.currentTimeMillis())
    println(get_current_date(System.currentTimeMillis()))
    println(first_policy_time_90_days)
    println(getBeg_End_one_two_new("2018-09-18 00:00:00.0".substring(0,19),get_current_date(System.currentTimeMillis()).toString.substring(0,19)))
    println(dateDelNintyDay(get_current_date(System.currentTimeMillis()).toString.substring(0,19)))

    var list:util.List[Any] = new util.ArrayList[Any]()
    var map1 = new util.HashMap[String,Any]()
    map1.put("name","Johnson")
    map1.put("age",18)
    map1.put("hobby","basketball")
    var map2 = new util.HashMap[String,Any]()
    map2.put("name","Jack")
    map2.put("age",20)
    map2.put("hobby","football")
    var map3 = new util.HashMap[String,Any]()
    map3.put("name","Johnson")
    map3.put("age",22)
    map3.put("hobby","swimming")
    list.add(map1)
    list.add(map2)
    list.add(map3)
    val jsonString = JSON.toJSONString(list, SerializerFeature.BeanToArray)
    println(jsonString)

    var ss = new util.ArrayList[(String,String)]
    ss.add(("0011","雇主"))
    ss.add(("0022","骑士"))
    ss.add(("0033","大货车"))
    ss.add(("0033","大货车"))
    val jsonStringNew = JSON.toJSONString(ss, SerializerFeature.BeanToArray)
    println(jsonStringNew)
  }


}
