package bzn.c_person.centinfo

import java.sql.Timestamp
import java.util

import bzn.job.common.Until
import com.alibaba.fastjson.{JSON, JSONArray}
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

    println(currTimeFuction("2019-7-18 09:54:55.0",-180))
    println(getNowTime)
    println(getBeg_End_one_two_new("2017-08-21 11:38:10.0","2015-08-21 11:39:09.0"))

    //[["12000002","亚洲旅游保"]]

    val res2= JSON.parseArray("[[\"12000002\",\"亚洲旅游保\"],[\"12000003\",\"世界旅游保\"]]")
    println(res2)
    res2.add(("12000005","zz旅游保"))
    res2.add(("12000006","旅游保"))
    res2.add(("12000006","旅游保"))
    res2.add(("0",null))

    //13936939565[["12000002","亚洲旅游保"],["12000003","世界旅游保"],["12000005","zz旅游保"],["12000006","旅游保"],["0","2019-4-7"],["0011","雇主"],["0022","骑士"],["0033","大货车"]]
    val zzz = JSON.toJSONString(res2,SerializerFeature.BeanToArray)
    val res3 = res2.fluentAddAll(JSON.parseArray(jsonStringNew)).toArray.distinct
    println ("13936939565"+JSON.toJSONString (res3, SerializerFeature.BeanToArray))
//    val value: AnyRef = JSON.parseArray(zzz).getJSONArray(JSON.parseArray(zzz).size()-1).get(0)
    println()
    println("2313")


    val zz = new util.ArrayList[(String, String)]
    zz.add(("1","201977879"))
    zz.add(("2","201977879"))
    println(JSON.toJSONString(zz, SerializerFeature.BeanToArray))
    println("2019-07-18 09:54:55".length)
    println("2019-7-18 09:54:55.0".compareTo("2019-12-10 09:54:55"))
    val currTime = getNowTime()
    val cuurTimeNew = Timestamp.valueOf(currTime)
    if(cuurTimeNew.compareTo(Timestamp.valueOf("2017-07-18 11:10:11")) < 0){
      println("21231")
    }
    println(Long.MaxValue)
  }


}
