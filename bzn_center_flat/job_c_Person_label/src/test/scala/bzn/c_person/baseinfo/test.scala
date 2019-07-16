package bzn.c_person.baseinfo

import java.util

import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.fastjson.{JSON, JSONObject}

object test {

  def main(args: Array[String]): Unit = {

    val json: JSONObject = new JSONObject()
    json.put("test", null)
    println(json.toString)

    val sjson: JSONObject = new JSONObject()
    sjson.put("test", null)
    json.put("test2", sjson.toString)
    println(json.toString)

    var list:util.List[Any] = new util.ArrayList[Any]()
    var map1: util.HashMap[String, Any] = new util.HashMap[String,Any]()
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

  }

}
