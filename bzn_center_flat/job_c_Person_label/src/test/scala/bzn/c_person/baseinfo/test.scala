package bzn.c_person.baseinfo

import java.util

import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

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

    println("---------------------------------")

    val string: String = "[{\"base_tel_name\":\"18611428597\",\"base_tel_operator\":\"联通\",\"base_tel_city\":\"北京\",\"base_tel_province\":\"北京\"}, {\"base_tel_name\":\"18611428597\",\"base_tel_operator\":\"联通\",\"base_tel_city\":\"北京\",\"base_tel_province\":\"北京\"}]"
//    val res: Array[Map[String, String]] = parse(string)
    val res: Array[JSONObject] = parse(string)
    val listBuffer: ListBuffer[String] = ListBuffer[(String)]()
    println(res(0).get("base"))
    listBuffer += (("1"))
    listBuffer += ("2")
    println(listBuffer.mkString("   "))

    println("---------------------------------")

    val str: String = "{\"游泳\":\"1\",\"篮球\":\"1\"}"
    val sss = JSON.parseObject(str)
    println(sss.get("游泳"))

    println("---------------------------------")
    val jsonf: JSONObject = new JSONObject()
    jsonf.put("游泳", "1")
    println(jsonf.toString)
    jsonf.put("游泳", "2")
    println(jsonf.toString)
    jsonf.put("游泳", "0")
    println(jsonf.toString)
    jsonf.put("游泳", "cnm")
    println(jsonf.toString)

    println("---------------------------------")
    println("---------------------------------")
    val listd: ListBuffer[String] = mutable.ListBuffer[String]()
    listd += "sjq"
    listd += "lyj"
    listd += "jyl"
    println(listd.mkString(","))


  }

  def parse(string: String): Array[JSONObject] = {
    val parseRes = JSON.parseArray(string)
    parseRes.toArray().map(x => JSON.parseObject(x.toString))
  }



}
