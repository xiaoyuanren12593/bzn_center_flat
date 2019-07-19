package bzn.c_person.highinfo

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import bzn.job.common.Until
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature

object test extends Until{

  def main(args: Array[String]): Unit = {

    println(Long.MinValue)
    println("2018-06-06 00:00:00.0".substring(0,19).length)
    if(getNowTime().compareTo(("2019-03-07 23:59:59")) <= 0) {
      //在保
      println("135213")
    }
    val currTime = getNowTime()
    val cuurTimeNew = Timestamp.valueOf(currTime)
    var cusType = "2"
    val policyEndDate = Timestamp.valueOf("2019-03-07 23:59:59")
    val policyStartDate = Timestamp.valueOf("2018-03-08 00:00:00")
    val policyNewStartDate = Timestamp.valueOf("2018-03-08 00:00:00")
    val lastCusTypeRes = JSON.parseArray("[[\"1\",\"2018-03-08 00:00:00\"]]")
    var becomeCurrCusTime = "2018-06-06 00:00:00.0"
    if(cusType == "2"){
      if(policyEndDate != null && policyStartDate != null ){
        if(cuurTimeNew.compareTo(policyEndDate) <= 0 && cuurTimeNew.compareTo(policyStartDate) >= 0){//在保
        val days1 = getBeg_End_one_two_new(policyStartDate.toString,policyEndDate.toString)//保障期间
        val days2 = getBeg_End_one_two_new(cuurTimeNew.toString,policyEndDate.toString) //终止天数
        var days3 = Long.MaxValue
          if(policyNewStartDate != null){
            days3 = getBeg_End_one_two_new(policyNewStartDate.toString,cuurTimeNew.toString) //复购天数
          }
          if(days1 >= 30 && days2 <= 60){//长期险
            cusType = "3"
            lastCusTypeRes.add(("2",timeSubstring(becomeCurrCusTime)))
            becomeCurrCusTime = timeSubstring(cuurTimeNew.toString)
          }else if(days1 < 30 && days3 < 30){ //短期险
            cusType = "3"
            lastCusTypeRes.add(("2",timeSubstring(becomeCurrCusTime)))
            becomeCurrCusTime = timeSubstring(cuurTimeNew.toString)
          }
        }else{//不在保
          var days3 = Long.MaxValue
          var easyToLossTime = ""
          if(policyEndDate != null){ //不在保情况
            days3 = getBeg_End_one_two_new(policyEndDate.toString,cuurTimeNew.toString) //复购天数
            println(policyEndDate,cuurTimeNew)
            easyToLossTime = currTimeFuction(policyEndDate.toString,30)
          }
          if(days3 < 30){
            cusType = "3"
            lastCusTypeRes.add(("2",timeSubstring(becomeCurrCusTime)))
            becomeCurrCusTime = timeSubstring(cuurTimeNew.toString)
          }else{  //超过30天时候
            if(easyToLossTime != ""){
              cusType = "3"
              lastCusTypeRes.add(("2",timeSubstring(becomeCurrCusTime)))
              becomeCurrCusTime = timeSubstring(easyToLossTime)
            }
          }
        }
      }
    }
    val jsonString = JSON.toJSONString(lastCusTypeRes, SerializerFeature.BeanToArray)
    println(cusType,jsonString,becomeCurrCusTime)
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
