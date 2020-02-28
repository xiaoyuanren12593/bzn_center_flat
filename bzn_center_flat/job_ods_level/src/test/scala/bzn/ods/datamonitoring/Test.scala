package bzn.ods.datamonitoring

import bzn.ods.policy.OdsMonitorSourceAndSourceDataDetailTest.{getCentralBaseMonitor, sparkConfInfo}
import bzn.ods.util.Until
import bzn.util.SparkUtil
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

object Test extends  SparkUtil with  Until{
  def main (args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    



  }


  //匹配空格
  def SpaceMatching(Temp: String): Boolean = {
    if (Temp != null) {
      "^\\s|\\s+$  ".r.findFirstIn(Temp).isDefined
    } else false
  }

  //匹配换行符
  def LinefeedMatching(Temp: String): Boolean = {
    if (Temp != null) {
      "^\\n|\\n+$".r.findFirstIn(Temp).isDefined
    } else false
  }




}
