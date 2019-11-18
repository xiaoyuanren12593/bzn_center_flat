package bzn.ods.policy

import java.security.MessageDigest
import java.sql.Timestamp
import java.text.{NumberFormat, SimpleDateFormat}
import java.util.{Calendar, Date}

import bzn.job.common.Until
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.math.BigDecimal.RoundingMode
import scala.math.BigDecimal.RoundingMode.RoundingMode

/**
  * author:xiaoYuanRen
  * Date:2019/5/21
  * Time:14:57
  * describe: this is new class
  **/
object test extends Until{
  def main(args: Array[String]): Unit = {
    println (getWeekOfDate ("2019-11-15"))
  }

}

