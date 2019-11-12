package bzn.piwik

import java.sql.{Date,Timestamp}

import bzn.utils.MySQLUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created with IntelliJ IDEA.
  * Author: fly_elephant@163.com
  * Description:DataFrame 中数据存入到MySQL
  * Date: Created in 2018-11-17 12:39
  */
object InsertMysqlDemo {

  case class CardMember(m_id: Int, card_type: String, expire: Timestamp, duration: Int, is_sale: Boolean, date: Date, user: String, salary: Float)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName(getClass.getSimpleName).set("spark.testing.memory", "3147480000")
    val sparkContext = new SparkContext(conf)
    val hiveContext = new SQLContext(sparkContext)
    import hiveContext.implicits._
    val memberSeq = Seq(
      CardMember(1, "月卡", new Timestamp(System.currentTimeMillis()), 31, false, new Date(System.currentTimeMillis()), "1259", 0.32f),
      CardMember(2, "季卡", new Timestamp(System.currentTimeMillis()), 31, false, new Date(System.currentTimeMillis()), "1259", 0.32f),
      CardMember(3, "月卡", new Timestamp(System.currentTimeMillis()), 31, false, new Date(System.currentTimeMillis()), "1259", 0.32f),
      CardMember(4, "月卡", new Timestamp(System.currentTimeMillis()), 31, false, new Date(System.currentTimeMillis()), "1259", 0.32f),
      CardMember(5, "季卡", null, 12, false, null, null, 0.95f)
      //CardMember(2, "季卡", new Timestamp(System.currentTimeMillis()), 93, false, new Date(System.currentTimeMillis()), 124224, 0.362f)
    )
    val memberDF = memberSeq.toDF().repartition(5)
    //MySQLUtils.saveDFtoDBCreateTableIfNotExist("member_test", memberDF)
    MySQLUtils.insertOrUpdateDFtoDBUsePool("member_test", memberDF, Array("expire","duration","date","card_type","user", "salary"))
    MySQLUtils.getDFFromMysql(hiveContext, "member_test", null).show()

    sparkContext.stop()
  }
}
