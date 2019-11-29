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

  case class CardMember(m_id: Int, card_type: String, expire: Timestamp, duration: Int, is_sale: Int, date: Date, user: String, salary: Float,tableName:String)
  case class CardMember1(m_id: Int)
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName(getClass.getSimpleName).set("spark.testing.memory", "3147480000")
    val sparkContext = new SparkContext(conf)
    val hiveContext = new SQLContext(sparkContext)
    import hiveContext.implicits._
    val memberSeq = Seq(
      CardMember(1, "月卡", new Timestamp(System.currentTimeMillis()), 31, 0, new Date(System.currentTimeMillis()), "1259", 0.32f,"member_test"),
      CardMember(2, "季卡", new Timestamp(System.currentTimeMillis()), 31, 0, new Date(System.currentTimeMillis()), "1259", 0.32f,"member_test"),
      CardMember(3, "月卡", new Timestamp(System.currentTimeMillis()), 31, 0, new Date(System.currentTimeMillis()), "1259", 0.32f,"member_test"),
      CardMember(4, "月卡", new Timestamp(System.currentTimeMillis()), 31, 0, new Date(System.currentTimeMillis()), "1259", 0.32f,"member_test"),
      CardMember(5, "季卡", null, 12, 1, null, null, 0.95f,"member_test"),
      CardMember(6, "月卡", new Timestamp(System.currentTimeMillis()), 31, 0, new Date(System.currentTimeMillis()), "1", 0.32f,"member_test"),
      CardMember(7, "季卡", new Timestamp(System.currentTimeMillis()), 31, 0, new Date(System.currentTimeMillis()), "2", 0.32f,"member_test"),
      CardMember(8, "月卡", new Timestamp(System.currentTimeMillis()), 31, 0, new Date(System.currentTimeMillis()), "3", 0.32f,"member_test"),
      CardMember(9, "月卡", new Timestamp(System.currentTimeMillis()), 31, 1, new Date(System.currentTimeMillis()), "45", 0.32f,"member_test"),
      CardMember(10, "月卡", new Timestamp(System.currentTimeMillis()), 31, 0, new Date(System.currentTimeMillis()), "8", 0.66f,"member_test")
      //CardMember(2, "季卡", new Timestamp(System.currentTimeMillis()), 93, false, new Date(System.currentTimeMillis()), 124224, 0.362f)
    )
    val memberDF = memberSeq.toDF().repartition(5)

    val memberSeq1 = Seq(
      CardMember1(1),
      CardMember1(2),
      CardMember1(3),
      CardMember1(4),
      CardMember1(5),
      CardMember1(6),
      CardMember1(7),
      CardMember1(8),
      CardMember1(9),
      CardMember1(10)
      //CardMember(2, "季卡", new Timestamp(System.currentTimeMillis()), 93, false, new Date(System.currentTimeMillis()), 124224, 0.362f)
    )
    val memberDF1 = memberSeq1.toDF().repartition(5)
//    MySQLUtils.saveDFtoDBCreateTableIfNotExist("member_test", memberDF)
    MySQLUtils.insertOrUpdateDFtoDBUsePool("member_test", memberDF, Array("expire","duration","date","card_type","user", "salary"))
//    MySQLUtils.deleteMysqlTableDataBatch(hiveContext: SQLContext,memberDF1, "member_test")
    MySQLUtils.getDFFromMysql(hiveContext, "member_test", null).show()

    sparkContext.stop()
  }
}
