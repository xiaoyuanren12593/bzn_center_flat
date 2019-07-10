package bzn.c_person.baseinfo

import bzn.c_person.util.SparkUtil
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

/**
  * author:xiaoYuanRen
  * Date:2019/7/10
  * Time:9:27
  * describe: c端标签基础信息类
  **/
object CPersonBaseinfoTest extends SparkUtil with Until {

  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf = sparkConfInfo(appName, "local[*]")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4

    val table = getAllCertNo(hiveContext)
    print(table.count())
    
  }

  /**
    * 获取官网（投保人、被保人）所有的证件号作为全集
    * @param hiveContext
    * @return 证件号DataFrame
    */
  def getAllCertNo(hiveContext: HiveContext): DataFrame = {
    import hiveContext.implicits._

    /**
      * 读取被保险人hive表
      */
    val insuredCertNo: DataFrame = hiveContext.sql("select insured_cert_no, insured_cert_type from odsdb.ods_policy_insured_detail")
      .where("insured_cert_type = '1' and length(insured_cert_no) > 0")
      .selectExpr("insured_cert_no as base_cert_no")

    val slaveCertNo: DataFrame = hiveContext.sql("select slave_cert_no, slave_cert_type from odsdb.ods_policy_insured_slave_detail")
      .where("slave_cert_type = '1' and length(slave_cert_no) > 0")
      .selectExpr("slave_cert_no as base_cert_no")

    val holderCertNo: DataFrame = hiveContext.sql("select holder_cert_no, holder_cert_type from odsdb.ods_holder_detail")
      .where("holder_cert_type = 1 and length(holder_cert_no) > 0")
      .selectExpr("holder_cert_no as base_cert_no")

    val baseCertNo: DataFrame = insuredCertNo
      .unionAll(slaveCertNo)
      .unionAll(holderCertNo)
      .distinct()

    baseCertNo

  }

  /**
    * 获取身份证号及其衍生信息、姓名、邮箱、关系的表格
    * @param hiveContext
    * @return DataFrame
    */
  def getBaseDetail(hiveContext: HiveContext): DataFrame = {
    import hiveContext.implicits._
    hiveContext.udf.register("getEmptyString", () => "")

    hiveContext.sql("select insured_cert_no")



  }


}
