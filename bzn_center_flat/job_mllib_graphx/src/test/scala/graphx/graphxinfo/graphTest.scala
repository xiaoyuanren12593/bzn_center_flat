package graphx.graphxinfo

import java.util.regex.Pattern

import bzn.job.common.{HbaseUtil, Until}
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import util.SparkUtil

object graphTest extends SparkUtil with Until with HbaseUtil {

  def main(args: Array[String]): Unit = {

    //    初始化设置
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName: String = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "local[*]")

    val sc: SparkContext = sparkConf._2
    val hiveContext: HiveContext = sparkConf._4

//    清洗图
    val vertex: RDD[(Long, String)] = getVertex(hiveContext)
    val edge: RDD[Edge[String]] = getEdge(hiveContext)

//    创建图
    val graph = createGraph(vertex, edge)

    sc.stop()

  }

  /**
    * 获得顶点信息
    * @param hiveContext
    * @return
    */
  def getVertex(hiveContext: HiveContext): RDD[(Long, String)] = {
    hiveContext.udf.register("dropEmptys", (line: String) => dropEmpty(line))
    hiveContext.udf.register("dropSpecial", (line: String) => dropSpecial(line))

    /**
      * 读取投保人的hive表
      */
    val holderInfo: DataFrame = hiveContext.sql("select holder_cert_no, holder_cert_type from odsdb.ods_holder_detail")
      .where("holder_cert_type = 1 and length(holder_cert_no) = 18")
      .selectExpr("holder_cert_no as cert_no")

    /**
      * 读取被保险人Master的hive表
      */
    val insuredInfo: DataFrame = hiveContext.sql("select insured_cert_no, insured_cert_type from odsdb.ods_policy_insured_detail")
      .where("insured_cert_type = '1' and length(insured_cert_no) = 18")
      .selectExpr("insured_cert_no as cert_no")

    /**
      * 读取被保人Slave的hive表
      */
    val slaveInfo: DataFrame = hiveContext.sql("select slave_cert_no, slave_cert_type from odsdb.ods_policy_insured_slave_detail")
      .where("slave_cert_type = '1' and length(slave_cert_no) = 18")
      .selectExpr("slave_cert_no as cert_no")

//    合并数据
    val vertexRDD: RDD[(Long, String)] = holderInfo
      .unionAll(insuredInfo)
      .unionAll(slaveInfo)
      .filter("dropSpecial(cert_no) as cert_no")
      .dropDuplicates(Array("cert_no"))
      .map(line => {
        val certNo: String = line.getAs[String]("cert_no")
        val vertex: Long = certNo.hashCode.toLong
//        结果
        (vertex, certNo)
      })

//    结果
    vertexRDD

  }

  /**
    * 获取边信息
    * @param hiveContext
    * @return
    */
  def getEdge(hiveContext: HiveContext): RDD[Edge[String]] = {
    hiveContext.udf.register("dropEmptys", (line: String) => dropEmpty(line))
    hiveContext.udf.register("dropSpecial", (line: String) => dropSpecial(line))

    /**
      * 读取投保人的hive表
      */
    val holderInfo: DataFrame = hiveContext.sql("select holder_cert_no, holder_cert_type, policy_id from odsdb.ods_holder_detail")
      .where("holder_cert_type = 1 and length(holder_cert_no) = 18")
      .selectExpr("holder_cert_no", "policy_id as holder_policy_id")
      .filter("dropSpecial(holder_cert_no) as holder_cert_no")

    /**
      * 读取被保险人Master的hive表
      */
    val insuredInfo: DataFrame = hiveContext.sql("select insured_cert_no, insured_cert_type, policy_id, insured_id from odsdb.ods_policy_insured_detail")
      .where("insured_cert_type = '1' and length(insured_cert_no) = 18")
      .selectExpr("insured_cert_no", "policy_id as insured_policy_id", "insured_id")
      .filter("dropSpecial(insured_cert_no) as insured_cert_no")

    /**
      * 读取被保人Slave的hive表
      */
    val slaveInfo: DataFrame = hiveContext.sql("select slave_cert_no, slave_cert_type, master_id  from odsdb.ods_policy_insured_slave_detail")
      .where("slave_cert_type = '1' and length(slave_cert_no) = 18")
      .selectExpr("slave_cert_no", "master_id")
      .filter("dropSpecial(slave_cert_no) as slave_cert_no")

//    连接投保人与被保人
    val edgeRes1: RDD[Edge[String]] = holderInfo
      .join(insuredInfo, holderInfo("holder_policy_id") === insuredInfo("insured_policy_id"))
      .selectExpr("holder_cert_no", "insured_cert_no")
      .map(line => {
        val holderCertNo: String = line.getAs[String]("holder_cert_no")
        val insuredCertNo: String = line.getAs[String]("insured_cert_no")
        val holderEdge: Long = holderCertNo.hashCode().toLong
        val insuredEdge: Long = insuredCertNo.hashCode().toLong
        val valueEdge: String = holderCertNo + " -> " + insuredCertNo
//        结果
        Edge(holderEdge, insuredEdge, valueEdge)
      })

//    连接被保人和从被保人
    val edgeRes2: RDD[Edge[String]] = insuredInfo
      .join(slaveInfo, insuredInfo("insured_id") === slaveInfo("master_id"))
      .selectExpr("insured_cert_no", "slave_cert_no")
      .map(line => {
        val insuredCertNo: String = line.getAs[String]("insured_cert_no")
        val slaveCertNo: String = line.getAs[String]("slave_cert_no")
        val insuredEdge: Long = insuredCertNo.hashCode().toLong
        val slaveEdge: Long = slaveCertNo.hashCode().toLong
        val valueEdge: String = insuredCertNo + " -> " + slaveCertNo
//        结果
        Edge(insuredEdge, slaveEdge, valueEdge)
      })

    val edgeRes = edgeRes1
      .union(edgeRes2)

//    结果
    edgeRes

  }

  /**
    * 创建图
    * @param vertex
    * @param edge
    * @return
    */
  def createGraph(vertex: RDD[(Long, String)], edge: RDD[Edge[String]]): Graph[String, String] = {

    Graph(vertex, edge)

  }

  /**
    * 将空字符串、空值转换为NULL
    * @param Temp
    * @return
    */
  def dropEmpty(Temp: String): String = {
    if (Temp == "" || Temp == "NULL" || Temp == null) null else Temp
  }

  /**
    * 身份证匹配
    * @param Temp
    * @return
    */
  def dropSpecial(Temp: String): Boolean = {
    if (Temp != null) {
      val pattern = Pattern.compile("^[\\d]{17}[\\dxX]{1}$")
      pattern.matcher(Temp).matches()
    } else false
  }

}
