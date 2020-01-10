package bzn.other

import bzn.job.common.{DataBaseUtil, Until}
import bzn.ods.util.SparkUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext

/**
  * author:xiaoYuanRen
  * Date:2020/1/9
  * Time:16:39
  * describe: this is new class
  **/
object test extends SparkUtil with Until with DataBaseUtil{
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName,"local[*]")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4




    hiveContext.sql("select * from  odsdb.ods_risk_extend_detail")
      .withColumnRenamed("policy_code","policy_code_salve")
      .withColumnRenamed("res_pay","res_pay_salve")
      .registerTempTable("temp")

    val data = hiveContext.sql(
      """
        |select a.*,b.* from odsdb.ods_risk_detail a
        |join temp b
        |on a.policy_code = b.policy_code_salve
      """.stripMargin)
    val hdfsPath = "C:\\Users\\xingyuan\\Desktop\\"

    println (data.columns.array.mkString ("`"))

//    sc.parallelize(.repartition(1).saveAsTextFile(hdfsPath + "TestSaveFileName")

    data.show()

    saveAsFileAbsPath(data, hdfsPath + "TestSaveFile", "`", SaveMode.Overwrite)
    sc.stop()
  }

  /**
    * 将 DataFrame 保存为 hdfs 文件 同时指定保存绝对路径 与 分隔符
    *
    * @param dataFrame  需要保存的 DataFrame
    * @param absSaveDir 保存保存的路径 （据对路径）
    * @param splitRex   指定分割分隔符
    * @param saveMode   保存的模式：Append、Overwrite、ErrorIfExists、Ignore
    */
  def saveAsFileAbsPath(dataFrame: DataFrame, absSaveDir: String, splitRex: String, saveMode: SaveMode): Unit = {
    dataFrame.sqlContext.sparkContext.hadoopConfiguration.set("mapred.output.compress", "false")
    //为了方便观看结果去掉压缩格式
    val allClumnName: String = dataFrame.columns.mkString(",")
    val result: DataFrame = dataFrame.selectExpr(s"concat_ws('$splitRex',$allClumnName) as allclumn")
    result.repartition(1).write.mode(saveMode).text(absSaveDir)
  }
}
