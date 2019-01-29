package before

import com.alibaba.fastjson.{JSON, JSONObject}
import kafka.serializer.StringDecoder
import org.apache.log4j.Logger
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

/**
  * Created by MK on 2018/6/1.
  */
object Streaming_predict {
  val logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    val redisHost = "172.16.11.103"
    val redisPort = 6379

    /**
      * init sparkStream couchbase kafka
      **/
      //client提交到spark master
    val conf = new SparkConf().setAppName("MLlib_predict")
//      .setMaster("spark://namenode2.cdh:7077")
//      .setMaster("local[2]")
    val sc = new SparkContext(conf)
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(1))

    val hiveContext = new HiveContext(sc)
    hiveContext.sql("set hive.exec.dynamic.partition.mode = nonstrict")
    import hiveContext.implicits._

    /**
      * kafka conf
      **/
    val kafkaParam: Map[String, String] = Map[String, String](
      //-----------kafka低级api配置-----------
      "zookeeper.connect" -> "namenode2.cdh:2181,datanode3.cdh:2181,namenode1.cdh:2181", //----------配置zookeeper-----------
      "metadata.broker.list" -> "namenode1.cdh:9092",
      "group.id" -> "spark_MLlib", //设置一下group id
      //      "auto.offset.reset" -> kafka.api.OffsetRequest.LargestTimeString, //----------从该topic最新的位置开始读数------------
      "client.id" -> "spark_MLlib",
      "zookeeper.connection.timeout.ms" -> "10000"
    )
    //加载报案时效的模型："hdfs://namenode1.cdh:8020/model/risk_level"
    //                val risk_level_model = KMeansModel.load(sc, "hdfs://namenode1.cdh:8020/model/risk_level")
    val risk_level_model = KMeansModel.load(sc, "/model/risk_level")
    val topicSet: Set[String] = Set("enter_model")
    val directKafka: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParam, topicSet)
    val lines: DStream[(String, String)] = directKafka.map((x: (String, String)) => (x._1, x._2))
    // kafka取出的数据，_1是其topic，_2是消息
    lines.foreachRDD(rdds => {
      if (!rdds.isEmpty()) {
        //        logger.info(s"读取kafka数据:${System.currentTimeMillis()}")
        println(s"读取kafka数据:${System.currentTimeMillis()}")
        val vectors = rdds.map(x => {
          val value = JSON.parseObject(x._2)
          //报案时效
          val reportHours = value.getString("reportHours")
          //出险天数
          val riskDays = value.getString("riskDays")
          //赔付金额
          val preCompensation = value.getString("preCompensation")
          //报案时效特征(0-2)
          val risk_level_features = Vectors.dense(Array(reportHours, riskDays, preCompensation).map(_.toDouble))

          //险情类别
          val injury = value.getString("injury")

          //企业价值
          val entValueScore = value.getString("entValueScore")
          //企业风险等级(1-3)
          val entRiskLevel = value.getString("entRiskLevel")

          //被保人风险等级
          val insuredRiskLevel_before = value.getString("insuredRiskLevel").toInt
          val insuredRiskLevel = if (insuredRiskLevel_before == 1 || insuredRiskLevel_before == 2) 1
          else if (insuredRiskLevel_before == 5 || insuredRiskLevel_before == 6) 2
          else 3

          //保单赔付率
          val policyPayRate = value.getString("policyPayRate")

          val requestId = value.getString("requestId")
          val policyId = value.getString("policyId")
          val entId = value.getString("entId")
          val insuredCertNo = value.getString("insuredCertNo")

          (
            risk_level_features, entValueScore,
            entRiskLevel, insuredRiskLevel,
            policyPayRate, requestId,
            policyId, entId,
            insuredCertNo,
            injury
          )
        }).toDF("risk_level_features", "entValueScore", "entRiskLevel", "insuredRiskLevel", "policyPayRate", "requestId", "policyId", "entId", "insuredCertNo", "injury")
        val scaler = new StandardScaler().setInputCol("risk_level_features").setOutputCol("scala_risk_level_features").setWithStd(true).setWithMean(true)
        val scalerModel = scaler.fit(vectors)
        val scaledData = scalerModel.transform(vectors)
        scaledData.foreachPartition(fea => {
          val redisClient = new Jedis(redisHost, redisPort)
          redisClient.auth("dmp@123$%^")
          redisClient.select(1)
          val redis_json_value = new JSONObject()
          fea.foreach(x => {
            //injury
            val injury = x.getAs[String]("injury")

            val scala_risk_level_features = x.getAs[org.apache.spark.mllib.linalg.Vector]("scala_risk_level_features")
            //报案时效,预测结果(0-2)
            val scala_risk_level_features_end = risk_level_model.predict(scala_risk_level_features)

            //riskInfoLevel(1-3)
            val riskInfoLevel = if (injury == "I005" || injury == "I006") 3 else scala_risk_level_features_end + 1

            //requestId
            val requestId = x.getAs("requestId").toString
            //policyPayRate
            val policyPayRate = x.getAs("policyPayRate").toString
            //entRiskLevel(1-3)
            val entRiskLevel = x.getAs("entRiskLevel").toString

            //entValueScore
            val entValueScore = x.getAs("entValueScore").toString.toDouble

            //insuredRiskLevel(1-3)
            val insuredRiskLevel = x.getAs("insuredRiskLevel").toString


            val one_one = if (riskInfoLevel == 1) 0 else if (riskInfoLevel == 2) 50 else 100
            val two_two = if (insuredRiskLevel == "1") 0 else if (insuredRiskLevel == "2") 50 else 100


            val four_four = if (entRiskLevel == "1") 0 else if (entRiskLevel == "2") 50 else 100
            val score = (one_one * 0.25) + (two_two * 0.25) + (four_four * 0.30) + (policyPayRate.toDouble * 100 * 0.20)
            val score_end = score - (entValueScore * 0.10)

            val conclusion = if (score_end > 0.0 && score_end <= 25) "从宽审核" else if (score_end > 25 && score_end <= 50) "正常审核"
            else if (score_end > 50 && score_end <= 75) "审慎审核" else if (score_end > 75 && score_end <= 100) "严格审核"

            //key
            val key = s"enter_model_res_$requestId"
            redis_json_value.put("requestId", requestId)
            redis_json_value.put("policyPayRate", policyPayRate)
            redis_json_value.put("entRiskLevel", entRiskLevel)
            redis_json_value.put("entValueScore", entValueScore)
            redis_json_value.put("insuredRiskLevel", insuredRiskLevel)
            redis_json_value.put("riskInfoLevel", riskInfoLevel)
            redis_json_value.put("score", score)
            redis_json_value.put("conclusion", conclusion)
            redis_json_value.put("million", System.currentTimeMillis)
            redisClient.setex(key, 600, redis_json_value.toString)
            //            println(redis_json_value.toString)

          })
          redisClient.close()
        })
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
