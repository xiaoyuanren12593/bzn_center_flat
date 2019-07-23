import sbt._

object Dependencies {

  // 数据库连接 Jar
  val neo4jJavaDriver = "org.neo4j.driver" % "neo4j-java-driver" % "1.0.4"
  val mysqlConnectorJava = "mysql" % "mysql-connector-java" % "5.1.36"
  val jedis = "redis.clients" % "jedis" % "2.9.0"
  //  Alibaba-json
  val fastjson = "com.alibaba" % "fastjson" % "1.2.24"
  // joda-time
  val jodaTime = "joda-time" % "joda-time" % "2.3"
  // spark 中文分词
  val ansjSeg = "org.ansj" % "ansj_seg" % "5.1.1"
  val nlpLang = "org.nlpcn" % "nlp-lang" % "1.7.2"
  //  hive
  val sparkHiveProvided = "org.apache.spark" %% "spark-hive" % "1.6.1" % "provided"
  //  spark-neo4j
  val sparkNeo4jProvided = "neo4j-contrib" % "neo4j-spark-connector" % "2.1.0-M4" % "provided"
  //  spark-csv
  val sparkCsvProvided = "com.databricks" %% "spark-csv" % "1.4.0" % "provided"
  // sparkStreaming
  val sparkStreamingProvided = "org.apache.spark" %% "spark-streaming" % "1.6.1" % "provided"
  val sparkStreamingKafkaProvided = "org.apache.spark" %% "spark-streaming-kafka" % "1.6.1" % "provided"
  // spark-mllib
  val sparkMllibProvided = "org.apache.spark" % "spark-mllib_2.10" % "1.6.1" % "provided"
  //  hbase
  val hbaseClientProvided = "org.apache.hbase" % "hbase-client" % "1.2.0" % "provided"
  val hbaseCommonProvided = "org.apache.hbase" % "hbase-common" % "1.2.0" % "provided"
  val hbaseServerProvided = "org.apache.hbase" % "hbase-server" % "1.2.0" % "provided"
  val hbaseHadoopCompatProvided = "org.apache.hbase" % "hbase-hadoop-compat" % "1.2.0" % "provided"
//  val hiveHbaseHandlerProvied = "org.apache.hive" % "hive-hbase-handler" % "1.2.0" % "provided"


  //  hive
  val sparkHive = "org.apache.spark" %% "spark-hive" % "1.6.1"
  //  spark-neo4j
  val sparkNeo4j = "neo4j-contrib" % "neo4j-spark-connector" % "2.1.0-M4"
  //  spark-csv
  val sparkCsv = "com.databricks" %% "spark-csv" % "1.4.0"
  // sparkStreaming
  val sparkStreaming = "org.apache.spark" %% "spark-streaming" % "1.6.1"
  val sparkStreamingKafka = "org.apache.spark" %% "spark-streaming-kafka" % "1.6.1"
  // spark-mllib
  val sparkMllib = "org.apache.spark" % "spark-mllib_2.10" % "1.6.1"
  //  hbase
  val hbaseClient = "org.apache.hbase" % "hbase-client" % "1.2.0"
  val hbaseCommon = "org.apache.hbase" % "hbase-common" % "1.2.0"
  val hbaseServer = "org.apache.hbase" % "hbase-server" % "1.2.0"
  val hbaseHadoopCompat = "org.apache.hbase" % "hbase-hadoop-compat" % "1.2.0"
  //  graphx
  val sparkGraphX = "org.apache.spark" % "spark-graphx_2.10" % "1.6.0"
//  val hiveHbaseHandler = "org.apache.hive" % "hive-hbase-handler" % "1.2.0"


  //--------------------------------------------全依赖-----------------------------------------------------------------
  val allDeps = Seq(neo4jJavaDriver, mysqlConnectorJava, jedis, fastjson, ansjSeg, nlpLang,
    sparkHive, sparkNeo4j, sparkCsv, sparkStreaming, sparkStreamingKafka, sparkMllib, hbaseClient,
    hbaseCommon, hbaseServer, hbaseHadoopCompat)


  val allDepsProvided = Seq(neo4jJavaDriver, mysqlConnectorJava, jedis, fastjson, ansjSeg, nlpLang,
    sparkHiveProvided, sparkNeo4jProvided, sparkCsvProvided, sparkStreamingProvided,
    sparkStreamingKafkaProvided, sparkMllibProvided, hbaseClientProvided, hbaseCommonProvided, hbaseServerProvided,
    hbaseHadoopCompatProvided)

  //--------------------------------------------工具模块----------------------------------------------------------------
  val utilDeps = Seq(jodaTime,mysqlConnectorJava, fastjson, sparkHiveProvided,hbaseClientProvided,hbaseCommonProvided,hbaseServerProvided,
    hbaseHadoopCompatProvided)

  //--------------------------------------------ods层----------------------------------------------------------------
  val bznOdsLevelDepsProvided = Seq(mysqlConnectorJava, fastjson, sparkHiveProvided,hbaseClientProvided,hbaseCommonProvided,hbaseServerProvided,
    hbaseHadoopCompatProvided)

  //--------------------------------------------dw层----------------------------------------------------------------
  val bznDwLevelDepsProvided = Seq(mysqlConnectorJava, fastjson, sparkHiveProvided,hbaseClientProvided,hbaseCommonProvided,hbaseServerProvided,
    hbaseHadoopCompatProvided)

  //--------------------------------------------dm层----------------------------------------------------------------
  val bznDmLevelDepsProvided = Seq(mysqlConnectorJava, fastjson, sparkHiveProvided,hbaseClientProvided,hbaseCommonProvided,hbaseServerProvided,
    hbaseHadoopCompatProvided)

  //--------------------------------------------c端标签层----------------------------------------------------------------
  val bznCPersonLabelDepsProvided = Seq(mysqlConnectorJava, fastjson, sparkHiveProvided,hbaseClientProvided,hbaseCommonProvided,hbaseServerProvided,
    hbaseHadoopCompatProvided)

  //--------------------------------------------机器学习+图计算----------------------------------------------------------------
  val bznMLLibGraphXDepsProvided = Seq(mysqlConnectorJava, fastjson, sparkHiveProvided, sparkMllib, sparkGraphX)
}
