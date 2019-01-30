import sbtassembly.Plugin.AssemblyKeys._

name := "bzn"

version := "0.1"

scalaVersion := "2.10.6"


//resolvers +=
resolvers ++= Seq(
  "Restlet Repositories" at "http://maven.restlet.org", //有些依赖包的地址
  "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"
)

//挡在java项目中写中文时，编译会报错，加上该行就行了
javacOptions ++= Seq("-encoding", "UTF-8")

libraryDependencies ++= Seq(
  //  通过驱动器来实现neo4j(写sql)
  //  "org.neo4j.driver" % "neo4j-java-driver" % "1.0.4",
  "org.neo4j.driver" % "neo4j-java-driver" % "1.0.4" % "provided",

  //  Alibaba-json
  "com.alibaba" % "fastjson" % "1.2.24",

  /**
    * saprk对中文进行分词
    * https://mvnrepository.com/artifact/org.ansj/ansj_seg
    * https://mvnrepository.com/artifact/org.nlpcn/nlp-lang
    **/
  "org.ansj" % "ansj_seg" % "5.1.1",
  "org.nlpcn" % "nlp-lang" % "1.7.2",
  //  "org.ansj" % "ansj_seg" % "5.1.1" % "provided",
  //  "org.nlpcn" % "nlp-lang" % "1.7.2" % "provided",

  //  mysql connect
  "mysql" % "mysql-connector-java" % "5.1.36",
  //  "mysql" % "mysql-connector-java" % "5.1.36" % "provided",

  "joda-time" % "joda-time" % "2.9.9",

  //  redis-client
  "redis.clients" % "jedis" % "2.9.0",
  //  "redis.clients" % "jedis" % "2.9.0" % "provided",

  // ===================================================================
  //        spark-neo4j
   "neo4j-contrib" % "neo4j-spark-connector" % "2.1.0-M4",
    // spark-csv
    "com.databricks" %% "spark-csv" % "1.4.0",
    //  hive
    "org.apache.spark" %% "spark-hive" % "1.6.1",
    // spark-mllib
    "org.apache.spark" % "spark-mllib_2.10" % "1.6.1",
    // sparkStreaming
    "org.apache.spark" %% "spark-streaming" % "1.6.1",
    "org.apache.spark" %% "spark-streaming-kafka" % "1.6.1",
    //  hbase
    "org.apache.hbase" % "hbase-client" % "1.2.0",
    "org.apache.hbase" % "hbase-common" % "1.2.0",
    "org.apache.hbase" % "hbase-server" % "1.2.0",
    "org.apache.hbase" % "hbase-hadoop-compat" % "1.2.0"
  // =========================================================================================

  //    hive
//  "org.apache.spark" %% "spark-hive" % "1.6.1" % "provided",
//  //  spark-neo4j
//  "neo4j-contrib" % "neo4j-spark-connector" % "2.1.0-M4" % "provided",
//  //  spark-csv
//  "com.databricks" %% "spark-csv" % "1.4.0" % "provided",
//  // sparkStreaming
//  "org.apache.spark" %% "spark-streaming" % "1.6.1" % "provided",
//  "org.apache.spark" %% "spark-streaming-kafka" % "1.6.1" % "provided",
//  // spark-mllib
//  "org.apache.spark" % "spark-mllib_2.10" % "1.6.1" % "provided",
//  //  hbase
//  "org.apache.hbase" % "hbase-client" % "1.2.0" % "provided",
//  "org.apache.hbase" % "hbase-common" % "1.2.0" % "provided",
//  "org.apache.hbase" % "hbase-server" % "1.2.0" % "provided",
//  "org.apache.hbase" % "hbase-hadoop-compat" % "1.2.0" % "provided"

).map(
  _.excludeAll(ExclusionRule(organization = "org.mortbay.jetty"))
).map(
  _.excludeAll(ExclusionRule(organization = "javax.servlet"))
)



//这条语句打开了assembly的插件功能
assemblySettings

//执行assembly的时候忽略测试
test in assembly := {}

//指定类的名字
mainClass in assembly := Some("auto.DateManual")

assemblyOption in packageDependency ~= {
  _.copy(appendContentHash = true)
}

/**
  * MergeStrategy.first:默认取得第一个匹配项
  * MergeStrategy.last:默认取得最后一个匹配项
  * MergeStrategy.discard:近丢弃匹配的文件
  **/
//解决依赖重复的问题
mergeStrategy in assembly := {
  //  case x if x.startsWith("META-INF") => MergeStrategy.discard
  //  case x if x.endsWith(".class") => MergeStrategy.discard
  //    case x if x.contains("slf4j-api") => MergeStrategy.last
  //    case x if x.contains("org/cyberneko/html") => MergeStrategy.first
  //如果后缀是.properties的文件，合并策略采用（MergeStrategy.first）第一个出现的文件
  case PathList(ps@_*) if ps.last endsWith ".properties" => MergeStrategy.first
  case PathList(ps@_*) if ps.last endsWith "Absent.class" => MergeStrategy.first
  case PathList(ps@_*) if ps.last endsWith ".xml" => MergeStrategy.first
  case PathList("com", "esotericsoftware", xs@_*) => MergeStrategy.first
  case x =>
    val oldStrategy = (mergeStrategy in assembly).value
    oldStrategy(x)
}
//定义jar包的名字
jarName in assembly := "sbt-solr-assembly.jar"

//把scala本身排除在Jar中，因为spark已经包含了scala
assemblyOption in assembly :=
  (assemblyOption in assembly).value.copy(includeScala = false)
        