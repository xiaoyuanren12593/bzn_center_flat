package com.bzn.kmeans

import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}


object KmeansTest {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("K-MeansClustering").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rawTrainingData = sc.textFile("D:\\idea-poject\\k-means\\src\\main\\customers_data")
    val parsedTrainingData = rawTrainingData.filter(!isColumnNameLine(_)).map(line => {
      Vectors.dense(line.split(",")
        .map(_.trim).filter(!"".equals(_)).map(_.toDouble)) }).cache()
    val numClusters = 8
    val numIterations = 30
    val runTimes = 3
    var clusterIndex: Int = 0
    val clusters: KMeansModel = KMeans.train(parsedTrainingData, numClusters, numIterations, runTimes)
    println("Cluster Number:" + clusters.clusterCenters.length)
    println("Cluster Centers Information Overview:")
    clusters.clusterCenters.foreach( x => {
      println("Center Point of Cluster " + clusterIndex + ":")
      println(x)
      clusterIndex += 1
    })
    val rawTestData = sc.textFile("D:\\idea-poject\\k-means\\src\\main\\customers_data")
    val parsedTestData = rawTestData.map(line => {
      Vectors.dense(line.split(",")
        .map(_.trim)
        .filter(!"".equals(_)).map(_.toDouble))
    })
    parsedTestData.collect().foreach(testDataLine => {
      val predictedClusterIndex: Int = clusters.predict(testDataLine)
      println("The data " + testDataLine.toString + " belongs to cluster " + predictedClusterIndex)
    })
    println("Spark MLlib K-means clustering test finished.") }
  private def isColumnNameLine(line: String): Boolean = {
    if (line != null && line.contains("Channel"))
      true else false
  }
}
