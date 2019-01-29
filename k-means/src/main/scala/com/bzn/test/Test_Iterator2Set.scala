package com.bzn.test

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object Test_Iterator2Set {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test").setMaster("local[2]")

    val sc = new SparkContext(conf)

    val hiveContest = new HiveContext(sc)

    val test: RDD[String] = sc.textFile("D:\\idea-poject\\k-means\\src\\main\\scala\\com\\bzn\\test\\ods_policy_detail")

    test.filter(x => {
      val length = x.split("\t").length
      if(length > 2) true else false
    }).map(t => {
      var split = t.split("\t")
      var ent_id = split(0)
      var insure_code = split(1)
      var policy_status = split(2)
      (ent_id,(insure_code,policy_status))
    }).groupByKey.map(x => {
//      x._2.map(z => println(z._1))
//      println("-------------------------")
//      val tuples: Iterable[(String, String)] = x._2.map(z => (z._1,z._2))
      val res: Set[String] = x._2.map(z => z._1).toSet
      println(res.mkString("|"))
      (x._1, res.mkString("|"), "ent_insure_code")
    }).take(100)
  }
}
