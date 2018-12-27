package io.github.kidokim509

import org.apache.spark.{SparkConf, SparkContext}

/**
  * QUIZ1: JOB이 몇 개가 나올까?
  * QUIZ2: STAGE는 몇 개가 나올까?
  * QUIZ3: reduce는 총 몇 번 계산될까?
  * QUIZ4: rdd의 partition, dependencies 구경하기
  * QUIZ5: 하둡 클러스터와 연결하기
  * (core-, hdfs-, hive-site.xml을 classpath에 추가. 손쉽게는 resources에 추가)
  */
object WordCount extends App {
  val conf = new SparkConf()
  conf.setAppName("SimpleDAG")
  conf.setMaster("local[*]")
  val sc = new SparkContext(conf)
  // sc.setLogLevel("debug")

  /*
  RDD 읽기
   */
  val rdd = sc.textFile("src/main/resources/shakespeare.txt")


  /*
  WORD COUNT
   */
  val words = rdd.flatMap(_.split(" "))
  // 차이를 보자 1)
  // val words = rdd.coalesce(1).flatMap(_.split(" "))
  val wordPairs = words.map((_, 1))
  val wordCounts = wordPairs.reduceByKey(_ + _)

  // 차이를 보자 2)
  // println(wordCounts.filter(_._1.startsWith("A")).count())

  // 차이를 보자 3)
  // wordCounts.cache()

  /*
  ACTION
   */
  println(wordCounts.count())
  wordCounts.take(10).foreach(println)

  // dummy codes for grap webgui
  // 3분의 시간을 드리리다
  Thread.sleep(3 * 60 * 1000)

}
