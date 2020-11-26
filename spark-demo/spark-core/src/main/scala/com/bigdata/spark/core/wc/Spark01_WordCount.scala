package com.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_WordCount {
  def main(args: Array[String]): Unit = {
    // 建立与SPark框架的连接
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    // 读取文件，获取行数据
    val lines: RDD[String] = sc.textFile("datas")
    // 拆分行数据，形成单词
    val words = lines.flatMap(_.split(" "))
    // 转换数据结构
    val wordToOne = words.map(word => (word, 1))
    // 将相同单词进行分组聚合
    val wordToCount = wordToOne.reduceByKey(_ + _)
    // 将数据聚合采集到内存中
    val array: Array[(String, Int)] = wordToCount.collect()
    // 打印结果
    array.foreach(println(_))
    // 关闭连接
    sc.stop
  }
}
