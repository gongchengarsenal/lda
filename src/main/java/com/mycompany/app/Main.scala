package com.mycompany.app

import org.apache.spark.{SparkConf, SparkContext}


object Main {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("lda").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)

    val rawCrop = sc.textFile("WikiQA.tsv", 5)
    val model = MAPLDAModel(rawCrop, Config(20, 100, 1, 1))
    model.getTopicWord.foreach(ws => System.out.println(ws.mkString(",")))
  }
}