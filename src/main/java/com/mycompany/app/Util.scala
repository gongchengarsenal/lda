package com.mycompany.app

import org.apache.spark.rdd.RDD


object Util {
  /** term id should be positive integer */
  def getVocab(rawCorp: RDD[String]): Map[String, Int] = ???

  def segment(rawDoc: String): Array[(String, Short)] = ???

  def main(args: Array[String]): Unit = {

  }
}


case class Config (
  nTopic: Int,
  nTerm: Int,
  maxIter: Int,
  alpha: Double,
  beta: Double
)
