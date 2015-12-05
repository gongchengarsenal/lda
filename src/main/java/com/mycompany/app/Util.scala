package com.mycompany.app

import breeze.numerics.log
import org.apache.spark.rdd.RDD
import epic.preprocess.TreebankTokenizer


object Util {
  /** term id should be positive integer */
  def getVocab(rawCorp: RDD[String]): Map[String, Int] = {
    val fineCorp: RDD[Array[(String, Long)]] = rawCorp.map(rawDoc => segment(rawDoc)).cache()
    val nDoc = fineCorp.count().toDouble

    val tfCounter: Map[String, Double] = fineCorp.flatMap(_.toTraversable)
      .reduceByKey(_ + _).filter(_._2 >= 10).map{ case(term, tf) => (term, log(tf + 1.0)) }.collect().toMap
    val idfCounter: Map[String, Double] = fineCorp.flatMap(fineDoc =>
      fineDoc.map{ case(term, cnt) => (term, 1)}.toTraversable
    ).reduceByKey(_ + _).map{ case(term, df) => (term, log(nDoc / df)) }.collect().toMap

    fineCorp.unpersist()
    tfCounter.map{ case(term, tf) => (term, tf * idfCounter(term)) }
      .toArray.sortWith((t1, t2) => t1._2 > t2._2).map(_._1).take(6000)
      .zipWithIndex.map{ case(term, idx) => (term, idx + 1) }.toMap
  }

  def segment(rawDoc: String): Array[(String, Long)] = {
    val tokenizer = new TreebankTokenizer()
    var ret = Map.empty[String, Long]
    tokenizer(rawDoc.toLowerCase).filter(_.length > 2).foreach(term => {
      ret.get(term) match {
        case Some(cnt) => ret += (term -> (cnt + 1))
        case None => ret += (term -> 1)
      }
    })
    ret.toArray
  }
}


case class Config (
  nTopic: Int,
  maxIter: Int,
  alpha: Double,
  beta: Double
)