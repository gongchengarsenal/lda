package com.mycompany.app

import breeze.linalg.{sum, DenseVector}
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._

import scala.collection.mutable
import scala.util.Random


class MAPLDAModel {

}


object MAPLDAModel {
  def apply(rawCorp: RDD[String], conf: Config): MAPLDAModel = {
    val vocab = Util.getVocab(rawCorp)

    val edges: RDD[Edge[Short]] = getEdges(rawCorp, vocab).cache()
    var vertexes: VertexRDD[DenseVector[Double]] = getVertexes(edges, conf.nTopic).cache()

    val vertexQueue = new mutable.Queue[VertexRDD[DenseVector[Double]]]()
    vertexQueue.enqueue(vertexes)

    /** concrete vertexes right after the enqueue operation */
    var nk: DenseVector[Double] = vertexes.filter(_._1 < 0).map(_._2)
      .fold(DenseVector.zeros[Double](conf.nTopic))(_ :+ _)

    val mergeMsg: (DenseVector[Double], DenseVector[Double]) => DenseVector[Double] = {
      (msg0, msg1) => msg0 :+ msg1
    }

    for (_ <- 1 to conf.maxIter) {
      val sendMsg: EdgeContext[DenseVector[Double], Short, DenseVector[Double]] => Unit = {
        context => {
          val (w, k) = (conf.nTerm, conf.nTopic)
          val (alpha, beta) = (conf.alpha, conf.beta)

          val ndw = context.attr
          val (nwk, ndk) = (context.srcAttr, context.dstAttr)

          val phiwk: DenseVector[Double] = (nwk :+ (beta - 1)) :/ (nk :+ (w * beta - w))
          val thetadk: DenseVector[Double] = (ndk :+ (alpha - 1.0)) :/ (sum(ndk) + k * alpha  - k)
          val gammadwk: DenseVector[Double] = phiwk :* thetadk

          val ndwk: DenseVector[Double] = gammadwk :/ (sum(gammadwk) / ndw)
          context.sendToSrc(ndwk)
          context.sendToDst(ndwk)
        }
      }

      vertexes = Graph(vertexes, edges)
        .aggregateMessages[DenseVector[Double]](sendMsg, mergeMsg).cache()

      vertexQueue.enqueue(vertexes)
      if (vertexQueue.size == 3) {
        vertexQueue.dequeue().unpersist()
      }

      /** pregal doesn't apply to this iterative procedure duo to the update of nk */
      nk = vertexes.filter(_._1 <= 0).map(_._2)
        .fold(DenseVector.zeros[Double](conf.nTopic))(_ :+ _)
    }

    vertexes.filter(_._1 <= 0).sor
    new MAPLDAModel
  }

  def getEdges(rawCorp: RDD[String], vocab: Map[String, Int]): RDD[Edge[Short]] = {
    rawCorp.zipWithIndex().flatMap { case(line, id) =>
      Util.segment(line).map { case(term, cnt) => Edge(vocab(term), -id, cnt)}
    }
  }

  def getVertexes(edges: RDD[Edge[Short]], k: Int): VertexRDD[DenseVector[Double]] = {
    val tmp: RDD[(VertexId, DenseVector[Double])] = edges.flatMap(edge => {
      val gammadwk = DenseVector.rand[Double](k)
      val ndw = edge.attr
      val ndwk = gammadwk :/ (sum(gammadwk) / ndw)
      Seq((edge.srcId, ndwk), (edge.dstId, ndwk))
    })
    VertexRDD(tmp.reduceByKey(_ :+ _))
  }

}