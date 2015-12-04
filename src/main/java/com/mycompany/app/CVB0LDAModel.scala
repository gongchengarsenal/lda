package com.mycompany.app

import org.apache.spark.rdd.RDD


class CVB0LDAModel {

}


object CVB0LDAModel {
  def apply(docs: RDD[Document], conf: Config): CVB0LDAModel = {


    new CVB0LDAModel
  }

  def next(model: CVB0LDAModel, docs: RDD[Document]): Unit = {

  }
}


case class Document (
  id: Int,
  terms: Array[(Int, Short)],
  size: Short
)


