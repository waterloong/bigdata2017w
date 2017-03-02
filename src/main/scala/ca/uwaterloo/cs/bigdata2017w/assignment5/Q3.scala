package ca.uwaterloo.cs.bigdata2017w.assignment5

import io.bespin.scala.util.Tokenizer
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  * Created by William on 2017-02-21.
  */

object Q3 extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    val date = args.date()
    val input = args.input()
    log.info("Input: " + input)
    log.info("Date: " + date)
    log.info("format: " + (if (args.text()) "text" else "parquet"))
    val conf = new SparkConf().setAppName("Q3")

    val sc = new SparkContext(conf)

    if (args.text()) {
      val partMap = sc.textFile(input + "/part.tbl")
        .map(line => {
          val cols = line.split("\\|")
          (cols(0).toInt, cols(1))
        }).collectAsMap()
      val bcPartMap = sc.broadcast(partMap)

      val supplierMap = sc.textFile(input + "/supplier.tbl")
        .map(line => {
          val cols = line.split("\\|")
          (cols(0).toInt, cols(1))
        }).collectAsMap()
      val bcSupplierMap = sc.broadcast(supplierMap)

      sc.textFile(input + "/lineitem.tbl")
        .filter(_.split("\\|")(10).startsWith(date))
        .map(line => {
          val cols = line.split("\\|")
          (cols(0).toInt, (bcPartMap.value(cols(1).toInt), bcSupplierMap.value(cols(2).toInt)))
        })
        .sortByKey(true, 1)
        .take(20)
        .foreach(t => {
          println(t._1, t._2._1, t._2._2)
        })
    } else {
      val sparkSession = SparkSession.builder.getOrCreate
      val partMap = sparkSession.read.parquet(input + "/part/").rdd
        .map(r => (r.getInt(0), r.getString(1)))
        .collectAsMap()
      val bcPartMap = sc.broadcast(partMap)

      val supplierMap = sparkSession.read.parquet(input + "/supplier/").rdd
        .map(r => (r.getInt(0), r.getString(1)))
        .collectAsMap()
      val bcSupplierMap = sc.broadcast(supplierMap)

        sparkSession.read.parquet(input + "/lineitem/").rdd
            .filter(_.getString(10).startsWith(date))
            .map(r => {
              (r.getInt(0), (bcPartMap.value(r.getInt(1)), bcSupplierMap.value(r.getInt(2))))
            })
        .sortByKey(true, 1)
        .take(20)
          .foreach(t => {
            println(t._1, t._2._1, t._2._2)
          })
    }

  }
}
