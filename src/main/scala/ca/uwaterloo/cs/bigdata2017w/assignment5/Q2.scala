package ca.uwaterloo.cs.bigdata2017w.assignment5

import io.bespin.scala.util.Tokenizer
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.ScallopConf
import org.apache.spark.sql.SparkSession

/**
  * Created by William on 2017-02-21.
  */

object Q2 extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    val date = args.date()
    val input = args.input()
    log.info("Input: " + input)
    log.info("Date: " + date)
    log.info("format: " + (if (args.text()) "text" else "parquet"))
    val conf = new SparkConf().setAppName("Q2")

    val sc = new SparkContext(conf)

    if (args.text()) {
      val items: RDD[Tuple2[Int, Int]] = sc.textFile(input + "/lineitem.tbl")
        .filter(_.split("\\|")(10).startsWith(date))
        .map(line => {
          (line.split("\\|")(0).toInt, 0)
        })
      sc.textFile(args.input() + "/orders.tbl")
            .map(line => {
              val cols = line.split("\\|")
              (cols(0).toInt, cols(6))
            })
        .cogroup(items)
        .filter(_._2._2.nonEmpty)
        .sortByKey(true, 1)
          .take(20)
        .foreach(tuple => {
          println(tuple._2._1.head, tuple._1)
        })
    } else {
      val sparkSession = SparkSession.builder.getOrCreate
      val items = sparkSession.read.parquet(input + "/lineitem/").rdd
        .filter(_.getString(10).startsWith(date))
        .map(r => {
          (r.getInt(0), 0)
        })
      sparkSession.read.parquet(input + "/orders/").rdd
          .map(r => {
            (r.getInt(0), r.getString(6))
          })
        .cogroup(items)
        .filter(!_._2._2.isEmpty)
        .sortByKey(true, 1)
        .take(20)
        .foreach(t => {
          println(t._2._1.head, t._1)
        })
    }

  }
}
