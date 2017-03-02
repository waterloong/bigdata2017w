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

object Q6 extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    val date = args.date()
    val input = args.input()
    log.info("Input: " + input)
    log.info("Date: " + date)
    log.info("format: " + (if (args.text()) "text" else "parquet"))

    val conf = new SparkConf().setAppName("Q6")
    if (args.text()) {
      val sc = new SparkContext(conf)
      val tableRdd: RDD[String] = sc.textFile(input + "/lineitem.tbl")
      tableRdd.filter(_.split("\\|")(10).startsWith(date))
        .map(line => {
          val cols = line.split("\\|")
          val quantity = cols(4).toDouble
          val extendedPrice = cols(5).toDouble
          val discount = cols(6).toDouble
          val tax = cols(7).toDouble
          val discountedPrice = extendedPrice * (1 - discount)
          ((cols(8), cols(9)),(quantity, extendedPrice, discountedPrice, discountedPrice * (1 + tax), discount, 1))
        })
        .reduceByKey((t1, t2) => {
          (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3, t1._4 + t2._4, t1._5 + t2._5, t1._6 + t2._6)
        })
        .collect()
        .foreach(t => {
          val key = t._1
          val value = t._2
          val count = value._6
          println((key._1, key._2, value._1, value._2, value._3, value._4, value._1 / count, value._2 / count, value._5 / count, count))
        })
    } else {
      SparkSession.builder.getOrCreate.read.parquet(input + "/lineitem/").rdd
        .filter(_.getString(10).startsWith(date))
        .map(r => {
          val quantity = r.getDouble(4)
          val extendedPrice = r.getDouble(5)
          val discount = r.getDouble(6)
          val tax = r.getDouble(7)
          val discountedPrice = extendedPrice * (1 - discount)
          ((r.getString(8), r.getString(9)),(quantity, extendedPrice, discountedPrice, discountedPrice * (1 + tax), discount, 1))
        })
        .reduceByKey((t1, t2) => {
          (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3, t1._4 + t2._4, t1._5 + t2._5, t1._6 + t2._6)
        })
        .collect()
        .foreach(t => {
          val key = t._1
          val value = t._2
          val count = value._6
          println((key._1, key._2, value._1, value._2, value._3, value._4, value._1 / count, value._2 / count, value._5 / count, count))
        })
    }

  }
}
