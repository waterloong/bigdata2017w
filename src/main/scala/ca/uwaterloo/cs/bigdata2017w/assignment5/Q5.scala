package ca.uwaterloo.cs.bigdata2017w.assignment5

import io.bespin.scala.util.Tokenizer
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.rogach.scallop.ScallopConf

/**
  * Created by William on 2017-02-21.
  */
class NoDateConf(args: Seq[String]) extends ScallopConf(args) with Tokenizer {
  mainOptions = Seq(input, text, parquet)
  val input = opt[String](descr = "input path", required = true)
  val text = opt[Boolean](descr = "text")
  val parquet = opt[Boolean](descr = "parquet")
  verify()
}
object Q5 extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new NoDateConf(argv)

    val input = args.input()
    log.info("Input: " + input)
    log.info("format: " + (if (args.text()) "text" else "parquet"))
    val conf = new SparkConf().setAppName("Q5")

    val sc = new SparkContext(conf)

    if (args.text()) {
      val itemMap = sc.textFile(input + "/lineitem.tbl")
        .map(line => {
          val cols = line.split("\\|")
          (cols(0).toInt, Map(cols(10).subSequence(0, 7) -> 1))
        })
        .reduceByKey((map1, map2) => {
          map1 ++ map2.map{ case (k,v) => k -> (v + map1.getOrElse(k,0)) }
        })
        .collectAsMap()
      val bcItemMap = sc.broadcast(itemMap)

      val orderMap = sc.textFile(input + "/orders.tbl")
        .map(line => {
          val cols = line.split("\\|")
          val map = bcItemMap.value.getOrElse(cols(0).toInt, Map())
          (cols(1).toInt, map)
        })
        .filter(_._2.nonEmpty)
        .reduceByKey((map1, map2) => {
          map1 ++ map2.map{ case (k,v) => k -> (v + map1.getOrElse(k,0)) }
        })
        .collectAsMap()


      bcItemMap.destroy()
      val bcOrderMap = sc.broadcast(orderMap)

      val customerMap = sc.textFile(input + "/customer.tbl")
        .map(line => {
          val cols = line.split("\\|")
          val map = bcOrderMap.value.getOrElse(cols(0).toInt, Map())
          (cols(3).toInt, map)
        })
        .filter(_._2.nonEmpty)
        .reduceByKey((map1, map2) => {
          map1 ++ map2.map{ case (k,v) => k -> (v + map1.getOrElse(k,0)) }
        })
        .collectAsMap()

      bcOrderMap.destroy()
      val bcCustomerMap = sc.broadcast(customerMap)

      sc.textFile(input + "/nation.tbl")
          .filter(line => {
            val nationKey = line.split("\\|")(0).toInt
            // ca = 3, us = 24
            nationKey == 3 || nationKey == 24
          })
        .flatMap(line => {
          val cols = line.split("\\|")
          bcCustomerMap.value.getOrElse(cols(0).toInt, Map()).view.map { case (k, v) => (cols(1), k, v)}
        })
        .foreach(println(_))

    } else {
      val sparkSession = SparkSession.builder.getOrCreate

      val itemMap = sparkSession.read.parquet(input + "/lineitem/").rdd
        .map(r => {
          (r.getInt(0), 1)
        })
        .reduceByKey(_ + _)
        .collectAsMap()
      val bcItemMap = sc.broadcast(itemMap)

      val orderMap = sparkSession.read.parquet(input + "/orders/").rdd
        .map(r => {
          (r.getInt(1), bcItemMap.value.getOrElse(r.getInt(0).toInt, 0))
        })
        .reduceByKey(_ + _)
        .collectAsMap()


      bcItemMap.destroy()
      val bcOrderMap = sc.broadcast(orderMap)

      val customerMap = sparkSession.read.parquet(input + "/customer/").rdd
        .map(r => {
          (r.getInt(3), bcOrderMap.value.getOrElse(r.getInt(0), 0))
        })
        .reduceByKey(_ + _)
        .collectAsMap()
      bcOrderMap.destroy()
      val bcCustomerMap = sc.broadcast(customerMap)

      sparkSession.read.parquet(input + "/nation/").rdd
        .map(r => {
          (r.getInt(0), r.getString(1), bcCustomerMap.value.getOrElse(r.getInt(0), 0))
        })
        .filter(_._3 > 0)
        .sortBy(_._1)
        .collect()
        .foreach(println(_))

    }

  }
}
