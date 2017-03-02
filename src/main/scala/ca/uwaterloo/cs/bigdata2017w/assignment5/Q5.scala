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
      val nationMap = sc.textFile(input + "/nation.tbl")
        .filter(line => {
          val nationKey = line.split("\\|")(0).toInt
          // ca = 3, us = 24
          nationKey == 3 || nationKey == 24
        })
        .map(line => {
          val cols = line.split("\\|")
          (cols(0).toInt, cols(1))
        }).collectAsMap()
      val bcNationMap = sc.broadcast(nationMap)

      val customerMap = sc.textFile(input + "/customer.tbl")
        .map(line => {
          val cols = line.split("\\|")
          (cols(0).toInt, bcNationMap.value.getOrElse(cols(3).toInt, ""))
        })
        .filter(_._2 != "")
        .collectAsMap()
      val bcCustomerMap = sc.broadcast(customerMap)

      val orderMap = sc.textFile(input + "/orders.tbl")
        .map(line => {
          val cols = line.split("\\|")
          (cols(0).toInt, bcCustomerMap.value.getOrElse(cols(1).toInt, ""))
        })
        .filter(_._2 != "")
        .collectAsMap()
      bcCustomerMap.destroy()
      val bcOrderMap = sc.broadcast(orderMap)

      sc.textFile(input + "/lineitem.tbl")
        .map(line => {
          val cols = line.split("\\|")
          (bcOrderMap.value.getOrElse(cols(0).toInt, ""), cols(10).substring(0, 7), 1)
        })
        .filter(_._1 != "").map(t => {
        ((t._1, t._2), t._3)
      })
        .reduceByKey(_ + _)
        .collect()
        .foreach(t => {
          println((t._1._1, t._1._2, t._2))
        })

    } else {
      val sparkSession = SparkSession.builder.getOrCreate
      val nationMap = sparkSession.read.parquet(input + "/nation/").rdd
        .filter(r => {
          val nationKey = r.getInt(0)
          // ca = 3, us = 24
          nationKey == 3 || nationKey == 24
        })
        .map(r => {
          (r.getInt(0), r.getString(1))
        }).collectAsMap()
      val bcNationMap = sc.broadcast(nationMap)

      val customerMap = sparkSession.read.parquet(input + "/customer/").rdd
        .map(r => {
          (r.getInt(0), bcNationMap.value.getOrElse(r.getInt(3), ""))
        })
        .filter(_._2 != "")
        .collectAsMap()
      val bcCustomerMap = sc.broadcast(customerMap)

      val orderMap = sparkSession.read.parquet(input + "/orders/").rdd
        .map(r => {
          (r.getInt(0), bcCustomerMap.value.getOrElse(r.getInt(1), ""))
        })
        .filter(_._2 != "")
        .collectAsMap()
      bcCustomerMap.destroy()
      val bcOrderMap = sc.broadcast(orderMap)

      sparkSession.read.parquet(input + "/lineitem/").rdd
        .map(r => {
          (bcOrderMap.value.getOrElse(r.getInt(0), ""), r.getString(10).substring(0, 7), 1)
        })
        .filter(_._1 != "").map(t => {
        ((t._1, t._2), t._3)
      })
        .reduceByKey(_ + _)
        .collect()
        .foreach(t => {
          println((t._1._1, t._1._2, t._2))
        })
      }


    }
  }
