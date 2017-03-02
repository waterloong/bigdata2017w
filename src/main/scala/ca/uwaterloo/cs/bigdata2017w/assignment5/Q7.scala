package ca.uwaterloo.cs.bigdata2017w.assignment5

import io.bespin.scala.util.Tokenizer
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  * Created by William on 2017-02-21.
  */

object Q7 extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    val date = args.date()
    val input = args.input()
    log.info("Input: " + input)
    log.info("Date: " + date)
    log.info("format: " + (if (args.text()) "text" else "parquet"))

    val conf = new SparkConf().setAppName("Q6")
    val sc = new SparkContext(conf)

    if (args.text()) {
      val itemMap = sc.textFile(input + "/lineitem.tbl")
        .filter(_.split("\\|")(10) > date)
        .map(line => {
          val cols = line.split("\\|")
          val extendedPrice = cols(5).toDouble
          val discount = cols(6).toDouble
          val discountedPrice = extendedPrice * (1 - discount)
          (cols(0).toInt, discountedPrice)
        })

      val customerMap = sc.textFile(input + "/customer.tbl")
        .map(line => {
          val cols = line.split("\\|")
          (cols(0).toInt, cols(1))
        })
        .collectAsMap()
      val bcCustomerMap = sc.broadcast(customerMap)

      sc.textFile(input + "/orders.tbl")
        .filter(line => {
          val cols = line.split("\\|")
          cols(4) < date
        })
        .map(line => {
          val cols = line.split("\\|")
          val customerName = bcCustomerMap.value(cols(1).toInt)
          (cols(0).toInt, (customerName, cols(4), cols(7).toInt))
        })
        .cogroup(itemMap)
        .filter(t => {
          t._2._1.nonEmpty && t._2._2.nonEmpty
        })
        .map(t => {
          val item = t._2._1.head
          ((item._1, t._1, item._2, item._3), t._2._2.sum)
        })
        .sortBy(_._2, false, 1)
        .take(10)
        .foreach(t => {
          val key = t._1
          println((key._1, key._2, t._2, key._3, key._4))
        })
    } else {
      val sparkSession = SparkSession.builder.getOrCreate
      val itemMap = sparkSession.read.parquet(input + "/lineitem/").rdd
        .filter(_.getString(10) > date)
        .map(r => {
          val extendedPrice = r.getDouble(5)
          val discount = r.getDouble(6)
          val tax = r.getDouble(7)
          val discountedPrice = extendedPrice * (1 - discount)
          (r.getInt(0), discountedPrice)
        })

      val customerMap = sparkSession.read.parquet(input + "/customer/").rdd
        .map(r => {
          (r.getInt(0), r.getString(1))
        })
        .collectAsMap()
      val bcCustomerMap = sc.broadcast(customerMap)

      sparkSession.read.parquet(input + "/orders/").rdd
        .filter(_.getString(4) < date)
        .map(r => {
          val customerName = bcCustomerMap.value(r.getInt(1))
          (r.getInt(0), (customerName, r.getString(4), r.getInt(7)))
        })
        .cogroup(itemMap)
        .filter(t => {
          t._2._1.nonEmpty && t._2._2.nonEmpty
        })
        .map(t => {
          val item = t._2._1.head
          ((item._1, t._1, item._2, item._3), t._2._2.sum)
        })
        .sortBy(_._2, false, 1)
        .take(10)
        .foreach(t => {
          val key = t._1
          println((key._1, key._2, t._2, key._3, key._4))
        })

    }

  }
}
