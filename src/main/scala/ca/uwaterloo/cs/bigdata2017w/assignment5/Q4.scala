package ca.uwaterloo.cs.bigdata2017w.assignment5

import io.bespin.scala.util.Tokenizer
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  * Created by William on 2017-02-21.
  */

object Q4 extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    val date = args.date()
    val input = args.input()
    log.info("Input: " + input)
    log.info("Date: " + date)
    log.info("format: " + (if (args.text()) "text" else "parquet"))
    val conf = new SparkConf().setAppName("Q4")

    val sc = new SparkContext(conf)

    if (args.text()) {

      val itemMap = sc.textFile(input + "/lineitem.tbl")
        .filter(_.split("\\|")(10).startsWith(date))
        .map(line => {
          (line.split("\\|")(0).toInt, 1)
        })
        .reduceByKey(_ + _)
        .collectAsMap()
      val bcItemMap = sc.broadcast(itemMap)

      val orderMap = sc.textFile(input + "/orders.tbl")
        .map(line => {
          val cols = line.split("\\|")
          (cols(1).toInt, bcItemMap.value.getOrElse(cols(0).toInt, 0))
        })
        .reduceByKey(_ + _)
        .collectAsMap()


      bcItemMap.destroy()
      val bcOrderMap = sc.broadcast(orderMap)

      val customerMap = sc.textFile(input + "/customer.tbl")
        .map(line => {
          val cols = line.split("\\|")
          (cols(3).toInt, bcOrderMap.value.getOrElse(cols(0).toInt, 0))
        })
        .reduceByKey(_ + _)
        .collectAsMap()
      bcOrderMap.destroy()
      val bcCustomerMap = sc.broadcast(customerMap)

      sc.textFile(input + "/nation.tbl")
        .map(line => {
          val cols = line.split("\\|")
          (cols(0).toInt, cols(1), bcCustomerMap.value.getOrElse(cols(0).toInt, 0))
        })
        .filter(_._3 > 0)
        .sortBy(_._1)
        .collect()
        .foreach(println(_))


    } else {
      val sparkSession = SparkSession.builder.getOrCreate

      val itemMap = sparkSession.read.parquet(input + "/lineitem/").rdd
        .filter(_.getString(10).startsWith(date))
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
