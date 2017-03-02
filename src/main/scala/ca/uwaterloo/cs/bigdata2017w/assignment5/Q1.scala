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

class Conf(args: Seq[String]) extends ScallopConf(args) with Tokenizer {
  mainOptions = Seq(input, date, text, parquet)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "date", required = true)
  val text = opt[Boolean](descr = "text")
  val parquet = opt[Boolean](descr = "parquet")
  verify()
}

object Q1 extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    val date = args.date()
    val input = args.input()
    log.info("Input: " + input)
    log.info("Date: " + date)
    log.info("format: " + (if (args.text()) "text" else "parquet"))

    val conf = new SparkConf().setAppName("Q1")
    if (args.text()) {
      val sc = new SparkContext(conf)
      val tableRdd: RDD[String] = sc.textFile(input + "/lineitem.tbl")
      val count = tableRdd.map((line: String) => line.split("\\|")(10))
        .filter(_.startsWith(date))
        .count()
      println("ANSWER=" + count)
    } else {
      val count = SparkSession.builder.getOrCreate.read.parquet(input + "/lineitem/").rdd.map(_.getString(10))
        .filter(_.startsWith(date))
        .count()
      println("ANSWER=" + count)
    }

  }
}
