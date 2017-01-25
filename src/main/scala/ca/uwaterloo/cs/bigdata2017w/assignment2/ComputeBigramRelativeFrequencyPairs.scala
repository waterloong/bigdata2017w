/**
  * Created by William on 2017-01-19.
  */

package ca.uwaterloo.cs.bigdata2017w.assignment2

import io.bespin.scala.util.Tokenizer
import org.apache.hadoop.fs._
import org.apache.log4j._
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop._

class Conf(args: Seq[String]) extends ScallopConf(args) with Tokenizer {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  verify()
}

object ComputeBigramRelativeFrequencyPairs extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf = new SparkConf().setAppName("Bigram Count")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())
    val counts = textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        if (tokens.size > 1) {
          tokens.sliding(2).flatMap(p => List(p(0) + " *", p.mkString(" "))).toList
        } else {
          List()
        }
      })
      .map((bigram: String) => (bigram, 1))
      .reduceByKey(_ + _)
      .map(t => {
        val wpair = t._1.split(" ")
        val w1 = wpair(0)
        val w2 = wpair(1)
        (w1, (w2, t._2))
      })
      .groupByKey()
      .flatMap(t => {
        val g1 = t._2.find(_._1 == "*").get._2.toFloat
        t._2
          .map(w2f => {
            val w1 = t._1
            val w2 = w2f._1
            if (w2 == "*") {
              s"($w1, $w2) $g1"
            } else {
              val g2 = w2f._2 / g1
              s"($w1, $w2) $g2"
            }
          }).toList
        })
    counts.saveAsTextFile(args.output())
  }
}