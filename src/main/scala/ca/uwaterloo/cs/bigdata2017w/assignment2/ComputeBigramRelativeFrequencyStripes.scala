/**
  * Created by William on 2017-01-19.
  */

package ca.uwaterloo.cs.bigdata2017w.assignment2

import io.bespin.scala.util.Tokenizer
import org.apache.hadoop.fs._
import org.apache.log4j._
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop._

object ComputeBigramRelativeFrequencyStripes extends Tokenizer {
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
          tokens.sliding(2).map(p => (p(0), Map(p(1) -> 1))).toList
        } else {
          List[(String, Map[String, Int])]()
        }
      })
      .reduceByKey((m1, m2) => {
        m1 ++ m2.map { case (k,v) => k -> (v + m1.getOrElse(k,0)) }
      })
      .map(t => {
        val w1 = t._1
        val g1 = t._2.values.sum.toFloat
        val stripe = t._2.map { case (w2, count) => w2 -> count / g1}
        s"$w1 $stripe"
      })
    counts.saveAsTextFile(args.output())
  }
}