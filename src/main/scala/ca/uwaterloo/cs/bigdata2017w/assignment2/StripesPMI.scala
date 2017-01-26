/**
  * Created by William on 2017-01-19.
  */

package ca.uwaterloo.cs.bigdata2017w.assignment2

import io.bespin.scala.util.Tokenizer
import org.apache.hadoop.fs._
import org.apache.log4j._
import org.apache.spark.{SparkConf, SparkContext}

import scala.math.log10


object StripesPMI extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new ThresholdConf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())
    log.info("Threshold: " + args.threshold())

    val conf = new SparkConf().setAppName("PairsPMI")

    val sc = new SparkContext(conf)

    val threshold = sc.broadcast(args.threshold())

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)


    val textFile = sc.textFile(args.input())
    val wordCount = textFile
      .flatMap(line => {
        val tokens = tokenize(line).distinct.take(40)
        val word = for {
          w <- tokens
        } yield (w, 1)
        word :+ ("*", 1)
      })
      .reduceByKey(_ + _)
      .filter(t => {
          t._2 >= threshold.value
      }).collectAsMap()

    val broadcastedWordCount = sc.broadcast(wordCount)

    val textFile2 = sc.textFile(args.input())
    val stripes = textFile2
      .flatMap(line => {
        val tokens = tokenize(line).distinct.take(40)
        for {
          w1 <- tokens;
          w2 <- tokens if (w1 != w2)
        } yield (w1, Map(w2 -> 1))
      }).reduceByKey((m1, m2) => {
        m1 ++ m2.map { case (k,v) => k -> (v + m1.getOrElse(k,0)) }
      })
      .filter(t => {
        broadcastedWordCount.value.contains(t._1)
      })
      .map(t => {
        val w1 = t._1
        val count1 = broadcastedWordCount.value.get(w1).get
        val sum = broadcastedWordCount.value.get("*").get
        val stripe = t._2.filter(
          entry => {
            entry._2 >= threshold.value
          })
          .map(entry => {
            val w2 = entry._1
            val count2 = broadcastedWordCount.value.get(w2).get
            val count = entry._2
            val pmi = log10(sum.toFloat / count1 / count2 * count)
            s"$w2=($pmi, $count)"
          })
          .mkString(", ")
        if (stripe.size == 0) {
          ""
        } else {
          s"$w1\t{$stripe}"
        }
      }).filter(_.size > 0)

    stripes.saveAsTextFile(args.output())
  }
}