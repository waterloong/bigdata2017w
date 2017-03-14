package ca.uwaterloo.cs.bigdata2017w.assignment6

import io.bespin.scala.util.Tokenizer
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.ScallopConf
import collection.mutable.HashMap

import scala.util.Random

/**
  * Created by William on 2017-03-12.
  */
class PredictorConf(args: Seq[String]) extends ScallopConf(args) with Tokenizer {
  mainOptions = Seq(input, model, output)
  val input = opt[String](descr = "input path", required = true)
  val model = opt[String](descr = "model", required = true)
  val output = opt[String](descr = "output", required = true)
  verify()
}

object ApplySpamClassifier extends Tokenizer {

  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new PredictorConf(argv)
    val input = args.input()
    val model = args.model()
    val output = args.output()

    log.info("Input: " + input)
    log.info("Model: " + model)
    log.info("Output: " + output)

    val conf = new SparkConf().setAppName("TrainSpamClassifier")
    val sc = new SparkContext(conf)

    FileSystem.get(sc.hadoopConfiguration).delete(new Path(output), true)

    val classfier = sc.textFile(model)
      .map(line => {
        val cols = line.substring(1, line.length - 1).split(",")
        (cols(0).toInt, cols(1).toDouble)
      }).collectAsMap()

    val bcClassifer = sc.broadcast(classfier)

    sc.textFile(input)
      .map(line => {
        val cols = line.split(" ")
        val w = bcClassifer.value
        val score = cols.slice(2,cols.length).map(_.toInt).map(w.getOrElse(_, 0d)).reduce(_ + _)
        (cols(0), cols(1), score, if (score > 0) "spam" else "ham")
      })
      .saveAsTextFile(args.output())
  }
}