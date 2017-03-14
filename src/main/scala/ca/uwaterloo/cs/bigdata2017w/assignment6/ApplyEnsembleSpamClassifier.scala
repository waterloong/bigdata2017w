package ca.uwaterloo.cs.bigdata2017w.assignment6

import io.bespin.scala.util.Tokenizer
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.ScallopConf

import collection.mutable

/**
  * Created by William on 2017-03-12.
  */
class EnsembleConf(args: Seq[String]) extends ScallopConf(args) with Tokenizer {
  mainOptions = Seq(input, model, output)
  val input = opt[String](descr = "input path", required = true)
  val model = opt[String](descr = "model", required = true)
  val output = opt[String](descr = "output", required = true)
  val method = opt[String](descr = "method", required = true)
  verify()
}

object ApplyEnsembleSpamClassifier extends Tokenizer {

  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new EnsembleConf(argv)
    val input = args.input()
    val model = args.model()
    val output = args.output()
    val useAverage = args.method() == "average"

    log.info("Input: " + input)
    log.info("Model: " + model)
    log.info("Output: " + output)
    log.info("Method: " + args.method())

    val conf = new SparkConf().setAppName("TrainSpamClassifier")
    val sc = new SparkContext(conf)

    FileSystem.get(sc.hadoopConfiguration).delete(new Path(output), true)

    val classifers = (0 to 2).map(i =>
      sc.textFile(model + "/part-0000" + i)
        .map(line => {
          val cols = line.substring(1, line.length - 1).split(",")
          (cols(0).toInt, cols(1).toDouble)
        }).collectAsMap()
    )

    val bcClassifers = sc.broadcast(classifers)

    sc.textFile(input)
      .map(line => {
        val cols = line.split(" ")
        val w = bcClassifers.value
        val scores = (0 to 2).map(i => cols.slice(2,cols.length).map(_.toInt).map(w(i).getOrElse(_, 0d)).reduce(_ + _))
        val finalScore = if (useAverage) scores.sum / 3 else scores.map(s => {
          if (s > 0) 1 else -1
        }).sum
        (cols(0), cols(1), finalScore, if (finalScore > 0) "spam" else "ham")
      })
      .saveAsTextFile(args.output())
  }
}