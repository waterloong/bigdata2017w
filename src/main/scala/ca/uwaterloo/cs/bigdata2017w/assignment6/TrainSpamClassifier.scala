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
class TrainerConf(args: Seq[String]) extends ScallopConf(args) with Tokenizer {
  mainOptions = Seq(input, model, shuffle)
  val input = opt[String](descr = "input path", required = true)
  val model = opt[String](descr = "model", required = true)
  val shuffle = opt[Boolean](descr = "shuffle the data")
  verify()
}

object TrainSpamClassifier extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new TrainerConf(argv)
    val input = args.input()
    val model = args.model()
    val shuffle = args.shuffle()

    log.info("Input: " + input)
    log.info("Model: " + model)
    log.info("Shuffle: " + shuffle)

    val conf = new SparkConf().setAppName("TrainSpamClassifier")
    val sc = new SparkContext(conf)

    FileSystem.get(sc.hadoopConfiguration).delete(new Path(model), true)

    var trainingSet = sc.textFile(input)

    if (shuffle) {
      trainingSet = trainingSet.map((Random.nextInt(), _))
        .sortByKey()
        .map(_._2)
    }

    // w is the weight vector (make sure the variable is within scope)
    val w = HashMap[Int, Double]()

    // Scores a document based on its list of features.
    def spamminess(features: Array[Int]) : Double = {
      var score = 0d
      features.foreach(f => if (w.contains(f)) score += w(f))
      score
    }

    // This is the main learner:
    val delta = 0.002

    trainingSet.map(
      line => {
        val cols = line.split("\\s+")
        val response = if (cols(1) == "spam") 1 else 0
        (0, (cols(0), response, cols.slice(2, cols.length).map(_.toInt)))
      })
      .groupByKey(1)
      .flatMap(t => {
        t._2.foreach(t2 => {
          val score = spamminess(t2._3)
          val prob = 1.0 / (1 + math.exp(-score))
          t2._3.foreach(f => {
            if (w.contains(f)) {
              w(f) += (t2._2 - prob) * delta
            } else {
              w(f) = (t2._2 - prob) * delta
            }
          })
        })
        w
      })
      .saveAsTextFile(model)
  }
}