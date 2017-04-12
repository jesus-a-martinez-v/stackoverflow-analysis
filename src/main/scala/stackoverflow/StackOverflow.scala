package stackoverflow

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import annotation.tailrec
import scala.reflect.ClassTag


/** The main class */
object StackOverflow extends StackOverflow {

  lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("StackOverflow")
  lazy val sc: SparkContext = new SparkContext(conf)

  /** Main function */
  def main(args: Array[String]): Unit = {

    val lines   = sc.textFile("src/main/resources/stackoverflow/stackoverflow.csv")
    val raw     = rawPostings(lines)
    val grouped = groupedPostings(raw)
    val scored  = scoredPostings(grouped)
    val vectors = vectorPostings(scored)

    assert(vectors.count() == 2121822, "Incorrect number of vectors: " + vectors.count())

    val means   = kmeans(sampleVectors(vectors), vectors, debug = true)
    val results = clusterResults(means, vectors)
    printResults(results)
  }
}


/** The parsing and kmeans methods */
class StackOverflow extends Serializable {

  /** Languages */
  val langs =
    List("JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS", "Objective-C", "Perl", "Scala",
      "Haskell", "MATLAB", "Clojure", "Groovy")

  /** K-means parameter: How "far apart" languages should be for the kmeans algorithm? */
  def langSpread = 50000
  assert(langSpread > 0, "If langSpread is zero we can't recover the language from the input data!")

  /** K-means parameter: Number of clusters */
  def kmeansKernels = 45

  /** K-means parameter: Convergence criteria */
  def kmeansEta: Double = 20.0D

  /** K-means parameter: Maximum iterations */
  def kmeansMaxIterations = 120


  // Parsing utilities:

  /** Load postings from the given file */
  def rawPostings(lines: RDD[String]): RDD[Posting] =
    lines.map(line => {
      val tokens: Array[String] = line.split(",")
      Posting(postingType = tokens(0).toInt,
              id = tokens(1).toInt,
              acceptedAnswer = if (tokens(2) == "") None else Some(tokens(2).toInt),
              parentId = if (tokens(3) == "") None else Some(tokens(3).toInt),
              score = tokens(4).toInt,
              tags = if (tokens.length >= 6) Some(tokens(5).intern()) else None)
    })


  /**
    * Group the questions and answers together
    * */
  def groupedPostings(postings: RDD[Posting]): RDD[(Int, Iterable[(Posting, Posting)])] = {
    val questions: RDD[(Int, Posting)] = postings.collect {
      case posting if posting.postingType == 1 =>
        (posting.id, posting)
    }

    val answers: RDD[(Int, Posting)] = postings.collect {
      case posting if posting.postingType == 2 && posting.parentId.isDefined =>
        (posting.parentId.get, posting)
    }

    questions.join(answers).groupByKey()
  }


  /** Compute the maximum score for each posting */
  def scoredPostings(grouped: RDD[(Int, Iterable[(Posting, Posting)])]): RDD[(Posting, Int)] = {

    def answerHighScore(as: Array[Posting]): Int = as.maxBy(_.score).score

    grouped.map {
      case (_, questionsAndAnswers) =>
        val question = questionsAndAnswers.head._1
        val answers: Iterable[Posting] = questionsAndAnswers.map(_._2)

        (question, answerHighScore(answers.toArray))
    }
  }


  /** Compute the vectors for the kmeans */
  def vectorPostings(scored: RDD[(Posting, Int)]): RDD[(Int, Int)] = {
    /** Return optional index of first language that occurs in `tags`. */
    def firstLanguageInTag(tag: Option[String], ls: List[String]): Option[Int] = {
      if (tag.isEmpty) None
      else if (ls.isEmpty) None
      else if (tag.get == ls.head) Some(0) // index: 0
      else {
        val tmp = firstLanguageInTag(tag, ls.tail)
        tmp match {
          case None => None
          case Some(i) => Some(i + 1) // index i in ls.tail => index i+1
        }
      }
    }

    scored.collect {
      case (posting, score) if posting.tags.isDefined =>
        (firstLanguageInTag(posting.tags, langs).get * langSpread, score)
    }.cache()
  }


  /** Sample the vectors */
  def sampleVectors(vectors: RDD[(Int, Int)]): Array[(Int, Int)] = {

    assert(kmeansKernels % langs.length == 0, "kmeansKernels should be a multiple of the number of languages studied.")
    val perLang = kmeansKernels / langs.length

    // http://en.wikipedia.org/wiki/Reservoir_sampling
    def reservoirSampling(language: Int, iterator: Iterator[Int], size: Int): Array[Int] = {
      val res = new Array[Int](size)
      val rnd = new util.Random(language)

      for (i <- 0 until size) {
        assert(iterator.hasNext, s"iterator must have at least $size elements")
        res(i) = iterator.next
      }

      var i = size.toLong
      while (iterator.hasNext) {
        val elt = iterator.next
        val j = math.abs(rnd.nextLong) % i
        if (j < size)
          res(j.toInt) = elt
        i += 1
      }

      res
    }

    val res =
      if (langSpread < 500) {
        // sample the space regardless of the language
        vectors.takeSample(withReplacement = false, num = kmeansKernels, seed = 42)
      } else {
        // sample the space uniformly from each language partition
        vectors.groupByKey.flatMap({
          case (lang, vectors) => reservoirSampling(lang, vectors.toIterator, perLang).map((lang, _))
        }).collect()
      }

    assert(res.length == kmeansKernels, res.length)
    res
  }



  //  Kmeans method:

  /** Main kmeans computation */
  @tailrec final def kmeans(means: Array[(Int, Int)], vectors: RDD[(Int, Int)], currentIteration: Int = 1, debug: Boolean = false): Array[(Int, Int)] = {

    val newMeans: Array[(Int, Int)] = means.clone()
    val vectorsPairedWithCluster: RDD[(Int, (Int, Int))] = vectors.map(vector => (findClosest(vector, means), vector))
    vectorsPairedWithCluster
      .groupByKey()
      .mapValues(averageVectors)
      .collect()
      .foreach { case (index, newMean) => newMeans(index) = newMean }

    val distance = euclideanDistance(means, newMeans)

    if (debug) {
      println(s"""Iteration: $currentIteration
                 |  * current distance: $distance
                 |  * desired distance: $kmeansEta
                 |  * means:""".stripMargin)
      for (idx <- 0 until kmeansKernels)
      println(f"   ${means(idx).toString}%20s ==> ${newMeans(idx).toString}%20s  " +
              f"  distance: ${euclideanDistance(means(idx), newMeans(idx))}%8.0f")
    }

    if (converged(distance))
      newMeans
    else if (currentIteration < kmeansMaxIterations)
      kmeans(newMeans, vectors, currentIteration + 1, debug)
    else {
      println("Reached max iterations!")
      newMeans
    }
  }


  //  Kmeans utilities:

  /** Decide whether the kmeans clustering converged */
  def converged(distance: Double): Boolean =
    distance < kmeansEta


  /** Return the euclidean distance between two points */
  def euclideanDistance(v1: (Int, Int), v2: (Int, Int)): Double = {
    val part1 = (v1._1 - v2._1).toDouble * (v1._1 - v2._1)
    val part2 = (v1._2 - v2._2).toDouble * (v1._2 - v2._2)
    part1 + part2
  }

  /** Return the euclidean distance between two array of points */
  def euclideanDistance(a1: Array[(Int, Int)], a2: Array[(Int, Int)]): Double = {
    assert(a1.length == a2.length)
    var sum = 0d
    var idx = 0
    while(idx < a1.length) {
      sum += euclideanDistance(a1(idx), a2(idx))
      idx += 1
    }
    sum
  }

  /** Return the closest point */
  def findClosest(p: (Int, Int), centers: Array[(Int, Int)]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity
    for (i <- centers.indices) {
      val tempDist = euclideanDistance(p, centers(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }
    bestIndex
  }


  /** Average the vectors */
  def averageVectors(points: Iterable[(Int, Int)]): (Int, Int) = {
    val iterator = points.iterator
    var count = 0
    var comp1: Long = 0
    var comp2: Long = 0
    while (iterator.hasNext) {
      val item = iterator.next
      comp1 += item._1
      comp2 += item._2
      count += 1
    }
    ((comp1 / count).toInt, (comp2 / count).toInt)
  }


  def calculateMedian(s: Seq[Int]): Int = {
    val (lower, upper) = s.sortWith(_ < _).splitAt(s.size / 2)
    if (s.size % 2 == 0) (lower.last + upper.head) / 2 else upper.head
  }


  //  Displaying results:

  def clusterResults(means: Array[(Int, Int)], vectors: RDD[(Int, Int)]): Array[(String, Double, Int, Int)] = {
    val closest: RDD[(Int, (Int, Int))] = vectors.map(p => (findClosest(p, means), p))
    val closestGrouped: RDD[(Int, Iterable[(Int, Int)])] = closest.groupByKey()

    val median = closestGrouped.mapValues { vs =>
      val mostCommonLanguage = vs.maxBy(_._1)._1
      val mostCommonLanguageIndex =  mostCommonLanguage / langSpread
      val mostCommonLanguageLabel: String   = langs(mostCommonLanguageIndex) // most common language in the cluster
      val clusterSize: Int    = vs.size
      val langPercent: Double = vs.count(_._1 == mostCommonLanguage) / clusterSize  * 100// percent of the questions in the most common language
      val scores: List[Int] = vs.map(_._2).toList
      val medianScore: Int = calculateMedian(scores)

      (mostCommonLanguageLabel, langPercent, clusterSize, medianScore)
    }

    median.collect().map(_._2).sortBy(_._4)
  }

  def printResults(results: Array[(String, Double, Int, Int)]): Unit = {
    println("Resulting clusters:")
    println("  Score  Dominant language (%percent)  Questions")
    println("================================================")
    for ((lang, percent, size, score) <- results)
      println(f"$score%7d  $lang%-17s ($percent%-5.1f%%)      $size%7d")
  }
}
