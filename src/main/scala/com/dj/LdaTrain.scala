package com.dj

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import scala.util.Random

/**
 * Created by jiangyu on 3/18/15.
 */
class LdaTrain(val inputPath:String, val outputPath:String, val topicNumber:Int,
                val iteratorTime:Int, val alpha:Double, val beta:Double,
val minDf:Int) {
  private val conf = new SparkConf().setAppName("LDA")
  private val sc = new SparkContext(conf)
  private var wordsAll:Int = 0

  def init = {
    val initParameters = sc.broadcast(Array(alpha,beta,topicNumber))
    val lines = sc.textFile(inputPath,20)
    val wordsNumber = lines.flatMap(_.split("""\ +""")).distinct(20).count().toInt
    wordsAll = wordsNumber
    val wordsParameters = sc.broadcast(wordsNumber)

    val wftf = lines.map{ line =>
      val random = new Random()
      val number = random.nextInt(20)
      val topicNumber = initParameters.value(2).toInt
      val words = line.split("""\ +""").map { word =>
        val randomTopic = random.nextInt(topicNumber)
        val wordNumber = Integer.parseInt(word)
        (wordNumber,randomTopic)
      }
      (number,words)
    }.partitionBy(new HashPartitioner(20)).persist()

    val wt = wftf.combineByKey[Array[Int]](
      createCombiner = (v: Array[(Int,Int)]) => {
        val result = Array.fill[Int](wordsAll*topicNumber)(0)
        for(i <- 0 until v.length) {
          val index = v(i)._1 * topicNumber + v(i)._2
          result(index) += 1
        }
        result
      },
      mergeValue = (c: Array[Int], v: Array[(Int,Int)]) => {
        for(i <- 0 until v.length) {
          val index = v(i)._1 * topicNumber + v(i)._2
          c(index) += 1
        }
        c
      },
      mergeCombiners = (c1: Array[Int], c2: Array[Int]) => {
        for(i <- 0 until c1.length) {
          c1(i) += c2(i)
        }
        c1
      }
    ).repartition(5).mapPartitions{iter =>
                  val wtLocal = Array.ofDim[Int](1,wordsNumber*topicNumber)
                  for(array <- iter) {
                    for(i <- 0 until array._2.length) {
                      wtLocal(0)(i) += array._2(i)
                    }
                  }
                  wtLocal.toIterator
                }.reduce{case(first,second) =>
                  for(i <- 0 until first.length)
                    first(i) += second(i)
                  first
                }

    println("Docs initialized.")
    (wftf, wt)
  }


  // Begin iterations
  def train(wftf:RDD[(Int,Array[(Int,Int)])],wt:Array[Int]) = {
    println("Begin iteration for "+iteratorTime+" times")
    var wtTransfer:Array[Int] = null

    for(i <- (0 until iteratorTime)) {
      if(i == 0) {
        wtTransfer = wt
      }
      else {
        wtTransfer = wftf.combineByKey[Array[Int]](
          createCombiner = (v: Array[(Int,Int)]) => {
            val result = Array.fill[Int](wordsAll*topicNumber)(0)
            for(i <- 0 until v.length) {
              val index = v(i)._1 * topicNumber + v(i)._2
              result(index) += 1
            }
            result
          },
          mergeValue = (c: Array[Int], v: Array[(Int,Int)]) => {
            for(i <- 0 until v.length) {
              val index = v(i)._1 * topicNumber + v(i)._2
              c(index) += 1
            }
            c
          },
          mergeCombiners = (c1: Array[Int], c2: Array[Int]) => {
            for(i <- 0 until c1.length) {
              c1(i) += c2(i)
            }
            c1
          }
        ).repartition(5).mapPartitions{iter =>
          val wtLocal = Array.ofDim[Int](1,wordsAll*topicNumber)
          for(array <- iter) {
            for(i <- 0 until array._2.length) {
              wtLocal(0)(i) += array._2(i)
            }
          }
          wtLocal.toIterator
        }.reduce{case(first,second) =>
          for(i <- 0 until first.length)
            first(i) += second(i)
          first
        }
      }
      val wtParam = sc.broadcast(wtTransfer.clone())
      wtTransfer = null

      wftf.mapPartitions{iter =>
        val wzMatrix = wtParam.value
        var nzd:Array[Int] = Array.fill[Int](topicNumber)(0)
        val nz:Array[Int] = Array.fill[Int](topicNumber)(0)
        for(i<- 0 until wzMatrix.length) {
          nz(i%topicNumber) += wzMatrix(i)
        }

        var probs = Array.fill[Double](topicNumber)(0.0)
        val random = new Random()

        for(doc <- iter) {
          nzd = Array.fill[Int](topicNumber)(0)
          val length = doc._2.length
          for(i <- 0 until length) {
            nzd(doc._2(i)._2) += 1
          }

          var likelihood = 0.0
          val docSize = doc._2.length

          for(i <- 0 until length) {
            val word = doc._2(i)._1
            val topic = doc._2(i)._2
            nzd(topic) -= 1
            nz(topic) -= 1
            wzMatrix(word*topicNumber + topic ) -= 1
            probs = Array.fill[Double](topicNumber)(0.0)
            likelihood += computeSamplingProbablity(wzMatrix,nzd,nz,word,probs,docSize)
            val nextTopic = sampleDistribution(probs, random)
            nzd(nextTopic) += 1
            nz(nextTopic) += 1
            wzMatrix(word*topicNumber + nextTopic) += 1
            doc._2(i) = (word,nextTopic)
          }
        }

        iter
      }

    }

    sc.makeRDD(wtTransfer).saveAsTextFile(outputPath)

  }

  private def sampleDistribution(probs:Array[Double], random:Random) :Int = {
    val sample = random.nextDouble()
    var sum = 0.0
    val returnTopic = probs.length - 1
    for(i <- (0 until probs.length)) {
      sum += probs(i)
      if(sample < sum)
        return i
    }
    returnTopic
  }

  private def computeSamplingProbablity(nwz:Array[Int],nzd:Array[Int], nz:Array[Int],
                                word:Int,probs:Array[Double],length:Int) : Double = {
    var norm = 0.0
    var likelihood = 0.0
    for(i <-(0 until topicNumber)) {
      val pwz = (nwz(word*topicNumber+i) + beta)  / (nz(i)+nwz.length*beta)
      val pzd = (nzd(i) + alpha) / (length + topicNumber*alpha)
      probs(i) = pwz * pzd
      norm += probs(i)
      likelihood += pwz
    }

    for(i <- 0 until topicNumber) {
      probs(i) /= norm
    }
    likelihood
  }

}
