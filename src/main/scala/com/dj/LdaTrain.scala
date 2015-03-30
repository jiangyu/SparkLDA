package com.dj

import org.apache.spark.rdd.RDD
import org.apache.spark.{AccumulatorParam, SparkContext, SparkConf,Accumulable}
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

    val wftf = lines.mapPartitions{iterator =>
      val iter = iterator.toIterable
      val random = new Random()
      val topicNumber = initParameters.value(2).toInt

      val result = iter.map{ case (line) =>
        val words = line.split("""\ +""").map{word =>
          val randomTopic = random.nextInt(topicNumber)
          val wordNumber = Integer.parseInt(word)
          println(wordNumber+" "+randomTopic)
          (wordNumber,randomTopic)
        }
        words.toIterable
      }
      result.toIterator
    }.persist()


    val wt = wftf.mapPartitions{iter =>
      val wtLocal = Array.ofDim[Int](1,wordsNumber*topicNumber)
      for(doc <- iter) {
        doc.map{ case(word,topic) =>
          wtLocal(0)(word*topicNumber + topic) += 1
        }
      }
      wtLocal.toIterator
    }.reduce{case(first,second) =>
      for(i<-(0 until first.length)){
        first(i) += second(i)
      }
      first
    }

    println("Docs initialized.")
    (wftf, wt)
  }

  // Begin iterations
  def train(wftf:RDD[Iterable[(Int,Int)]],wt:Array[Int]) = {
    println("Begin iteration for "+iteratorTime+" times")
    for(i <- (0 until iteratorTime)) {
      val wtParam = sc.broadcast(wt)

      val d = wftf.mapPartitions{iter =>
          // very big wzMatrix
        val wzMatrix = wtParam.value
        var nzd:Array[Int] = Array.fill[Int](topicNumber)(0)
        var nz:Array[Int] = Array.fill[Int](topicNumber)(0)
        val delta:Array[Int] = Array.fill[Int](wordsAll*topicNumber)(0)
        var probs = Array.fill[Double](topicNumber)(0.0)
        val random = new Random()

        val c = for(doc <- iter) yield {
          nzd = Array.fill[Int](topicNumber)(0)
          nz = Array.fill[Int](topicNumber)(0)
          doc.map{case(word,topic) =>
              nzd(topic) += 1
          }

          var likelihood = 0.0
          var docSize = doc.size

          doc.map{case(word,topic) =>
            nzd(topic) -= 1
            nz(topic) -= 1
            wzMatrix(word*topicNumber + topic ) -= 1
            delta(word*topicNumber + topic) -= 1
            probs = Array.fill[Double](topicNumber)(0.0)
            likelihood += computeSamplingProbablity(wzMatrix,nzd,nz,word,probs,docSize)
            val nextTopic = sampleDistribution(probs, random)
            nzd(nextTopic) += 1
            nz(nextTopic) += 1
            wzMatrix(word*topicNumber + nextTopic) += 1
            delta(word*topicNumber + nextTopic) += 1
            (word,nextTopic)
          }.toIterable

        }

//          val changeIter = iter.toIterable.map{docIter=>
//            nzd = Array.fill[Int](topicNumber)(0)
//            nz = Array.fill[Int](topicNumber)(0)
//            docIter.map{ pair =>
//              nzd(pair._2) += 1
//            }
////
//            var likelihood = 0.0
//
//            val changePair = docIter.map{ pair =>
//              nzd(pair._2) -= 1
//              nz(pair._2) -= 1
//              wzMatrix(pair._1*topicNumber + pair._2) -= 1
//              println(pair._1 +" "+pair._2)
//              println(wzMatrix(pair._1*topicNumber+pair._2))
//              delta(pair._1*topicNumber + pair._2) -= 1
//              probs = Array.fill[Double](topicNumber)(0.0)
//              likelihood += computeSamplingProbablity(wzMatrix,nzd,nz,pair._1,probs,docIter.size)
//              val nextTopic = sampleDistribution(probs, random)
//              nzd(nextTopic) += 1
//              nz(nextTopic) += 1
//              wzMatrix(pair._1*topicNumber + nextTopic) += 1
//              delta(pair._1*topicNumber + nextTopic) += 1
//              (pair._1,nextTopic)
//            }
//
//            changePair.toIterable
//          }
//
//          changeIter.toIterator
//          iter
        c.toIterator
        }
    }
  }

  private def sampleDistribution(probs:Array[Double], random:Random) :Int = {
    val sample = random.nextDouble()
    var sum = 0.0
    var returnTopic = 0
    for(i <- (0 until probs.length)) {
      sum += probs(i)
      if(sample < sum)
        returnTopic  = i
      else
        returnTopic = probs.length - 1
    }
    returnTopic
  }

  private def computeSamplingProbablity(nwz:Array[Int],nzd:Array[Int], nz:Array[Int],
                                word:Int,probs:Array[Double],length:Int) : Double = {
    var norm = 0.0
    var dummyNorm = 1.0
    var likelihood = 0.0
    for(i <-(0 until topicNumber)) {
      val pwz = (nwz(word*topicNumber+i) + beta)  / (nz(i)+nwz.length*beta)
      val pzd = (nzd(i) + alpha) / (length + topicNumber*alpha)
      probs(i) = pwz * pzd
      norm += probs(i)
      likelihood += pwz
    }

    for(i <- (0 until topicNumber))
      probs(i) /= norm
    likelihood
  }


}
