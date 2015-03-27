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


  implicit object ArrayAccumulatorParam extends AccumulatorParam[Array[Int]] {
    def zero(init:Array[Int]) : Array[Int] = {
      Array.fill[Int](init.length)(0)
    }

    def addInPlace(m1:Array[Int], m2:Array[Int]): Array[Int] = {
      for(i <- (0 until m1.length))
        m1(i)+=m2(i)
      m1
    }
  }

  def init = {
    val initParameters = sc.broadcast(Array(alpha,beta,topicNumber))
    val lines = sc.textFile(inputPath,20)
    val wordsNumber = lines.flatMap(_.split("""\ +""")).distinct(20).count().toInt
    val arrayAccum = sc.accumulable(Array.fill[Int](wordsNumber*topicNumber)(0))
    wordsAll = wordsNumber
    val wordsParameters = sc.broadcast(wordsNumber)

    val wftf = lines.mapPartitions{iterator =>
      val iter = iterator.toIterable
      val random = new Random()
      val topicNumber = initParameters.value(2).toInt
      val wordsNumber = wordsParameters.value.toInt
//      val wtLocal = Array.fill[Int](wordsNumber*topicNumber)(0)

      val result = iter.map{ case (line) =>
        val words = line.split("""\ +""").map{word =>
          val randomTopic = random.nextInt(topicNumber)
          val wordNumber = Integer.parseInt(word)
//          wtLocal(wordNumber*topicNumber+randomTopic)  +=  1
          println(wordNumber+" "+randomTopic)
          (wordNumber,randomTopic)
        }
        words.toIterable
      }
//      result.tail
//      arrayAccum+=wtLocal
      result.toIterator
    }.persist()


    val d = wftf.mapPartitions{iter =>
      val wt = Array.ofDim[Int](1,wordsNumber*topicNumber)
      //      val wt = Array.fill[Int](wordsNumber*topicNumber)(0)
      iter.toIterable.map{doc =>
        doc.map{ case(word,topic) =>
          wt(0)(word*topicNumber + topic) += 1
        }

      }
      wt.iterator
    }.reduce{case(first,second) =>
      for(i<-(0 until first.length)){
        first(i) += second(i)
      }
      first
    }


    val wt = Array.fill[Int](wordsNumber*topicNumber)(0)
    wftf.map{iter =>
      iter.map{case(word,topic) =>
        wt(word*topicNumber+topic)  +=  1
      }
    }

    println("Docs initialized.")
    arrayAccum.setValue(Array.fill[Int](wordsNumber*topicNumber)(0))
    for(i <-(0 until arrayAccum.zero.length)) {
      arrayAccum.zero(i) = 0
    }
    (wftf, arrayAccum)
  }

  // Begin iterations
  def train(wftf:RDD[Iterable[(Int,Int)]],wt:Accumulable[Array[Int],Array[Int]]) = {
    println("Begin iteration for "+iteratorTime+" times")
    for(i <- (0 until iteratorTime)) {
      val wtParam = sc.broadcast(wt.value)
        val d = wftf.mapPartitions{iter =>
          // very big wzMatrix
          val wzMatrix = wtParam.value
          for(i <- (0 until wzMatrix.length)) {
            print(wzMatrix(i)+" ")
          }
          var nzd:Array[Int] = Array.fill[Int](topicNumber)(0)
          var nz:Array[Int] = Array.fill[Int](topicNumber)(0)
          var delta:Array[Int] = Array.fill[Int](wordsAll*topicNumber)(0)
          var probs = Array.fill[Double](topicNumber)(0.0)
          val random = new Random()

          val changeIter = iter.map{docIter=>
            nzd = Array.fill[Int](topicNumber)(0)
            nz = Array.fill[Int](topicNumber)(0)
            docIter.map{ pair =>
              nzd(pair._2) += 1
            }

            var likelihood = 0.0

            val changePair = docIter.map{ pair =>
              nzd(pair._2) -= 1
              nz(pair._2) -= 1
              wzMatrix(pair._1*topicNumber + pair._2) -= 1
              delta(pair._1*topicNumber + pair._2) -= 1
              probs = Array.fill[Double](topicNumber)(0.0)
              likelihood += computeSamplingProbablity(wzMatrix,nzd,nz,pair._1,probs,docIter.size)
              val nextTopic = sampleDistribution(probs, random)
              nzd(nextTopic) += 1
              nz(nextTopic) += 1
              wzMatrix(pair._1*topicNumber + nextTopic) += 1
              delta(pair._1*topicNumber + nextTopic) += 1
              (pair._1,nextTopic)
            }

            changePair.toIterable
          }

          changeIter.toIterator
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
