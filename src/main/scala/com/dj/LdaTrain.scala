package com.dj

import org.apache.spark.{AccumulatorParam, SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg.{Matrix, Matrices}
import org.jblas.DoubleMatrix


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
    val vecAccum = sc.accumulator(new DoubleMatrix(wordsAll,topicNumber,
      Array.fill(wordsAll*topicNumber)(0.0):_*))
    val wordsParameters = sc.broadcast(wordsNumber)

    val init = lines.mapPartitions{iter =>
      val random = new Random()
      val topicNumber = initParameters.value(2).toInt
      val wordsNumber = wordsParameters.value.toInt
      val wtLocal = Array.fill(wordsNumber*topicNumber)(0.0)
      val result = iter.map{ case (line) =>
        val words = line.split("""\ +""").map{word =>
          val randomTopic = random.nextInt(topicNumber)
          val wordNumber = Integer.parseInt(word)
          wtLocal(wordNumber*topicNumber+randomTopic)  +=  1
          (wordNumber,randomTopic)
        }
        words.toIterable
      }
      val wtMatrix:DoubleMatrix = new DoubleMatrix(wordsNumber,topicNumber,wtLocal:_*)
      vecAccum+=wtMatrix
      result
    }

    vecAccum.value
  }



  implicit object MatrixAccumulatorParam extends AccumulatorParam[DoubleMatrix] {
    def zero(init:DoubleMatrix) : DoubleMatrix = {
      init
    }

    def addInPlace(m1:DoubleMatrix, m2:DoubleMatrix) :DoubleMatrix = {
      m1.add(m2)
    }
  }


  def train = {
  }
}
