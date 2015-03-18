package com.dj

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._

/**
 * Created by jiangyu on 3/18/15.
 */
class LdaTrain(val inputPath:String, val outputPath:String, val topicNumber:Int,
                val iteratorTime:Int, val alpha:Double, val beta:Double, val maxWords:Int,
val minDf:Int) {
  private val conf = new SparkConf().setAppName("LDA")
  private val sc = new SparkContext(conf)

  def init = {
    val initParameters = sc.broadcast(Array(alpha,beta,topicNumber))
    val lines = sc.textFile(inputPath,20)
    val initWordList = lines.flatMap(wordFetch(_))
    val trainingWord = initWordList.reduceByKey((x,y) => (x._1+y._1,x._2+y._2))
    val allDocWordNumber = trainingWord.filter(words => words._1==" ")
    val broadcastDocWordNumber = sc.broadcast(allDocWordNumber.take(1))
    val wordList = trainingWord.filter(word => word._1!=" " && word._2._1 > 0)
      .mapPartitions{wordsAll =>
      val sum = broadcastDocWordNumber.value(0)._2
      val result = wordsAll.map{ case(word,(tf,df)) =>
          val tfdf = 1.0* tf / sum._1 * Math.log(sum._2*1.0/df)
        (tfdf,word)
      }
      result.toIterator
    }.sortByKey(false)
  }

  def wordFetch(line:String):Iterable[(String,(Int,Int))] = {
        var wordsSum:Int = 0
        val wordFrenq = line.split("""\ +""").seq.groupBy(word=>word).map{case (word,time) =>
            wordsSum+=time.length;(word,(time.length,1))}
        val wordFrenqAll = wordFrenq + (" " -> (wordsSum,1))
        wordFrenqAll.toIterable
  }


  def train = {
  }
}
