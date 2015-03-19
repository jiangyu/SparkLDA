package com.dj

import com.dj.LdaTrain
import org.clapper.argot._
import ArgotConverters._

/**
 * Created by jiangyu on 3/17/15.
 */

object Entry {
  val parser = new ArgotParser("LDA", preUsage=Some("Version 0.0.1"))
  val inputOption = parser.option[String](List("input"),
    "InputPath","The input path of the train data")
  val outputOption = parser.option[String](List("output"),
    "OutputPath","The output path for final result")
  val topicOption = parser.option[String](List("topicNumber","t"),
    "TopicNumber","Number of topic for gibbs sampling, default 200")
  val iteratorOption = parser.option[Int](List("interator","i"),
    "Iterator time","The iterator time for whole training, default 50")
  val alphaOption = parser.option[Double](List("alpha","a"),
    "alpha","Alpha for training, default 50/numTopic")
  val betaOption = parser.option[Double](List("beta","b"),
    "beta","Beta for training, default 0.01")
//  val maxNumberOption = parser.option[Int](List("maxWords"),
//    "max words to process","Max number of words to process,default is 1000000")
  val minDfOption = parser.option[Int](List("minTime"),
    "min times for words to show","The minimun time for one word to show in all docs,default 5")

  def main(args: Array[String]): Unit = {
    try{
      parser.parse(args)

       val inputPath:String = inputOption.value match {
        case Some(inputPath) => inputPath
        case None => throw new ArgotUsageException("Need inputPath")
      }

      val outputPath:String = outputOption.value match {
        case Some(outputPath) => outputPath
        case None => throw new ArgotUsageException("Need outputPath")
      }

      val topicNumber:Int = topicOption.value match {
        case Some(topicNumber) => topicNumber.toInt
        case None => 200
      }

      val iteratorTime:Int = iteratorOption.value match {
        case Some(iteratorTime) => iteratorTime.toInt
        case None => 50
      }

      val alpha:Double = alphaOption.value match {
        case Some(alpha) => alpha.toDouble
        case None =>  50.0/topicNumber
      }

      val beta:Double = betaOption.value match {
        case Some(beta) => beta.toDouble
        case None => 0.01
      }

//      val maxWords:Int = maxNumberOption.value match {
//        case Some(maxWords) => maxWords.toInt
//        case None => 10000000
//      }

      val minDf:Int = minDfOption.value match {
        case Some(minDf) => minDf.toInt
        case None => 5
      }

      val ldaTrain = new LdaTrain(inputPath,
        outputPath,topicNumber,iteratorTime,alpha,beta,minDf)
      ldaTrain.init
    } catch {
      case e: ArgotUsageException => println(e.message)
    }
  }
}
