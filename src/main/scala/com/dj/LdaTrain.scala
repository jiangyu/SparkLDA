package com.dj

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import scala.util.Random
import scala.util.control.Breaks

/**
 * Created by jiangyu on 3/18/15.
 */
class LdaTrain(val inputPath:String, val outputPath:String, val topicNumber:Int,
                val iteratorTime:Int, val alpha:Double, val beta:Double,
val minDf:Int) extends Serializable{
  private val conf = new SparkConf().setAppName("LDA")
    .registerKryoClasses(Array(classOf[Array[(Int,Int)]],classOf[Array[Int]],classOf[(Int,Array[(Int,Int)])]))
  private val sc = new SparkContext(conf)
  private var wordsAll:Int = 0


  def init = {
    val lines = sc.textFile(inputPath,20)
    val partitionNumber = 20
    val initParameters = sc.broadcast(Array(alpha,beta,topicNumber,partitionNumber))
    val wordsNumber = lines.flatMap(_.split("""\ +""")).distinct(20).count().toInt
    wordsAll = wordsNumber
    val wordsParameters = sc.broadcast(wordsNumber)

    val wftf = lines.map{ line =>
      val random = new Random()
      val partitionNumber = initParameters.value(3).toInt
      val number = random.nextInt(partitionNumber)
      val topicNumber = initParameters.value(2).toInt
      val words = line.split("""\ +""").map { word =>
        val randomTopic = random.nextInt(topicNumber)
        val wordNumber = Integer.parseInt(word)
        (wordNumber,randomTopic)
      }
      (number,words)
    }.partitionBy(new HashPartitioner(partitionNumber)).persist(StorageLevel.MEMORY_ONLY)


    val wt = wftf.combineByKey[Array[Int]](
      createCombiner = (v: Array[(Int,Int)]) => {
        val wordsAll = wordsParameters.value
        val topicNumber = initParameters.value(2).toInt
        val result = Array.fill[Int](wordsAll*topicNumber)(0)
        for(i <- 0 until v.length) {
          val index = v(i)._1 * topicNumber + v(i)._2
          result(index) += 1
        }
        result
      },
      mergeValue = (c: Array[Int], v: Array[(Int,Int)]) => {
        val topicNumber = initParameters.value(2).toInt
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
                  val wordsNumber = wordsParameters.value
                  val topicNumber = initParameters.value(2).toInt
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

  private def createCombiner(v: Array[(Int,Int)]) = {
    val result = Array.fill[Int](wordsAll*topicNumber)(0)
    for(i <- 0 until v.length) {
      val index = v(i)._1 * topicNumber + v(i)._2
      result(index) += 1
    }
    result
  }

  private def mergeValue(c: Array[Int], v: Array[(Int,Int)]) = {
    for(i <- 0 until v.length) {
      val index = v(i)._1 * topicNumber + v(i)._2
      c(index) += 1
    }
    c
  }

  private def mergeCombiners(c1: Array[Int], c2: Array[Int]) = {
    for(i <- 0 until c1.length) {
      c1(i) += c2(i)
    }
    c1
  }


  // Begin iterations
  def train(wftf:RDD[(Int,Array[(Int,Int)])],wt:Array[Int]) = {
    println("Begin iteration for "+iteratorTime+" times")
    var wtTransfer:Array[Int] = null


    for(i <- (0 until iteratorTime)) {
//      val wordsParameters = sc.broadcast(wordsAll)
//      val initParameters = sc.broadcast(Array(alpha,beta,topicNumber,20))
      val wordsParameters  = wftf.context.broadcast(wordsAll)
      val initParameters = wftf.context.broadcast(Array(alpha,beta,topicNumber,20))

      if(i == 0) {
        wtTransfer = wt
      }
      else {
        wtTransfer = wftf.combineByKey[Array[Int]](
          createCombiner = (v: Array[(Int,Int)]) => {
            val wordsSum = wordsParameters.value
            val topicAll = initParameters.value(2).toInt
            val result = Array.fill[Int](wordsSum*topicAll)(0)
            for(i <- 0 until v.length) {
              val index = v(i)._1 * topicAll + v(i)._2
              result(index) += 1
            }
            result
          },
          mergeValue = (c: Array[Int], v: Array[(Int,Int)]) => {
            val topicAll = initParameters.value(2).toInt
            for(i <- 0 until v.length) {
              val index = v(i)._1 * topicAll + v(i)._2
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
          val wordsSum = wordsParameters.value
          val topicAll = initParameters.value(2).toInt
          val wtLocal = Array.ofDim[Int](1,wordsSum*topicAll)
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
//      val wtParam = sc.broadcast(wtTransfer.clone())
      val wtParam = wftf.context.broadcast(wtTransfer.clone())
      if(i != iteratorTime-1)
        wtTransfer = null


      wftf.mapPartitions{iter =>
        val topicAll = initParameters.value(2).toInt
        val alpha = initParameters.value(0).toDouble
        val beta = initParameters.value(1).toDouble
        var wzMatrix = wtParam.value
        var nzd:Array[Int] = Array.fill[Int](topicAll)(0)
        val nz:Array[Int] = Array.fill[Int](topicAll)(0)
        for(i<- 0 until wzMatrix.length) {
          nz(i%topicAll) += wzMatrix(i)
        }

        var probs = Array.fill[Double](topicAll)(0.0)
        val random = new Random()

        for(doc <- iter) {
          nzd = Array.fill[Int](topicAll)(0)
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
            wzMatrix(word*topicAll + topic ) -= 1
            probs = Array.fill[Double](topicAll)(0.0)

            //            likelihood += computeSamplingProbablity(wzMatrix,nzd,nz,word,probs,docSize,topicAll,alpha1,beta1)
            val pzd = (nzd(i) + alpha) / (length + topicAll*alpha)
            var norm = 0.0
            var likelihood = 0.0
            for(i <-(0 until topicAll)) {
              val pwz = (wzMatrix(word*topicAll+i) + beta)  / (nz(i)+wzMatrix.length*beta)
              probs(i) = pwz * pzd
              norm += probs(i)
              likelihood += pwz
            }

            for(i <- 0 until topicAll) {
              probs(i) /= norm
            }

//            val nextTopic = sampleDistribution(probs, random)
            var nextTopic = probs.length - 1
            val sample = random.nextDouble()
            var sum = 0.0
            val loop = new Breaks
            loop.breakable {
              for(i <- (0 until probs.length)) {
                sum += probs(i)
                if(sample < sum) {
                  nextTopic = i
                  loop.break()
                }
              }
            }

//            wzMatrix(word*topicNumber + nextTopic) += 1
            doc._2(i) = (word,nextTopic)
          }
        }

        wzMatrix = null

        iter
      }

    }

    var begin = 0
    var end = 0
    val times = 5
    val wordIndex = wordsAll/times
    var wordBegin = 0
    var wordEnd = 0


    for(i <- 0 until times) {
      val output:Array[(Int,String)] = Array.fill[(Int,String)](wordIndex)(0,"")
      wordBegin = i*wordIndex
      if(i == times - 1) wordEnd = wordsAll
      else wordEnd = (i+1) * wordIndex - 1

      for(j<- wordBegin until wordEnd) {
        begin = j * topicNumber
        end = (j+1)  * topicNumber - 1
        val temp = for(k<- begin until end)
          yield wtTransfer(k)
        output(j%wordIndex) = (j,temp.mkString(" "))
      }

      sc.makeRDD(output).saveAsTextFile(outputPath+"/"+i)
    }

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
                                word:Int,probs:Array[Double],length:Int,topicAll:Int,alpha1:Double,beta1:Double) : Double = {
    var norm = 0.0
    var likelihood = 0.0
    for(i <-(0 until topicAll)) {
      val pwz = (nwz(word*topicAll+i) + beta1)  / (nz(i)+nwz.length*beta1)
      val pzd = (nzd(i) + alpha1) / (length + topicAll*alpha)
      probs(i) = pwz * pzd
      norm += probs(i)
      likelihood += pwz
    }

    for(i <- 0 until topicAll) {
      probs(i) /= norm
    }
    likelihood
  }

}
