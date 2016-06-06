package org.warcbase.spark.matchbox

import java.util.{Locale, Formatter}

import org.warcbase.spark.rdd.RecordRDD._
import cc.mallet.pipe.iterator.StringArrayIterator
import cc.mallet.pipe._
import cc.mallet.topics.ParallelTopicModel
import cc.mallet.types._
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.jsoup.Jsoup
import org.tartarus.snowball.ext.EnglishStemmer
import org.warcbase.spark.archive.io.ArchiveRecord

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * Created by youngbinkim on 6/2/16.
  */
object ExtractTopicModels {
  def apply(records: RDD[ArchiveRecord], output:String, sc: SparkContext, stopWords: String, numTopics: Int = 20) = {
    val aa = new java.util.ArrayList[String]
    aa.add("apple fruit orange banana strawberry computer television")
    aa.add("mango fruit avengers hulk thor")


    var ob1: Array[String] = new Array[String](aa.size)

    val rec = records.keepValidPages().persist()

    ob1 = records.map(x=>Jsoup.parse(x.getContentString).body().text()).collect()// aa.toArray(ob1)
    val pipeList: java.util.ArrayList[Pipe] = new java.util.ArrayList[Pipe]

    pipeList.add( new CharSequenceLowercase() )
    pipeList.add( new CharSequence2TokenSequence(java.util.regex.Pattern.compile("\\p{L}[\\p{L}\\p{P}]+\\p{L}")) )
    pipeList.add( new TokenSequenceRemoveStopwords(new java.io.File(stopWords), "UTF-8", false, false, false) );
    pipeList.add(new TokenSequence2FeatureSequence)

    val training: InstanceList = new InstanceList(new SerialPipes(pipeList))

    training.addThruPipe(new StringArrayIterator(ob1))

    val model: ParallelTopicModel = new ParallelTopicModel(numTopics, 1.0, 0.01)

    model.addInstances(training)

    // Use two parallel samplers, which each look at one half the corpus and combine
    //  statistics after every iteration.
    model.setNumThreads(1)

    // Run the model for 50 iterations and stop (this is for testing only,
    //  for real applications, use 1000 to 2000 iterations)
    model.setNumIterations(50)
    model.estimate

    val arr = model.getTopWords(10);
    val res:ListBuffer[String] = new ListBuffer[String]
    for (i <- Range(0, arr.length)) {
      val words:ListBuffer[String] = new ListBuffer[String]
      for (j <- Range(0, arr(i).length)) {
        words += arr(i)(j).toString
      }
      res += "topic " + i + ":\t" + words.mkString(",")
    }
    sc.parallelize(res, 1).saveAsTextFile(output)
  }
}
