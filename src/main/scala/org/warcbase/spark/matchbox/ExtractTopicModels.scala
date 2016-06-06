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
  def apply(records: RDD[ArchiveRecord], output:String, sc: SparkContext,
            stopWords: String, numTopics: Int = 20, numIteration: Int = 10000, numTopWords: Int = 10) = {
    val rec = records.keepValidPages().persist()
    val ob1 = records.map(x=>Jsoup.parse(x.getContentString).body().text()).collect()// aa.toArray(ob1)
    val pipeList: java.util.ArrayList[Pipe] = new java.util.ArrayList[Pipe]

    pipeList.add( new CharSequenceLowercase() )
    pipeList.add( new CharSequence2TokenSequence(java.util.regex.Pattern.compile("\\p{L}[\\p{L}\\p{P}]+\\p{L}")) )
    pipeList.add( new TokenSequenceRemoveStopwords(new java.io.File(stopWords), "UTF-8", false, false, false) );
    pipeList.add(new TokenSequence2FeatureSequence)

    val training: InstanceList = new InstanceList(new SerialPipes(pipeList))

    training.addThruPipe(new StringArrayIterator(ob1))

    val model: ParallelTopicModel = new ParallelTopicModel(numTopics, 1.0, 0.01)

    model.addInstances(training)

    model.setNumThreads(1)

    model.setNumIterations(numIteration)
    model.estimate

    val arr = model.getTopWords(numTopWords);
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
