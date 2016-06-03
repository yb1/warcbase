package org.warcbase.spark.matchbox

import java.util.{Locale, Formatter}

import cc.mallet.pipe.iterator.StringArrayIterator
import cc.mallet.pipe.{Pipe, SerialPipes, TokenSequence2FeatureSequence, CharSequence2TokenSequence}
import cc.mallet.topics.ParallelTopicModel
import cc.mallet.types._
import org.apache.spark.rdd.RDD
import org.warcbase.spark.archive.io.ArchiveRecord

/**
  * Created by youngbinkim on 6/2/16.
  */
object ExtractTopicModels {
  def apply(records: RDD[ArchiveRecord]) = {
    val aa = new java.util.ArrayList[String]
    aa.add("apple fruit orange banana strawberry computer television")
    aa.add("mango fruit avengers hulk thor")

    records.map(x=>x.getContentString)
    var ob1: Array[String] = new Array[String](aa.size)
    ob1 = aa.toArray(ob1)
    val pipeList: java.util.ArrayList[Pipe] = new java.util.ArrayList[Pipe]

    pipeList.add(new CharSequence2TokenSequence)
    //....
    pipeList.add(new TokenSequence2FeatureSequence)

    val training: InstanceList = new InstanceList(new SerialPipes(pipeList))

    training.addThruPipe(new StringArrayIterator(ob1))

    val numTopics: Int = 100
    val model: ParallelTopicModel = new ParallelTopicModel(numTopics, 1.0, 0.01)

    model.addInstances(training)

    // Use two parallel samplers, which each look at one half the corpus and combine
    //  statistics after every iteration.
    model.setNumThreads(2)

    // Run the model for 50 iterations and stop (this is for testing only,
    //  for real applications, use 1000 to 2000 iterations)
    model.setNumIterations(50)
    model.estimate

    for (arr <- model.getTopWords(10)) {
      arr.foreach(x=>print(x + "\t"))
      println()
    }
  }
}
