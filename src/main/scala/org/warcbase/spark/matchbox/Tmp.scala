/*
package org.warcbase.spark.matchbox

import java.util.Locale

import cc.mallet.pipe.iterator.StringArrayIterator
import cc.mallet.pipe.{SerialPipes, TokenSequence2FeatureSequence, CharSequence2TokenSequence, Pipe}
import cc.mallet.topics.ParallelTopicModel
import cc.mallet.types._
import org.apache.spark.rdd.RDD
import org.warcbase.spark.archive.io.ArchiveRecord

/**
  * Created by youngbinkim on 6/2/16.
  */
object Tmp {
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

    // The data alphabet maps word IDs to strings
    val dataAlphabet: Alphabet = training.getDataAlphabet

    val tokens: FeatureSequence = model.getData.get(0).instance.getData.asInstanceOf[FeatureSequence]
    val topics: LabelSequence = model.getData.get(0).topicSequence

    var out: java.util.Formatter = new java.util.Formatter(new java.lang.StringBuilder(), Locale.US);
    {
      var position: Int = 0
      while (position < tokens.getLength) {
        {
          out.format("%s-%d ", dataAlphabet.lookupObject(tokens.getIndexAtPosition(position)), topics.getIndexAtPosition(position))
        }
        ({
          position += 1; position - 1
        })
      }
    }
    System.out.println(out)


    // Estimate the topic distribution of the first instance,
    //  given the current Gibbs state.
    val topicDistribution: Array[Double] = model.getTopicProbabilities(0)

    // Get an array of sorted sets of word ID/count pairs
    val topicSortedWords: java.util.ArrayList[java.util.TreeSet[IDSorter]] = model.getSortedWords

    // Show top 5 words in topics with proportions for the first document
    {
      var topic: Int = 0
      while (topic < numTopics) {
        {
          val iterator: java.util.Iterator[IDSorter] = topicSortedWords.get(topic).iterator
          out = new java.util.Formatter(new java.lang.StringBuilder(), Locale.US)
          out.format("%d\t%.3f\t", topic, topicDistribution(topic))
          var rank: Int = 0
          while (iterator.hasNext && rank < 5) {
            val idCountPair: IDSorter = iterator.next
            out.format("%s (%.0f) ", dataAlphabet.lookupObject(idCountPair.getID), idCountPair.getWeight)
            rank += 1
          }
          System.out.println(out)
        }
        ({
          topic += 1; topic - 1
        })
      }
    }
  }
}
*/
