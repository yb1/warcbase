package org.warcbase.spark.matchbox

import java.util
import java.util.{Comparator, Locale, Formatter}

import org.warcbase.spark.rdd.RecordRDD._
import cc.mallet.pipe.iterator.StringArrayIterator
import cc.mallet.pipe._
import cc.mallet.topics.{TopicAssignment, ParallelTopicModel}
import cc.mallet.types._
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.jsoup.Jsoup
import scala.collection.mutable.PriorityQueue
import org.tartarus.snowball.ext.EnglishStemmer
import org.warcbase.spark.archive.io.ArchiveRecord

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * Created by youngbinkim on 6/2/16.
  */
object ExtractTopicModels {
  private val urlPrefix = "https://web.archive.org/web/"

  def apply(records: RDD[ArchiveRecord], output:String, sc: SparkContext,
            stopWords: String, numTopics: Int = 20, numIteration: Int = 80, numTopWords: Int = 10) = {
    val rec:RDD[(String, String)] = records.keepValidPages().map(x=> (x.getUrl, RemoveHttpHeader(x.getContentString))).persist()
    val urls:RDD[(Long, String)] = rec.map(x=>x._1).zipWithIndex().map(_.swap).persist()
    val body:RDD[String] = rec.map(x=> ExtractBoilerpipeText(Jsoup.parse(x._2).body().text()).toString).persist()
    val ob1 = body.filter(str=>DetectLanguage(str)=="en").collect()// aa.toArray(ob1)
    val pipeList: java.util.ArrayList[Pipe] = new java.util.ArrayList[Pipe]
    //LanguageIdentifier
    pipeList.add( new CharSequenceLowercase() )
    pipeList.add( new CharSequence2TokenSequence(java.util.regex.Pattern.compile("\\p{L}[\\p{L}\\p{P}]+\\p{L}")) )
    pipeList.add( new TokenSequenceRemoveStopwords(new java.io.File(stopWords), "UTF-8", false, false, false) );
    pipeList.add(new TokenSequence2FeatureSequence)
    val start = System.currentTimeMillis();
    val training: InstanceList = new InstanceList(new SerialPipes(pipeList))

    training.addThruPipe(new StringArrayIterator(ob1))

    val model: ParallelTopicModel = new ParallelTopicModel(numTopics, 1.0, 0.01)

    //val topicDocuments = new ListBuffer[PriorityQueue[(Int, Double)]]()
    /*for (i <- Range(0, numTopics)) {
      topicDocuments += PriorityQueue[(Int, Double)]()(Ordering[Double].on(x => -x._2))
    }*/

    val topicDocuments2 = new ListBuffer[PriorityQueue[(Int, Double)]]()
    for (i <- Range(0, numTopics)) {
      topicDocuments2 += PriorityQueue[(Int, Double)]()(Ordering[Double].on(x => x._2))
    }

    model.addInstances(training)

    model.setNumThreads(1)

    model.setNumIterations(numIteration)

    model.estimate

    val topicDocs = model.getTopicDocuments(10.0) // not scalable.. need to build new function
/*
    for (i <- Range(0, ob1.length)) {
      val dist = model.getTopicProbabilities(i)
      for (j <- Range(0, dist.length)) {
        topicDocuments(j) += ((i, dist(j)))
      }
    }*/
    for (i <- Range(0, ob1.length)) {
      val dist = model.getTopicProbabilities(i)
      var total = 0.0d;
      for (j <- Range(0, dist.length)) {
        total += dist(j)
      }
      for (j <- Range(0, dist.length)) {
        topicDocuments2(j) += ((i, (dist(j) + 10.0d) / (total + 10.0d * dist.length)))
      }
    }

    val arr = model.getTopWords(numTopWords);
    val res:ListBuffer[String] = new ListBuffer[String]
    val res2:ListBuffer[String] = new ListBuffer[String]
    val res3:ListBuffer[String] = new ListBuffer[String]
    for (i <- Range(0, arr.length)) {
      val words:ListBuffer[String] = new ListBuffer[String]
      val docs = topicDocs.get(i)
      for (j <- Range(0, arr(i).length)) {
        words += arr(i)(j).toString
      }
      val rowHeader = "* topic " + i + ":\t" + words.mkString(",");
      var row = rowHeader;
      //var row2 = rowHeader;
      var row3 = rowHeader;
      //val topicDocumentList:PriorityQueue[(Int, Double)] = topicDocuments(i)
      /*for (j <- Range(0, 5)) {
        val doc = topicDocumentList.dequeue()
        val id = doc._1
        val url = urls.lookup(id).mkString(",")
        println(doc._2 + " : " + url)
        row2 += "\n\t" + j + ". [" + url + "](" + urlPrefix + url + ")"  // + " - " + doc.getWeight
      }*/

      val topicDocumentList2:PriorityQueue[(Int, Double)] = topicDocuments2(i)
      for (j <- Range(0, 5)) {
        if (!topicDocumentList2.isEmpty) {
          val doc = topicDocumentList2.dequeue()
          val id = doc._1
          val url = urls.lookup(id).mkString(",")
          println("\tha " + doc._2 + " : " + url)
          row3 += "\n\t" + j + ". [" + url + "](" + urlPrefix + url + ")"  // + " - " + doc.getWeight
        }
      }

      for (j <- Range(0, 5)) {
        val doc2 = docs.pollLast();
        // print(j + " large " + doc2.getWeight);
        val doc = docs.pollFirst();
        val data = model.getData.get(1).instance
        val name = data.getName;
        print("source: " + data.getSource)
        // print("data: " + data.getData)
        print("name " + name.toString);
        print("target " + data.getTarget)
        val source = data.getSource
        val lookup = urls.lookup(doc.getID)
        val url = if (lookup != null) lookup.mkString(",") else doc.getID
        print("url " + url)
        row += "\n\t" + j + ". [" + url + "](" + urlPrefix + url + ")"  // + " - " + doc.getWeight
      }
      res += row
      //res2 += row2
      res3 += row3
    }
    sc.parallelize(res, 1).saveAsTextFile(output)
    //sc.parallelize(res2, 1).saveAsTextFile(output + "tmp")
    sc.parallelize(res3, 1).saveAsTextFile(output + "tmp2")
    val end = System.currentTimeMillis();
    System.out.println("Topic Modelling finished in " + ((end-start)/1000) + " seconds.");
  }

  /*
  def getTopicsDocuments(model: ParallelTopicModel): Unit = {
    val topicSortedDocuments:util.ArrayList[util.TreeSet[Nothing]]  = new util.ArrayList(model.numTopics);

    for(i <- Range(0, model.numTopics)) {
      topicSortedDocuments.add(new util.TreeSet());
    }

    val var8 = new Int[model.numTopics];

    for(int doc = 0; doc < this.data.size(); ++doc) {
      val dataSet = model.getData;
      val topics = (dataSet.get(doc)).topicSequence.getFeatures();

      int topic;
      for(topic = 0; topic < topics.length; ++topic) {
        ++var8[topics[topic]];
      }

      for(topic = 0; topic < this.numTopics; ++topic) {
        ((TreeSet)topicSortedDocuments.get(topic)).add(new IDSorter(doc, ((double)var8[topic] + smoothing) / ((double)topics.length + (double)this.numTopics * smoothing)));
        var8[topic] = 0;
      }
    }

    return topicSortedDocuments;
  }
  */
  def getLemmas(text: String): String = {
    var lemmas = "";
    val words = text.split("\\s")
    for (lemma <- words){
      if (lemma.length > 2 && DetectLanguage(lemma) == "en") {
        lemmas += lemma
      }
    }
    lemmas
  }
}
