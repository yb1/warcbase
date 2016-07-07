package org.warcbase.spark.matchbox

import java.io.PrintWriter

import cc.mallet.pipe.iterator.StringArrayIterator
import cc.mallet.pipe.{Pipe, SerialPipes, TokenSequence2FeatureSequence, CharSequence2TokenSequence}
import cc.mallet.topics.ParallelTopicModel
import cc.mallet.types.InstanceList
import org.apache.spark.{AccumulatorParam, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.jsoup.Jsoup
import org.tartarus.snowball.ext.EnglishStemmer
import org.warcbase.spark.archive.io.ArchiveRecord
import org.warcbase.spark.rdd.RecordRDD._
import org.warcbase.spark.scripts.KMeansArchiveCluster
import scala.collection.mutable.ListBuffer
import org.apache.log4j.{Level, Logger}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, RegexTokenizer, StopWordsRemover}
import org.apache.spark.mllib.clustering.{DistributedLDAModel, EMLDAOptimizer, LDA, OnlineLDAOptimizer}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row}


object ExtractClusters {
  implicit object MapAccumulator extends AccumulatorParam[Map[Int, Double]] {
    def zero(m: Map[Int, Double]) = Map()
    def addInPlace(m1: Map[Int, Double], m2: Map[Int, Double]) = m1 ++ m2
  }

  def runTestK(tfidf: RDD[Vector], maxIterations: Int, maxK: Int, sc: SparkContext, stepK: Int, minK: Int) = {
    val ran = Range(minK, maxK, stepK)
    val accum = sc.accumulator(Map[Int, Double]())(MapAccumulator)
    ran.par.foreach(i => {
      val model = KMeans.train(tfidf, i, maxIterations)
      val cost =model.computeCost(tfidf)
      accum += Map((i, cost))
    })

    // val x = minK.toDouble until maxK.toDouble by stepK.toDouble
    // output(ASCII, xyChart(x -> accum.value.toSeq.sortBy(_._1).map(_._2)))
  }

  def apply(records: RDD[ArchiveRecord], sc: SparkContext, k: Int = 20, maxIterations: Int = 30, numVocabs: Int = 10000,
            testK: Boolean=false, maxK: Int= 50, minDocThreshold: Int=5, stepK: Int=10, minK: Int=5) = {
    val stopwords = sc.broadcast(Set("a", "as", "able", "about", "above", "according", "accordingly", "across", "actually", "after", "afterwards", "again", "against", "aint", "all", "allow", "allows", "almost", "alone", "along", "already", "also", "although", "always", "am", "among", "amongst", "an", "and", "another", "any", "anybody", "anyhow", "anyone", "anything", "anyway", "anyways", "anywhere", "apart", "appear", "appreciate", "appropriate", "are", "arent", "around", "as", "aside", "ask", "asking", "associated", "at", "available", "away", "awfully", "be", "became", "because", "become", "becomes", "becoming", "been", "before", "beforehand", "behind", "being", "believe", "below", "beside", "besides", "best", "better", "between", "beyond", "both", "brief", "but", "by", "cmon", "cs", "came", "can", "cant", "cannot", "cant", "cause", "causes", "certain", "certainly", "changes", "clearly", "co", "com", "come", "comes", "concerning", "consequently", "consider", "considering", "contain", "containing", "contains", "corresponding", "could", "couldnt", "course", "currently", "definitely", "described", "despite", "did", "didnt", "different", "do", "does", "doesnt", "doing", "dont", "done", "down", "downwards", "during", "each", "edu", "eg", "eight", "either", "else", "elsewhere", "enough", "entirely", "especially", "et", "etc", "even", "ever", "every", "everybody", "everyone", "everything", "everywhere", "ex", "exactly", "example", "except", "far", "few", "ff", "fifth", "first", "five", "followed", "following", "follows", "for", "former", "formerly", "forth", "four", "from", "further", "furthermore", "get", "gets", "getting", "given", "gives", "go", "goes", "going", "gone", "got", "gotten", "greetings", "had", "hadnt", "happens", "hardly", "has", "hasnt", "have", "havent", "having", "he", "hes", "hello", "help", "hence", "her", "here", "heres", "hereafter", "hereby", "herein", "hereupon", "hers", "herself", "hi", "him", "himself", "his", "hither", "hopefully", "how", "howbeit", "however", "i", "id", "ill", "im", "ive", "ie", "if", "ignored", "immediate", "in", "inasmuch", "inc", "indeed", "indicate", "indicated", "indicates", "inner", "insofar", "instead", "into", "inward", "is", "isnt", "it", "itd", "itll", "its", "its", "itself", "just", "keep", "keeps", "kept", "know", "knows", "known", "last", "lately", "later", "latter", "latterly", "least", "less", "lest", "let", "lets", "like", "liked", "likely", "little", "look", "looking", "looks", "ltd", "mainly", "many", "may", "maybe", "me", "mean", "meanwhile", "merely", "might", "more", "moreover", "most", "mostly", "much", "must", "my", "myself", "name", "namely", "nd", "near", "nearly", "necessary", "need", "needs", "neither", "never", "nevertheless", "new", "next", "nine", "no", "nobody", "non", "none", "noone", "nor", "normally", "not", "nothing", "novel", "now", "nowhere", "obviously", "of", "off", "often", "oh", "ok", "okay", "old", "on", "once", "one", "ones", "only", "onto", "or", "other", "others", "otherwise", "ought", "our", "ours", "ourselves", "out", "outside", "over", "overall", "own", "particular", "particularly", "per", "perhaps", "placed", "please", "plus", "possible", "presumably", "probably", "provides", "que", "quite", "qv", "rather", "rd", "re", "really", "reasonably", "regarding", "regardless", "regards", "relatively", "respectively", "right", "said", "same", "saw", "say", "saying", "says", "second", "secondly", "see", "seeing", "seem", "seemed", "seeming", "seems", "seen", "self", "selves", "sensible", "sent", "serious", "seriously", "seven", "several", "shall", "she", "should", "shouldnt", "since", "six", "so", "some", "somebody", "somehow", "someone", "something", "sometime", "sometimes", "somewhat", "somewhere", "soon", "sorry", "specified", "specify", "specifying", "still", "sub", "such", "sup", "sure", "ts", "take", "taken", "tell", "tends", "th", "than", "thank", "thanks", "thanx", "that", "thats", "thats", "the", "their", "theirs", "them", "themselves", "then", "thence", "there", "theres", "thereafter", "thereby", "therefore", "therein", "theres", "thereupon", "these", "they", "theyd", "theyll", "theyre", "theyve", "think", "third", "this", "thorough", "thoroughly", "those", "though", "three", "through", "throughout", "thru", "thus", "to", "together", "too", "took", "toward", "towards", "tried", "tries", "truly", "try", "trying", "twice", "two", "un", "under", "unfortunately", "unless", "unlikely", "until", "unto", "up", "upon", "us", "use", "used", "useful", "uses", "using", "usually", "value", "various", "very", "via", "viz", "vs", "want", "wants", "was", "wasnt", "way", "we", "wed", "well", "were", "weve", "welcome", "well", "went", "were", "werent", "what", "whats", "whatever", "when", "whence", "whenever", "where", "wheres", "whereafter", "whereas", "whereby", "wherein", "whereupon", "wherever", "whether", "which", "while", "whither", "who", "whos", "whoever", "whole", "whom", "whose", "why", "will", "willing", "wish", "with", "within", "without", "wont", "wonder", "would", "would", "wouldnt", "yes", "yet", "you", "youd", "youll", "youre", "youve", "your", "yours", "yourself", "yourselves", "zero"))
    val rec = records.keepValidPages().persist()
    val ob1 = rec.map(q=>Jsoup.parse(q.getContentString).body().text()).collect()

    val pipeList: java.util.ArrayList[Pipe] = new java.util.ArrayList[Pipe]

    pipeList.add(new CharSequence2TokenSequence)
    //....
    pipeList.add(new TokenSequence2FeatureSequence)

    val training: InstanceList = new InstanceList(new SerialPipes(pipeList))

    training.addThruPipe(new StringArrayIterator(ob1))

    val numTopics = 50;
    val model:ParallelTopicModel = new ParallelTopicModel(numTopics, 1.0, 0.01);

    model.addInstances(training);
    model.setNumThreads(Runtime.getRuntime.availableProcessors);
    model.setNumIterations(10);
    model.estimate();
    printTopWords("top10", model, 10)

  }

  /**
    * Prints the top {@code wordsPerTopic} words for each topic in the model
    * and stores the topic words, with one topic per line, in {@code outFile}.
    */
  def printTopWords(outFile:String, topicModel:ParallelTopicModel,
                    wordsPerTopic:Int) {
    System.err.println("Printing top words")
    val w = new PrintWriter(outFile)
    topicModel.getTopWords(wordsPerTopic)
      .map(_.mkString(" "))
      .foreach(w.println)
    w.close
  }

  def getLemmas(q: ArchiveRecord, stemmer: EnglishStemmer, stopwords: Broadcast[Set[String]]): Seq[String] = {
    val text = Jsoup.parse(q.getContentString).select("body").first().text()
    val lemmas = new ListBuffer[String]()
    val words = text.split("\\s")
    for (word <- words){
      stemmer.setCurrent(word.toLowerCase.replaceAll("[^\\p{L}]+", ""))
      stemmer.stem()
      val lemma = stemmer.getCurrent()
      if (lemma.length > 2 && !stopwords.value.contains(lemma)) {
        lemmas += lemma
      }
    }
    lemmas.toList
  }

  def convertToDF(lemmatized: RDD[(String, Seq[String])], sc: SparkContext) = {
    val sqlContext = new SQLContext(sc)
    sqlContext.createDataFrame(lemmatized).toDF("url", "words")
  }

  def getTfIdf(lemmatized: RDD[Seq[String]], minDocThreshold: Int, numFeatures: Long) = {
    val hashingTF = new HashingTF()
    val tf = hashingTF.transform(lemmatized)

    tf.cache()
    val idf = new IDF(minDocFreq = minDocThreshold).fit(tf)
    idf.transform(tf)
  }

  private def preprocess(
                          sc: SparkContext,
                          sqlContext:SQLContext,
                          lemmatized: RDD[(String, Seq[String])],
                          vocabSize: Int): (RDD[(Long, Vector)], Array[String], Long) = {

    // Get dataset of document texts
    // One document per line in each text file. If the input consists of many small files,
    // this can result in a large number of small partitions, which can degrade performance.
    // In this case, consider using coalesce() to create fewer, larger partitions.
    val df = sqlContext.createDataFrame(lemmatized).toDF("url", "tokens")

    val countVectorizer = new CountVectorizer()
      .setVocabSize(vocabSize)
      .setInputCol("tokens")
      .setOutputCol("features")

    val pipeline = new Pipeline()
      .setStages(Array(countVectorizer))

    val model = pipeline.fit(df)
    val documents = model.transform(df)
      .select("features")
      .rdd
      .map { case Row(features: Vector) => features }
      .zipWithIndex()
      .map(_.swap)

    (documents,
      model.stages(2).asInstanceOf[CountVectorizerModel].vocabulary,  // vocabulary
      documents.map(_._2.numActives).sum().toLong) // total token count
  }
}