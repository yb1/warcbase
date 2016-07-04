package org.warcbase.spark.matchbox

import java.security.MessageDigest

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.jsoup.Jsoup
import org.warcbase.spark.archive.io.{ArchiveRecord, WarcRecord}


/**
  * Created by youngbinkim on 6/30/16.
  */
object ExtractPopularImages {
  def apply(records: RDD[ArchiveRecord], limit: Int) = {
    val res = records
      .map(r => (r.getUrl, r.getContentString))
      .flatMap(x => ExtractImageLinks(x._1, x._2))
      .map(link => (link, 1))
      .reduceByKey(_+_)
      .partitionBy(new HashPartitioner(200))
      .flatMap(tuple => {
        try {
          val imageUrl = tuple._1
          val imageContent = new String(Jsoup.connect("https://web.archive.org/web/" + imageUrl).timeout(5000).ignoreContentType(true).execute().bodyAsBytes())
          println("image Content " + imageUrl);
          val checksum = new String(MessageDigest.getInstance("MD5").digest(imageContent.getBytes))
          println("message Digest");
          Iterator((checksum, (imageUrl, tuple._2)))
        }
        catch {
          case e: Exception =>
            e.printStackTrace()
            Iterator()
        }
      })
      .reduceByKey((image1, image2) => (image1._1, image1._2 + image2._2))
      .takeOrdered(limit)(Ordering[Int].on(x => -x._2._2))
    res.foreach(x => println(x._2._1 + "\t\t" + x._2._2))
    res
  }
}
