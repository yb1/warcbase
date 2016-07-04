package org.warcbase.spark.scripts

import java.io.{InputStreamReader, BufferedReader, File}
import java.net.URLEncoder
import java.nio.charset.{StandardCharsets, Charset}
import java.nio.file.{Files, Paths}
import java.util.concurrent.{ExecutorService, TimeUnit, Executors}

import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD
import org.openqa.selenium.{Dimension, OutputType}
import org.openqa.selenium.phantomjs.{PhantomJSDriver, PhantomJSDriverService}
import org.openqa.selenium.remote.DesiredCapabilities
import org.warcbase.spark.archive.io.ArchiveRecord
import org.warcbase.spark.rdd.RecordRDD._

import scala.collection.mutable.ArrayBuffer

/**
  * Created by youngbinkim on 6/28/16.
  */
object Screenshot {
  implicit def runnable(f: () => Any): Runnable = new Runnable() { def run() = f() }

  def takeScreenshots(executor: ExecutorService, arr: ArrayBuffer[String]): Unit ={
    executor.execute(() => {
      var line = ""
      val p = Runtime.getRuntime().exec(arr.toArray)
      val bri = new BufferedReader (new InputStreamReader(p.getInputStream()));
      val bre = new BufferedReader (new InputStreamReader(p.getErrorStream()));
      while ({line = bri.readLine(); line != null}) {
        Console.out.println(line)
      }
      bri.close();
      while ({line = bre.readLine(); line != null}) {
        Console.err.println(line)
      }
      bre.close();
      p.waitFor();
      System.out.println("Done.");
    })
  }
  def apply(records: RDD[ArchiveRecord], numThreads: Int, output: String
            , phantomjs: String = "phantomjs-2.1.1-linux-x86_64/bin/phantomjs",
            script: String = "screenshot.js"): Unit = {
    val time = System.currentTimeMillis / 1000

    val executor = Executors.newFixedThreadPool(numThreads);
    val iter = records.keepValidPages().map(r=>r.getUrl)
        .repartition(200)
      .toLocalIterator
    val arr = ArrayBuffer[String]()
    arr += phantomjs
    arr += script
    arr += output
    var len = 0
    for (url <- iter) {
      if (len > 100) {
        // println(arr.mkString(" "))
        takeScreenshots(executor, arr)
        arr.clear()
        arr += phantomjs
        arr += script
        arr += output
        len = 0
      }
      len += url.length + 1
      arr += url
    }

    if (arr.length > 3)
      takeScreenshots(executor, arr)

      /*
      .collect().foreach(url=> {
      executor.execute(() => {
        try {
          val fileName = if (url.length > 30) url.substring(0, 30) else url
          FileUtils.copyFile(screenshot, new File("/scratch0/%s/%s%d.png".format(output,
            URLEncoder.encode(fileName, "UTF-8"), System.currentTimeMillis())))
          //println("DONE:" + url)
        }
        catch {
          case e: Exception =>
            Console.err.println("Failed: " + url)
          //e.printStackTrace()
        }
      })
        })
      */



    executor.shutdown
    executor.awaitTermination(30, TimeUnit.DAYS)
    val time2 = System.currentTimeMillis / 1000

    println(time2-time)
  }
}