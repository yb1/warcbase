package org.warcbase.spark.scripts

import java.io.File
import java.nio.charset.Charset
import java.nio.file.{Files, Paths}
import java.util.concurrent.{TimeUnit, Executors}
import java.net.URLEncoder;

import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD
import org.openqa.selenium.{Dimension, OutputType}
import org.openqa.selenium.phantomjs.{PhantomJSDriver, PhantomJSDriverService}
import org.openqa.selenium.remote.DesiredCapabilities
import org.warcbase.spark.archive.io.ArchiveRecord
import org.warcbase.spark.rdd.RecordRDD._
import java.net.URLEncoder;

/**
  * Created by youngbinkim on 6/28/16.
  */
object TakeScreenshot {
  implicit def runnable(f: () => Unit): Runnable = new Runnable() { def run() = f() }

  def apply(records: RDD[ArchiveRecord], numThreads: Int, output: String
            , phantomjs: String = "phantomjs-2.1.1-linux-x86_64/bin/phantomjs"): Unit = {
    val caps = new DesiredCapabilities()
    caps.setJavascriptEnabled(true)
    caps.setCapability("takesScreenshot", true)
    caps.setCapability(
      PhantomJSDriverService.PHANTOMJS_EXECUTABLE_PATH_PROPERTY,
      phantomjs
    )

    val driver = new  PhantomJSDriver(caps)
    driver.manage().window().setSize(new Dimension(640, 480))
    val time = System.currentTimeMillis / 1000

    val executor = Executors.newFixedThreadPool(numThreads);
    val iter = records.keepValidPages().map(r=>r.getUrl).toLocalIterator

    for (url <- iter) {
      executor.execute(() => {
        try {
          driver.get("https://web.archive.org/web/" + url)
          val screenshot = driver.getScreenshotAs(OutputType.FILE)
          val fileName = if (url.length > 30) url.substring(0, 30) else url
          FileUtils.copyFile(screenshot, new File("/scratch0/%s/%s%d.png".format(output,
            URLEncoder.encode(fileName, "UTF-8"), System.currentTimeMillis())))
          //println("DONE:" + url)
        }
        catch {
          case e: Exception =>
            Console.err.println("Failed: " + url)
            e.printStackTrace()
        }
      })
    }

    executor.shutdown
    executor.awaitTermination(30, TimeUnit.DAYS)
    val time2 = System.currentTimeMillis / 1000

    println(time2-time)
    driver.quit()
  }
}
