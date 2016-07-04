package org.warcbase.spark.matchbox

import java.security.MessageDigest


/**
  * Created by youngbinkim on 6/30/16.
  */
object ComputeMD5 {
  def apply(input: String): String = {
    new String(MessageDigest.getInstance("MD5").digest(input.getBytes))
  }
}
