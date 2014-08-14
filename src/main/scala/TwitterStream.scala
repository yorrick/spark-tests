import java.io.File

import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.api.java.function._
import org.apache.spark.streaming._
import org.apache.spark.streaming.api._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter.TwitterUtils

import com.typesafe.config.{Config, ConfigFactory}


object TwitterStream {

  def configureTwitterOAuth() {
    val conf = ConfigFactory.parseFile(new File("conf/application.conf"))

    System.setProperty("twitter4j.oauth.consumerKey", conf.getString("consumerKey"))
    System.setProperty("twitter4j.oauth.consumerSecret", conf.getString("consumerSecret"))
    System.setProperty("twitter4j.oauth.accessToken", conf.getString("accessToken"))
    System.setProperty("twitter4j.oauth.accessTokenSecret", conf.getString("accessTokenSecret"))
  }

  val filters = Seq("scala")

  def main(args: Array[String]) {
    configureTwitterOAuth()

    val ssc = new StreamingContext("local[2]", "TwitterStream", Seconds(5))
    val stream = TwitterUtils.createStream(ssc, None, filters)

    stream.print()

    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate
  }
}