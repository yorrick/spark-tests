import java.io.File
import com.typesafe.config.ConfigFactory

import org.apache.spark.streaming.{Seconds, StreamingContext}
import StreamingContext._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.dstream.DStream


object TwitterStream {

  def configureTwitterOAuth() {
    val conf = ConfigFactory.parseFile(new File("conf/application.conf"))

    System.setProperty("twitter4j.oauth.consumerKey", conf.getString("consumerKey"))
    System.setProperty("twitter4j.oauth.consumerSecret", conf.getString("consumerSecret"))
    System.setProperty("twitter4j.oauth.accessToken", conf.getString("accessToken"))
    System.setProperty("twitter4j.oauth.accessTokenSecret", conf.getString("accessTokenSecret"))
  }

  def main(args: Array[String]) {
    configureTwitterOAuth()

    val interval = Seconds(5)

    val ssc = new StreamingContext("local[4]", "TwitterStream", interval)
    val stream = TwitterUtils.createStream(ssc, None, args)

    val hashTags: DStream[String] = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

    val pairs: DStream[(String, Int)] = hashTags.map(tag => (tag, 1))
    val countByHashTag: DStream[(String, Int)] = pairs .reduceByKeyAndWindow(_ + _, Seconds(60))
    val hashTagByCount: DStream[(Int, String)] = countByHashTag map {case (tag, count) => (count, tag)}
    val topCounts60 = hashTagByCount.transform(_.sortByKey(false))

    topCounts60.print()

    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate
  }
}