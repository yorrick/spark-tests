import java.io.File
import com.typesafe.config.ConfigFactory
import org.apache.spark.rdd.RDD

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

  val interval = Seconds(5)
  val window = Seconds(30)

  def main(args: Array[String]) {
    configureTwitterOAuth()


    val ssc = new StreamingContext("local[4]", "TwitterStream", interval)
    val stream = TwitterUtils.createStream(ssc, None, args)

    val hashTags: DStream[String] = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

    val pairs: DStream[(String, Int)] = hashTags.map(tag => (tag, 1))
    val sum = (c1: Int, c2: Int) => c1 + c2
    val countByHashTag: DStream[(String, Int)] = pairs reduceByKeyAndWindow(sum, window)
    val hashTagByCount: DStream[(Int, String)] = countByHashTag map {case (tag, count) => (count, tag)}
    val topCounts60: DStream[(Int, String)] = hashTagByCount.transform(_.sortByKey(false))

    topCounts60 foreachRDD { rdd: RDD[(Int, String)] =>
      val topList: Seq[(Int, String)] = rdd.take(10)
      println(s"\nPopular topics in last ${window.milliseconds / 1000} seconds (${rdd.count}) total):")
      topList foreach { case (count, tag) => println(s"$count: $tag")}
    }

    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate
  }
}