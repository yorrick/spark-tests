import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._

import org.apache.spark.api.java.function._
import org.apache.spark.streaming._
import org.apache.spark.streaming.api._
import org.apache.spark.streaming.dstream.DStream


object NetworkWordCount {
  def main(args: Array[String]) {
  	val ssc = new StreamingContext("local[2]", "NetworkWordCount", Seconds(5))

  	val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.split(" "))

    // Count each word in each batch
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    // Print a few of the counts to the console
    wordCounts.print()

    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate
  }
}