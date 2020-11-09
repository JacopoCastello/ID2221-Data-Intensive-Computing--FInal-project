// Databricks notebook source
import twitter4j.FilterQuery
import scala.io.Source 
import java.time.LocalDateTime

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils

import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder

   
    System.setProperty("twitter4j.oauth.consumerKey", "Lt3MK1oWU2ew8D377JpLElFVn")
    System.setProperty("twitter4j.oauth.consumerSecret", "7MyEAw7DFzG9WhXOl2wvVJ4Vjy89NTHKCEzwNroQ6xywKgIpoW")
    System.setProperty("twitter4j.oauth.accessToken", "1177594783608516608-mDQ0N1WqsxZYOKgucd1Jc9vy7Hai91")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "U0QRrVLnwhWuXyhdc65Sjx2CgaIHkP8ATZ0KbZ45NrlDN")

val outputDirectory = "/twetr"

dbutils.fs.rm(outputDirectory, true)

var n = 0

val runningTime = 600 * 1000

val locFilter = {
      System.out.println("Using location for Europe, new results every 10s: ")
      val southWest = Array(-5.38, 36.12)  //Stockholm Array(17.93, 59.29)   //San Francisco Array(-122.75, 36.8) //NY Array(-74.70, 40.34)
      val northEast = Array(24.88, 65.20) //Stockholm Array(18.17, 59.46)    //San Francisco Array(-121.75, 37.8) //NY Array(-73.26, 41.18)
      Array(southWest, northEast)
    } 

val locQuery = new FilterQuery().locations(locFilter : _*)
    

    val ssc = new StreamingContext(sc, Seconds(10))
    val stream = TwitterUtils.createFilteredStream(ssc, None, Some(locQuery))

    val hashtags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

    // Read in the word-sentiment list and create a static RDD from it
    val path = "dbfs:/data/sent.txt"
    val words = ssc.sparkContext.textFile(path).map{ line =>
     val Array(word, happinessValue) = line.split("\t")
      (word, happinessValue.toInt)
    }.cache()

 val wordFromTags = hashtags.map(hash => (hash.tail, 1))
 val tagsOccurency = wordFromTags.reduceByKeyAndWindow(_ + _, Seconds(60))
 val tagsJoinWords = tagsOccurency.transform{agg => words.join(agg)}
 val tagsEvaluation = tagsJoinWords.map{case (h, v) => (h, v._1 * v._2)}
 val tagsRanking = tagsEvaluation.map{case (h, v) => (v, h)}.transform(_.sortByKey(false))
     
 var total = ""
 

    tagsRanking.foreachRDD(rdd => {
      val top10 = rdd.take(10)
      println("\nHappiest Hashtags in last 60 seconds (%s total):".format(rdd.count()))
      top10.foreach{case (v, tag) => println("%s (%s happiness score)".format(tag, v))}
      
      var newline = "\n\nHappiest tags at "
      var time = (LocalDateTime.now()).toString//(System.currentTimeMillis()/1000).toString
      var end = ":\n"      
      time = time.concat(end)
      
      newline = newline.concat(time)
      
      
      total = total.concat(newline)
      total = total.concat(top10.mkString("\n"))
      
      dbutils.fs.put(s"${outputDirectory}/tweetsEvaluation", total, true)
      
    })


  

    

    ssc.start()
    ssc.awaitTerminationOrTimeout(runningTime)

// COMMAND ----------


