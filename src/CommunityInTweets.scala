import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.io.File


object CommunityInTweets{

  val data_file_path = "data/extractedTweetGraph/"
  val file_tweets = "twitter_combined_n.txt"
  val col_text_tweet = 7 //10 per covid, 7 per isis, non serve per il dataset Tweets_combined

  def main(args:Array[String]):Unit = {
    val sparkConf = new SparkConf()
      .setAppName("CommunityInTweets")
      .setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    if (File(data_file_path+file_tweets).exists & file_tweets == "twitter_combined_n.txt"){
      val edges_raw = sc.textFile(data_file_path+file_tweets)
      val edges = edges_raw.map(x => {
        val splitted = x.replace("(", "").replace(")", "").split(",").toList
        (splitted.head, splitted(1))
      })
      val raw_edges = edges.countByValue().map(x => (x._1._1, x._1._2, x._2))
      val graph_edges = raw_edges.map(x => Edge(x._1.##.toLong, x._2.##.toLong, (x._3)))
      val RDDEdge = sc.parallelize(graph_edges.toList)
      val graph = Graph.fromEdges(RDDEdge, 0L)
      val RDDVertex = graph.vertices.map(x=>(x._1))
      TweetGraph.create_graph(sc, RDDEdge, RDDVertex, data_file_path)
    }
    else{
      """Per provare sul tweet dell'ISIS o del Covid"""
      if (File(data_file_path+"/edges/part-00000").exists && File(data_file_path+"/nodes/part-00000").exists){
        val edges_raw = sc.textFile(data_file_path+file_tweets)
        val nodes = sc.textFile(data_file_path+"/nodes/part-00000")
        val edges = edges_raw.map(x=>{
          val splitted = x.replace("(", "").replace(")", "").split(",").toList
          (splitted.head, splitted(1))
        })
        val raw_edges = edges.countByValue().map(x => (x._1._1, x._1._2, x._2))
        val graph_edges = raw_edges.map(x => Edge(x._1.##.toLong, x._2.##.toLong, (x._3)))
        val RDDEdge = sc.parallelize(graph_edges.toList).distinct()
        val graph = Graph.fromEdges(RDDEdge, 0L)
        val RDDVertex = graph.vertices.map(x => (x._1))
        TweetGraph.create_graph(sc, RDDEdge, RDDVertex, data_file_path)
      }
      else{
        val x = create_nodes_edges_file(sc)
        val edges = x._1
        val nodes = x._2
        val raw_edges = edges.countByValue().map(x => (x._1._1, x._1._2, x._2))
        val graph_edges = raw_edges.map(x => Edge(x._1.##.toLong, x._2.##.toLong, (x._3)))
        val RDDEdge = sc.parallelize(graph_edges.toList).distinct()
        val graph = Graph.fromEdges(RDDEdge, 0L)
        val RDDVertex = graph.vertices.map(x => (x._1))
        TweetGraph.create_graph(sc, RDDEdge, RDDVertex, data_file_path)
      }
    }
  }

  def create_nodes_edges_file(sc:SparkContext): (RDD[(String, String)], RDD[String]) ={
    var tweets = sc.textFile(data_file_path+file_tweets)
    tweets = tweets.filter(filter_value)
    val authors = tweets.map(x => x.split(",").toList(1)).filter(x => x.strip().nonEmpty).map(x=>x.replace(":", "")).distinct()
    val cited_tweet = tweets.map(x => x.split(",").toList(col_text_tweet).split(" ").filter(w => w.contains('@')))
    val cited_account = cited_tweet.map(x => x.toList.map(w =>
      (w.substring(w.lastIndexOf("@") + 1))
    )).flatMap(list => list).filter(x => x.strip().nonEmpty).map(x=>x.replace(":", "")).distinct()
    val nodes = (authors ++ cited_account).distinct()
    val edges = tweets.cartesian(cited_account).filter(x => {
      x._1.split(",").toList(col_text_tweet).contains(x._2)
    }).map(x => {
      (x._1.split(",").toList(1), (x._2))
    })
    edges.foreach(println)
    edges.coalesce(1).saveAsTextFile(data_file_path+"/edges")
    nodes.coalesce(1).saveAsTextFile(data_file_path+"/nodes")
    (edges, nodes)
  }

  def filter_value(x: String): Boolean ={
    val splitted = x.split(',').toList
    splitted.length >= col_text_tweet
  }

}

