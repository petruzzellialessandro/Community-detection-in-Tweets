import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.collection.mutable

object TweetGraph{

  def create_graph(sc: SparkContext, edges: RDD[Edge[Long]], nodes: RDD[Long], data_file_path:String): Unit ={

    val nodes_label = nodes.map(x => (x.toLong, x.toString))
    //SimilarInterestComm(edges, nodes_label, thWeight = 5, thInDegree = 3, data_file_path)
    //StrongInteractingCommunities(edges, nodes_label, thWeight = 5, thInDegree = 3, thCtMember = 5, data_file_path)
    //StrongInteractingCommunitiesWInnerCircle(sc, edges, nodes_label, thWeight = 5, thInDegree = 3, thCtMember = 5, data_file_path)
    //Dencast(edges, nodes_label, 0.1 , data_file_path)

  }


  def SimilarInterestComm(edgeRDD: RDD[Edge[Long]], vertexRDD: RDD[(Long, String)], thWeight: Int, thInDegree: Int, data_file_path:String): Unit ={
    val filteredEdges = edgeRDD.filter(x=> x.attr>thWeight)
    val graph = Graph(vertexRDD, filteredEdges)
    val filteredVertex = graph.inDegrees.filter(x=> x._2>thInDegree).map(x=>(x._1, x._2.toLong))
    val filtered_graph = Graph(filteredVertex, filteredEdges)
    val edges_com = filtered_graph.edges.map(x=>(x.srcId, x.dstId)).groupBy(x=>x._1).map(x=>(x._1, (x._2.toList)))
    val edges_com_n = edges_com.map(x=>(x._1, (x._2.map(y=>y._2)))).sortBy(x=>(x._1))
    var graph_com = edges_com_n.map(x=>x._2.map(y=> (x._1, y))).flatMap(x=>x)
    graph_com = graph_com.map(x=>(x._2, x._1))
    graph_com.coalesce(1).saveAsTextFile(data_file_path+"/results/outputNodeSIC")
    filteredEdges.map(x=>(x.srcId, x.dstId)).coalesce(1).saveAsTextFile(data_file_path+"/results/outputGraphSIC")
  }

  def PreElab(edgeRDD: RDD[Edge[Long]], vertexRDD: RDD[(Long, String)], thWeight: Int, thInDegree: Int, thCtMember: Int):  Graph[VertexId, Long] = {
    val filteredEdges = edgeRDD.filter(x => x.attr > thWeight)
    val graph = Graph(vertexRDD, filteredEdges)
    val filteredVertex = graph.inDegrees.filter(x => x._2 > thInDegree).map(x => (x._1, x._2.toLong))
    val filtered_graph = Graph(filteredVertex, filteredEdges)
    val temp_grap = filtered_graph.stronglyConnectedComponents(2)
    val com_ID = temp_grap.vertices.groupBy(x=>x._2).filter(x=> x._2.toList.length>thCtMember).map(x=>x._1).collect().toList
    val filteredGraphWCom = temp_grap.vertices.filter(x=>com_ID.contains(x._2))
    Graph(filteredGraphWCom,temp_grap.edges)
  }

  def StrongInteractingCommunities(edgeRDD: RDD[Edge[Long]], vertexRDD: RDD[(Long, String)], thWeight: Int, thInDegree: Int, thCtMember: Int, data_file_path:String): Unit ={
    val sccs = PreElab(edgeRDD, vertexRDD, thWeight, thInDegree, thCtMember)
    val graph_com = sccs.vertices.sortBy(x => x._2)
    graph_com.coalesce(1).saveAsTextFile(data_file_path+"/results/outputNodeSC")
    sccs.edges.map(x=>(x.srcId, x.dstId)).coalesce(1).saveAsTextFile(data_file_path+"/results/outputGraphSC")
  }

  def StrongInteractingCommunitiesWInnerCircle(sc:SparkContext, edgeRDD: RDD[Edge[Long]], vertexRDD: RDD[(Long, String)], thWeight: Int, thInDegree: Int, thCtMember: Int, data_file_path: String): Unit ={
    val sccs = PreElab(edgeRDD, vertexRDD, thWeight, thInDegree, thCtMember)
    val filteredEdges = edgeRDD.filter(x => x.attr > thWeight)
    val graph = Graph(vertexRDD, filteredEdges)
    val filteredVertex = graph.inDegrees.filter(x => x._2 > thInDegree).map(x => (x._1, x._2.toLong))
    val filtered_graph = Graph(filteredVertex, filteredEdges)
    val filteredVertex_into = filtered_graph.edges.map(x=>(x.dstId, x.srcId))
    val dfIntoSCC = sccs.vertices.join(filteredVertex_into)
    val filteredVertex_from = filtered_graph.edges.map(x => (x.srcId, x.dstId))
    val dfFromSCC = sccs.vertices.join(filteredVertex_from)
    val dfComExpand = dfIntoSCC.union(dfFromSCC).map(x=>(x._1, x._2._1, x._2._2))
    val comSCC = findComSCIC(dfComExpand.sortBy(x=>x._2, ascending = true))
    val graph_com = sc.parallelize(comSCC.map(x => x._1.split(",").toList).flatMap(x => x.map(y => (y.toLong, x.head.toLong))).toList)
    graph_com.coalesce(1).saveAsTextFile(data_file_path + "/results/outputNodeSCIC")
    sccs.edges.map(x => (x.srcId, x.dstId)).coalesce(1).saveAsTextFile(data_file_path + "/results/outputGraphSCIC")
  }

  def findComSCIC(dfComExpand: RDD[(VertexId, VertexId, VertexId)]): Map[String, Int] = {
    val dfcom = dfComExpand.collect()
    var comSCC = Map[String, Int]()
    var prevIdCom = dfcom.head._2
    var keyStr = ""
    keyStr += dfcom.head._1.toString
    keyStr += ","
    keyStr += dfcom.head._3.toString
    var nMember = 2
    for (row <- dfcom) {
      if (prevIdCom == row._2) {
        keyStr += ","
        keyStr += row._3.toString
        nMember += 1
      }
      else {
        comSCC = comSCC.updated(keyStr, nMember)
        prevIdCom = row._2
        keyStr = row._3.toString
        nMember = 1
      }
    }
    comSCC
  }

  def Dencast(edgeRDD: RDD[Edge[Long]], vertexRDD: RDD[(Long, String)],labelChangeRate: Double, data_file_path: String):Unit ={
    val thWeight = 0
    val filtered_edge = edgeRDD.filter(x=>x.attr>thWeight)
    val cores = mutable.MutableList[Long]()
    Graph(vertexRDD, filtered_edge).inDegrees.sortBy(x=>x._2, ascending = false).take(5).map(x=>x._1).foreach(x=>{
      cores += x
    })
    var clusterID=0
    var clusters = vertexRDD.map(x=>(
      if (cores.contains(x._1)){
        clusterID += 1
        (x._1, clusterID)
      }
      else{
        (x._1, 0)
      }
    ))
    val temp_cl = clusters.filter(x=>x._2>0).map(x=>(x._1, x._2)).collect().toMap
    val edge_n = edgeRDD.map(x=>(
      (x.srcId, temp_cl.getOrElse(x.srcId, 0)), (x.dstId, temp_cl.getOrElse(x.dstId, 0)), x.attr
    ))
    val threshold = (labelChangeRate * filtered_edge.count()).toInt
    println(threshold)
    var propagations = 0
    do{
      propagations = 0
      clusters =edge_n.map({
        case ((src, srcID), (dst, dstID), w)=>if (cores.contains(src) && srcID>=dstID  ) {
          println(propagations+" "+cores.contains(src)+" "+src+" "+dst+" "+srcID+" "+dstID)
          propagations +=1
          (dst, srcID, w)
        }
        else{
          (dst, dstID, w)
        }
      }).filter(x=>x._3>thWeight).map(x=>(x._1, x._2))
      clusters = clusters.reduceByKey(math.max)
    }while(propagations<threshold)

    val node_label = clusters.cartesian(vertexRDD).filter(x => x._1._1 == x._2._1).map(x => (x._1._1, x._2._2, x._1._2))
    node_label.foreach(println)
    node_label.coalesce(1).saveAsTextFile(data_file_path + "/results/outputNodeDencast")
    filtered_edge.map(x => (x.srcId, x.dstId)).coalesce(1).saveAsTextFile(data_file_path + "/results/outputDencast")
  }
}
