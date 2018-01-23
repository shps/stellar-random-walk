package au.csiro.data61.randomwalk.algorithm

import au.csiro.data61.randomwalk.common.Params
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.util.{Random, Try}

case class VCutRandomWalk(context: SparkContext,
                          config: Params) extends RandomWalk {

  def loadGraph(hetero: Boolean, bcMetapath: Broadcast[Array[Short]]): RDD[(Int, (Array[Int]))] = {

    val edgePartitions = hetero match {
      case true => loadHeteroGraph().partitionBy(partitioner).persist(StorageLevel.MEMORY_AND_DISK)
      case false => loadHomoGraph().partitionBy(partitioner).persist(StorageLevel.MEMORY_AND_DISK)
    }

    val vertexPartitions = edgePartitions.mapPartitions({ iter =>
      iter.map { case (src, (_, pId, _)) =>
        (src, pId)
      }
    }, preservesPartitioning = true).cache()

    val g = vertexPartitions.join(edgePartitions).map {
      case (v, (pId, (neighbors, _, vType))) => (pId, (v, neighbors, vType))
    }.partitionBy(partitioner).persist(StorageLevel.MEMORY_AND_DISK)

    routingTable = buildRoutingTable(g).persist(StorageLevel.MEMORY_ONLY)
    routingTable.count()

    val vAccum = context.longAccumulator("vertices")
    val eAccum = context.longAccumulator("edges")

    val rAcc = context.collectionAccumulator[Int]("replicas")
    val lAcc = context.collectionAccumulator[Int]("links")

    g.foreachPartition { iter =>
      val (r, e) = HGraphMap.getGraphStatsOnlyOnce
      if (r != 0) {
        rAcc.add(r)
        lAcc.add(e)
      }
      iter.foreach {
        case (_, (_, edgeTypes, _)) =>
          vAccum.add(1)
          edgeTypes.foreach { case (neighbors: Array[(Int, Int, Float)], _) =>
            eAccum.add(neighbors.length)
          }
      }
    }
    nVertices = vAccum.sum.toInt
    nEdges = eAccum.sum.toInt

    logger.info(s"edges: $nEdges")
    logger.info(s"vertices: $nVertices")
    println(s"edges: $nEdges")
    println(s"vertices: $nVertices")

    val ePartitions = lAcc.value.toArray.mkString(" ")
    val vPartitions = rAcc.value.toArray.mkString(" ")
    logger.info(s"E Partitions: $ePartitions")
    logger.info(s"V Partitions: $vPartitions")
    println(s"E Partitions: $ePartitions")
    println(s"V Partitions: $vPartitions")

    val walkers = g.filter(v => v._2._3 == bcMetapath.value(0)).mapPartitions({ iter =>
      iter.map {
        case (pId: Int, (vId, _, _)) =>
          (pId, Array(vId))
      }
    }, preservesPartitioning = true
    )

    initWalkersToTheirPartitions(routingTable, walkers).persist(StorageLevel.MEMORY_AND_DISK)
  }

  def loadHomoGraph(): RDD[(Int, (Array[(Array[(Int, Int, Float)], Short)], Int, Short))] = {
    val bcDirected = context.broadcast(config.directed)
    val bcWeighted = context.broadcast(config.weighted) // is weighted?
    val bcRddPartitions = context.broadcast(config.rddPartitions)

    context.textFile(config.input, minPartitions
      = config
      .rddPartitions).flatMap { triplet =>
      val parts = triplet.split("\\s+")
      // if the weights are not specified it sets it to 1.0

      val pId: Int = parts.length > 2 match {
        case true => Try(parts(2).toInt).getOrElse(Random.nextInt(bcRddPartitions.value))
        case false => Random.nextInt(bcRddPartitions.value)
      }

      // if the weights are not specified it sets it to 1.0
      val weight = bcWeighted.value && parts.length > 3 match {
        case true => Try(parts.last.toFloat).getOrElse(1.0f)
        case false => 1.0f
      }

      val (src, dst) = (parts.head.toInt, parts(1).toInt)
      val srcTuple = (src, (Array((dst, pId, weight)), pId))

      if (bcDirected.value) {
        Array(srcTuple, (dst, (Array.empty[(Int, Int, Float)], pId)))
      } else {
        Array(srcTuple, (dst, (Array((src, pId, weight)), pId)))
      }
    }.reduceByKey((x, y) => (x._1 ++ y._1, x._2)).map { case (src, (neighbors, srcPid)) =>
      val defaultNodeType: Short = 0
      (src, (Array((neighbors, defaultNodeType)), srcPid, defaultNodeType))
    }
  }

  def loadHeteroGraph(): RDD[(Int, (Array[(Array[(Int, Int, Float)], Short)], Int, Short))] = {
    val bcDirected = context.broadcast(config.directed)
    val bcWeighted = context.broadcast(config.weighted) // is weighted?
    val bcRddPartitions = context.broadcast(config.rddPartitions)

    val edges = context.textFile(config.input, minPartitions = config.rddPartitions).flatMap {
      triplet =>
        val parts = triplet.split("\\s+")
        // if the weights are not specified it sets it to 1.0

        val pId: Int = parts.length > 2 match {
          case true => Try(parts(2).toInt).getOrElse(Random.nextInt(bcRddPartitions.value))
          case false => Random.nextInt(bcRddPartitions.value)
        }

        // if the weights are not specified it sets it to 1.0
        val weight = bcWeighted.value && parts.length > 3 match {
          case true => Try(parts.last.toFloat).getOrElse(1.0f)
          case false => 1.0f
        }

        val (src, dst) = (parts.head.toInt, parts(1).toInt)
        val dstTuple = (dst, ((src, pId, weight), pId))

        if (bcDirected.value) {
          //        Array((dst, (src, weight)), (None, (dst, None)))
          Array(dstTuple)
        } else {
          Array(dstTuple, (src, ((dst, pId, weight), pId)))
        }
      /* TODO: Check for input data correctness: e.g., no redundant edges, no bidirectional
      representations, vertex-type exists for all vertices, and so on*/
    }.partitionBy(partitioner)

    val vTypes = loadNodeTypes().partitionBy(partitioner).cache()
    appendNodeTypes(edges, vTypes, bcDirected).reduceByKey((x, y) => (x._1 ++ y._1, x._2)).map {
      case ((src, dstType), (neighbors, srcPid)) => (src, (Array((neighbors, dstType)), srcPid))
    }.reduceByKey((x, y) => (x._1 ++ y._1, x._2)).join(vTypes).map { case (src, ((neighbors, srcPid), srcType)) =>
      (src, (neighbors, srcPid, srcType))
    }
  }

  def appendNodeTypes(reversedEdges: RDD[(Int, ((Int, Int, Float), Int))], vTypes: RDD[(Int, Short)],
                      bcDirected: Broadcast[Boolean]):
  RDD[((Int, Short), (Array[(Int, Int, Float)], Int))] = {
    return reversedEdges.join(vTypes).flatMap { case (dst, (((src, srcPid, weight), dstPid), dstType)) =>
      val v = Array(((src, dstType), (Array((dst, dstPid, weight)), srcPid)))
      if (bcDirected.value) {
        v ++ Array(((dst, dstType), (Array.empty[(Int, Int, Float)], dstPid)))
      } else {
        v
      }
    }
  }

  def initWalkersToTheirPartitions(routingTable: RDD[Int], walkers: RDD[(Int, Array[Int])]) = {
    routingTable.zipPartitions(walkers.partitionBy(partitioner)) {
      (_, iter2) =>
        iter2
    }
  }

  def buildRoutingTable(graph: RDD[(Int, (Int, Array[(Array[(Int, Int, Float)], Short)], Short))]): RDD[Int] = {

    // the size must be compatible with the vertex type ids (from 0 to vTypeSize - 1)
    val bcTypeSize = context.broadcast(config.vTypeSize)

    graph.mapPartitionsWithIndex({ (id: Int, iter: Iterator[(Int, (Int, Array[(Array[(Int, Int, Float)],
      Short)], Short))]) =>
      HGraphMap.initGraphMap(bcTypeSize.value)
      iter.foreach { case (_, (vId, edgeTypes, _)) =>
        edgeTypes.foreach { case (neighbors, dstType) =>
          HGraphMap.addVertex(dstType, vId, neighbors)
        }
        id
      }
      Iterator.empty
    }, preservesPartitioning = true
    )
  }

  def prepareWalkersToTransfer(walkers: RDD[(Int, (Array[Int], Array[(Int, Float)], Boolean,
    Short))]) = {
    walkers.mapPartitions({
      iter =>
        iter.map {
          case (_, (steps, prevNeighbors, completed, mpIndex)) =>
            val pId = HGraphMap.getPartition(mpIndex, steps.last) match {
              case Some(pId) => pId
              case None => -1 // Must exists!
            }
            (pId, (steps, prevNeighbors, completed, mpIndex))
        }
    }, preservesPartitioning = false)

  }

}
