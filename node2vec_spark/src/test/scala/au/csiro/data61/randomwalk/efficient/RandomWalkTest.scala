package au.csiro.data61.randomwalk.efficient

import au.csiro.data61.Main.Params
import au.csiro.data61.randomwalk.naive
import org.apache.spark.graphx.{EdgeDirection, Graph}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.BeforeAndAfter

class RandomWalkTest extends org.scalatest.FunSuite with BeforeAndAfter {

  private val master = "local[4]"
  private val appName = "rw-unit-test"
  private var sc: SparkContext = _

  before {
    val conf = new SparkConf().setMaster(master).setAppName(appName)
    sc = SparkContext.getOrCreate(conf)
  }

  after {
    if (sc != null) {
      sc.stop()
    }
    GraphMap.reset
  }

  test("load graph as undirected") {
    val config = Params(input = "./src/test/graph/karate.txt", directed = false)
    val rw = RandomWalk(sc, config)
    val paths = rw.loadGraph() // loadGraph(int)
    assert(rw.nEdges == 156)
    assert(rw.nVertices == 34)
    assert(paths.count() == 34)
    val vAcc = sc.longAccumulator("v")
    val eAcc = sc.longAccumulator("e")
    paths.coalesce(1).mapPartitions { iter =>
      vAcc.add(GraphMap.getNumVertices)
      eAcc.add(GraphMap.getNumEdges)
      iter
    }.first()
    assert(eAcc.sum == 156)
    assert(vAcc.sum == 34)
  }

  test("load graph as directed") {
    val config = Params(input = "./src/test/graph/karate.txt", directed = true)
    val rw = RandomWalk(sc, config)
    val paths = rw.loadGraph()
    assert(rw.nEdges == 78)
    assert(rw.nVertices == 34)
    assert(paths.count() == 34)
    val vAcc = sc.longAccumulator("v")
    val eAcc = sc.longAccumulator("e")
    paths.coalesce(1).mapPartitions { iter =>
      vAcc.add(GraphMap.getNumVertices)
      eAcc.add(GraphMap.getNumEdges)
      iter
    }.first()
    assert(eAcc.sum == 78)
    assert(vAcc.sum == 34)
  }

  test("the first step of Random Walk") {
    val config = Params(input = "./src/test/graph/testgraph.txt", directed = true)
    val rw = RandomWalk(sc, config)
    val paths = rw.loadGraph()
    val result = rw.doFirsStepOfRandomWalk(paths)
    assert(result.count == paths.count())
    for (t <- result.collect()) {
      val p = t._2
      if (p.length == 2) {
        assert(p.head == 1)
        assert(p sameElements Array(1L, 2L))
      }
      else {
        assert(p.head == 2)
        assert(p sameElements Array(2L))
      }
    }
  }

  test("test 2nd order random walk undirected1") {
    // Undirected graph
    val rValue = 0.1
    val wLength = 1
    val nextDoubleGen = () => rValue
    val config = Params(input = "./src/test/graph/karate.txt", directed = false, walkLength =
      wLength, rddPartitions = 8, numWalks = 1)
    val rw = RandomWalk(sc, config)
    val graph = rw.loadGraph()
    val paths = rw.randomWalk(graph, nextDoubleGen)
    val nSampler = naive.RandomSample(nextDoubleGen)
    assert(paths.count() == rw.nVertices) // a path per vertex
    val baseRw = naive.RandomWalk(sc, config).loadGraph()
    paths.collect().foreach { case (p: Array[Long]) =>
      val p2 = doSecondOrderRandomWalk(baseRw, p(0), wLength, nSampler)
      assert(p sameElements p2)
    }
  }

  test("test 2nd order random walk undirected2") {
    // Undirected graph
    val rValue = 0.1
    val nextDoubleGen = () => rValue
    val nSampler = naive.RandomSample(nextDoubleGen)
    val wLength = 50
    val config = Params(input = "./src/test/graph/karate.txt", directed = false, walkLength =
      wLength, rddPartitions = 8, numWalks = 1)
    val rw = RandomWalk(sc, config)
    val graph = rw.loadGraph()
    val paths = rw.randomWalk(graph, nextDoubleGen)
    assert(paths.count() == rw.nVertices) // a path per vertex
    val baseRw = naive.RandomWalk(sc, config).loadGraph()
    paths.collect().foreach { case (p: Array[Long]) =>
      val p2 = doSecondOrderRandomWalk(baseRw, p(0), wLength, nSampler)
      assert(p sameElements p2)
    }
  }

  test("test 2nd order random walk undirected3") {
    // Undirected graph
    val wLength = 50
    val config = Params(input = "./src/test/graph/karate.txt", directed = false, walkLength =
      wLength, rddPartitions = 8, numWalks = 1)
    val rValue = 0.9
    val nextDoubleGen = () => rValue
    val rw = RandomWalk(sc, config)
    val graph = rw.loadGraph()
    val paths = rw.randomWalk(graph, nextDoubleGen)
    assert(paths.count() == rw.nVertices) // a path per vertex
    val baseRw = naive.RandomWalk(sc, config).loadGraph()
    val nSampler = naive.RandomSample(nextDoubleGen)
    paths.collect().foreach { case (p: Array[Long]) =>
      val p2 = doSecondOrderRandomWalk(baseRw, p(0), wLength, nSampler)
      assert(p sameElements p2)
    }
  }

  test("test 2nd order random walk undirected4") {
    // Undirected graph
    val rValue = 0.1
    val nextDoubleGen = () => rValue
    val nSampler = naive.RandomSample(nextDoubleGen)
    val wLength = 50
    val config = Params(input = "./src/test/graph/karate.txt", directed = false, walkLength =
      wLength, rddPartitions = 8, numWalks = 1)
    val rw = RandomWalk(sc, config)
    val graph = rw.loadGraph()
    val paths = rw.randomWalk(graph, nextDoubleGen)
    assert(paths.count() == rw.nVertices) // a path per vertex
    val baseRw = naive.RandomWalk(sc, config).loadGraph()
    paths.collect().foreach { case (p: Array[Long]) =>
      val p2 = doSecondOrderRandomWalk(baseRw, p(0), wLength, nSampler)
      assert(p sameElements p2)
    }
  }

  test("test 2nd order random walk directed1") {
    val wLength = 50
    val rValue = 0.9
    val nextDoubleGen = () => rValue

    // Directed Graph
    val config = Params(input = "./src/test/graph/karate.txt", directed = true, walkLength =
      wLength, rddPartitions = 8, numWalks = 1)
    val rw = RandomWalk(sc, config)
    val graph = rw.loadGraph()
    val paths = rw.randomWalk(graph, nextDoubleGen)
    val rSampler = RandomSample(nextDoubleGen)
    val nSampler = naive.RandomSample(nextDoubleGen)
    assert(paths.count() == rw.nVertices) // a path per vertex
    val baseRw = naive.RandomWalk(sc, config).loadGraph()
    paths.collect().foreach { case (p: Array[Long]) =>
      val p2 = doSecondOrderRandomWalk(baseRw, p(0), wLength, nSampler)
      assert(p sameElements p2)
    }
  }

  test("test 2nd order random walk directed2") {
    // Undirected graph
    val wLength = 50

    // Directed Graph
    val config = Params(input = "./src/test/graph/karate.txt", directed = true, walkLength =
      wLength, rddPartitions = 8, numWalks = 1)
    val rw = RandomWalk(sc, config)
    val rValue = 0.1
    val nextDoubleGen = () => rValue
    val graph = rw.loadGraph()
    val paths = rw.randomWalk(graph, nextDoubleGen)
    val nSampler = naive.RandomSample(nextDoubleGen)
    assert(paths.count() == rw.nVertices) // a path per vertex
    val baseRw = naive.RandomWalk(sc, config).loadGraph()
    paths.collect().foreach { case (p: Array[Long]) =>
      val p2 = doSecondOrderRandomWalk(baseRw, p(0), wLength, nSampler)
      assert(p sameElements p2)
    }
  }

  private def doSecondOrderRandomWalk(graph: Graph[Array[Long], Double], src: Long,
                                      walkLength: Int, rSampler: naive.RandomSample): Array[Long]
  = {
    val v2N = graph.collectEdges(EdgeDirection.Out)
    var path = Array(src)
    val neighbors = v2N.lookup(src)
    if (neighbors.length > 0) {
      path = rSampler.sample(v2N.lookup(src)(0)) match {
        case Some(e) => path ++ Array(e.dstId)
      }
    }
    else {
      return path
    }

    for (_ <- 0 until walkLength) {

      val curr = path.last
      val prev = path(path.length - 2)
      val currNeighbors = v2N.lookup(curr)
      if (currNeighbors.length > 0) {
        val prevNeighbors = Some(v2N.lookup(prev)(0))
        path = rSampler.secondOrderSample()(prev, prevNeighbors, currNeighbors(0)) match {
          case Some(e) => path ++ Array(e.dstId)
          case None => path
        }
      } else {
        return path
      }
    }

    path
  }

}