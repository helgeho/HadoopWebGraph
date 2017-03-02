# Hadoop WebGraph InputFormat

This input format enables the use of graphs in the *[WebGraph](http://webgraph.di.unimi.it/)* format (*[BVGraph](http://webgraph.di.unimi.it/docs/it/unimi/dsi/webgraph/BVGraph.html)*) on clusters running [Hadoop](http://hadoop.apache.org/) or [Spark](http://spark.apache.org/).
Graphs must be provided as required by *WebGraph* in the form of three files stored on HDFS: `basename.graph`, `basename.offsets`, `basename.properties`.
A nice collection of such graphs is available for download on http://law.di.unimi.it/datasets.php.
  
The input format loads the nodes of a graph in parallel, distributed according to the number of splits specified (default: 100). Each node is loaded by its ID (key) and an array of successor IDs (value), i.e., its neighbors connected by outgoing edges.

Loading a *WebGraph* with Spark using this input format works as follows:

```scala
import de.l3s.mapreduce.webgraph.io._

WebGraphInputFormat.setBasename(sc.hadoopConfiguration, "/hdfs/path/to/webgraph/basename")
WebGraphInputFormat.setNumberOfSplits(sc.hadoopConfiguration, 100)

val rdd = sc.newAPIHadoopRDD(sc.hadoopConfiguration, classOf[WebGraphInputFormat], classOf[IntWritable], classOf[IntArrayWritable])
```

To transform this into an RDD of tuples in the form of `(ID, successor IDs)` run:

```scala
val adjacencyList = rdd.map{case (k,v) => (k.get, v.values.toList)}
```

The following code counts the number of edges in the graph:

```scala
rdd.map{case (k,v) => v.values.size}.reduce(_ + _)
```

## GraphX

Once loaded in Spark, a graph can also be used with [GraphX](http://spark.apache.org/graphx/), Spark's graph framework:

```scala
import org.apache.spark.graphx._

val edges = rdd.flatMap{case (id, out) => out.values.map(outId => (id.get.toLong, outId.toLong))}
val graph = Graph.fromEdgeTuples(edges, true)
```

Now, edges and nodes can be counted as follows:

```scala
graph.numVertices
graph.numEdges
```

## Build

To build a JAR file that can be added to your classpath, simple use `MVN`:

```
git clone https://github.com/helgeho/HadoopWebGraph.git
cd HadoopWebGraph
mvn package
```

## License

This project has been published under [GPL 3.0](https://www.gnu.org/licenses/gpl-3.0.txt), since this license is used by *[WebGraph](http://webgraph.di.unimi.it/)*, of which parts of the code are reused here. 