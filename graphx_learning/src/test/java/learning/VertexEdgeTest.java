package learning;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.EdgeTriplet;
import org.apache.spark.graphx.Graph;
import org.apache.spark.rdd.RDD;
import org.apache.spark.storage.StorageLevel;
import org.junit.Test;
import scala.Tuple2;
import scala.reflect.ClassTag;

import java.util.ArrayList;
import java.util.List;

public class VertexEdgeTest {

    @Test
    public void testGraphCreation() {
        // Spark configuration with master url set to local
        SparkConf conf = new SparkConf().setMaster("local").setAppName("GraphLearning");
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

        // Tuple (id, name)
        List<Tuple2<Object, String>> vertices = new ArrayList<Tuple2<Object, String>>();

        vertices.add(new Tuple2<Object, String>((long) 1, "James"));
        vertices.add(new Tuple2<Object, String>((long) 2, "Robert"));
        vertices.add(new Tuple2<Object, String>((long) 3, "Charlie"));
        vertices.add(new Tuple2<Object, String>((long) 4, "Roger"));
        vertices.add(new Tuple2<Object, String>((long) 5, "Tony"));

        // Edges (from, to, property)
        List<Edge<String>> edges = new ArrayList<Edge<String>>();

        edges.add(new Edge<String>((long) 1, (long) 2, "Friend"));
        edges.add(new Edge<String>((long) 2, (long) 3, "Advisor"));
        edges.add(new Edge<String>((long) 1, (long) 3, "Friend"));
        edges.add(new Edge<String>((long) 4, (long) 3, "colleague"));
        edges.add(new Edge<String>((long) 4, (long) 5, "Relative"));
        edges.add(new Edge<String>((long) 5, (long) 2, "BusinessPartners"));

        JavaRDD<Tuple2<Object, String>> verticesRDD = javaSparkContext.parallelize(vertices);
        JavaRDD<Edge<String>> edgesJavaRDD = javaSparkContext.parallelize(edges);

        ClassTag<String> stringTag = scala.reflect.ClassTag$.MODULE$.apply(String.class);

        // Creates Graph with vertices, edges, default property value set to "", edge storage level,
        // vertices storage level, class tag for vertices, class tag for edges
        Graph<String, String> graph = Graph.apply(verticesRDD.rdd(), edgesJavaRDD.rdd(), "",
                StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), stringTag, stringTag);

        graph.vertices().toJavaRDD().collect().forEach(System.out::println);
        graph.edges().toJavaRDD().collect().forEach(System.out::println);
    }

    /**
     * No property with vertices. Map tranformation needs to execute on verticies to associate the property with verticies.
     * TODO details on above.
     */
    @Test
    public void fromEdgesOnly() {
        // Spark configuration with master url set to local
        SparkConf conf = new SparkConf().setMaster("local").setAppName("GraphLearning");
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

         // Edges (from, to, property)
        List<Edge<String>> edges = new ArrayList<Edge<String>>();

        edges.add(new Edge<String>((long) 1, (long) 2, "Friend"));
        edges.add(new Edge<String>((long) 2, (long) 3, "Advisor"));
        edges.add(new Edge<String>((long) 1, (long) 3, "Friend"));
        edges.add(new Edge<String>((long) 4, (long) 3, "colleague"));
        edges.add(new Edge<String>((long) 4, (long) 5, "Relative"));
        edges.add(new Edge<String>((long) 5, (long) 2, "BusinessPartners"));

        JavaRDD<Edge<String>> edgesJavaRDD = javaSparkContext.parallelize(edges);

        ClassTag<String> stringTag = scala.reflect.ClassTag$.MODULE$.apply(String.class);

        Graph<String, String> graph = Graph.fromEdges(edgesJavaRDD.rdd(), "", StorageLevel.MEMORY_ONLY(),
                StorageLevel.MEMORY_ONLY(), stringTag, stringTag);


        graph.edges().toJavaRDD().collect().forEach(System.out::println);
    }

    /**
     * Edge triplets will show destination node id with its attribute (string in this case) and origin
     * node id with its attribute.
     */
    @Test
    public void edgeTriplets() {
                 // Spark configuration with master url set to local
        SparkConf conf = new SparkConf().setMaster("local").setAppName("GraphLearning");
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

        // Tuple (id, name)
        List<Tuple2<Object, String>> vertices = new ArrayList<Tuple2<Object, String>>();

        vertices.add(new Tuple2<Object, String>((long) 1, "James"));
        vertices.add(new Tuple2<Object, String>((long) 2, "Robert"));
        vertices.add(new Tuple2<Object, String>((long) 3, "Charlie"));
        vertices.add(new Tuple2<Object, String>((long) 4, "Roger"));
        vertices.add(new Tuple2<Object, String>((long) 5, "Tony"));

        // Edges (from, to, property)
        List<Edge<String>> edges = new ArrayList<Edge<String>>();

        edges.add(new Edge<String>((long) 1, (long) 2, "Friend"));
        edges.add(new Edge<String>((long) 2, (long) 3, "Advisor"));
        edges.add(new Edge<String>((long) 1, (long) 3, "Friend"));
        edges.add(new Edge<String>((long) 4, (long) 3, "colleague"));
        edges.add(new Edge<String>((long) 4, (long) 5, "Relative"));
        edges.add(new Edge<String>((long) 5, (long) 2, "BusinessPartners"));

        JavaRDD<Tuple2<Object, String>> verticesRDD = javaSparkContext.parallelize(vertices);
        JavaRDD<Edge<String>> edgesJavaRDD = javaSparkContext.parallelize(edges);

        ClassTag<String> stringTag = scala.reflect.ClassTag$.MODULE$.apply(String.class);

        // Creates Graph with vertices, edges, default property value set to "", edge storage level,
        // vertices storage level, class tag for vertices, class tag for edges
        Graph<String, String> graph = Graph.apply(verticesRDD.rdd(), edgesJavaRDD.rdd(), "",
                StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), stringTag, stringTag);


        RDD<EdgeTriplet<String, String>> tripletRDD = graph.triplets();

        tripletRDD.toJavaRDD().collect().forEach(System.out::println);
    }
}
