package learning;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.storage.StorageLevel;
import org.junit.Test;
import scala.Tuple2;
import scala.reflect.ClassTag;

import java.util.ArrayList;
import java.util.List;

public class VertexEdgeTest {

    @Test
    public void testGraphCreation() {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("GraphLearning");
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

        List<Tuple2<Object, String>> vertices = new ArrayList<Tuple2<Object, String>>();

        vertices.add(new Tuple2<Object, String>((long) 11, "James"));
        vertices.add(new Tuple2<Object, String>((long) 21, "Robert"));
        vertices.add(new Tuple2<Object, String>((long) 31, "Charlie"));
        vertices.add(new Tuple2<Object, String>((long) 41, "Roger"));
        vertices.add(new Tuple2<Object, String>((long) 51, "Tony"));

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

        Graph<String, String> graph = Graph.apply(verticesRDD.rdd(), edgesJavaRDD.rdd(), "",
                StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), stringTag, stringTag);

        graph.vertices().toJavaRDD().collect().forEach(System.out::println);
        graph.edges().toJavaRDD().collect().forEach(System.out::println);
    }
}
