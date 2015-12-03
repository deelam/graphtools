package net.deelam.graphtools.graphfactories;

import static org.junit.Assert.*;

import java.io.IOException;

import net.deelam.graphtools.GraphTransaction;
import net.deelam.graphtools.GraphUri;
import net.deelam.graphtools.GraphUtils;

import org.apache.commons.configuration.BaseConfiguration;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Iterables;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.neo4j.Neo4jGraph;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

public class IdGraphFactoryNeo4jTest {

  @BeforeClass
  public static void setUp() throws Exception {
    IdGraphFactoryNeo4j.register();
  }

  @Test
  public void testNeo4j() {
    GraphUri gUri = new GraphUri("neo4j:./target/myDb");
    IdGraph<Neo4jGraph> graph = gUri.openIdGraph(Neo4jGraph.class);
    graph.shutdown();
  }

  @Test
  public void testNeo4jFileUri() throws IOException {
    GraphUri gUri = new GraphUri("neo4j:file:///./target/myDb");
    IdGraph<Neo4jGraph> graph = gUri.createNewIdGraph(true);
    graph.shutdown();
  }

  @Test
  public void testNeo4jAbsoluteFileUri() {
    GraphUri gUri = new GraphUri("neo4j:file:/tmp/target/myDb");
    IdGraph<Neo4jGraph> graph = gUri.openIdGraph(Neo4jGraph.class);
    graph.shutdown();
  }

  @Test
  public void testNeo4jUsingConf() throws IOException {
    BaseConfiguration conf = new BaseConfiguration();
    conf.setProperty("node_auto_indexing", "false"); // FIXME: insert neo4j setting that can be checked

    GraphUri gUri = new GraphUri("neo4j:./target/myDb", conf);
    IdGraph<Neo4jGraph> graph = gUri.createNewIdGraph(true);
    //		Neo4jGraph neo = graph.getBaseGraph();
    //		AutoIndexer<Node> autoIndexer = neo.getRawGraph().index().getNodeAutoIndexer(); // TODO: check if config took effect
    //		System.out.println(autoIndexer);
    graph.shutdown();
  }

  @Test
  public void testNeo4jUsingUriQuery() {
    GraphUri gUri = new GraphUri("neo4j:./target/myDb?node_auto_indexing=true");
    IdGraph<Neo4jGraph> graph = gUri.openIdGraph(Neo4jGraph.class);
    graph.shutdown();
  }

  @Test
  public void testTwoNeo4jUsingUriQuery() throws IOException {
    GraphUri gUri = new GraphUri("neo4j:./target/myDb?node_auto_indexing=true");
    IdGraph<Neo4jGraph> graph = gUri.createNewIdGraph(true);
    GraphUri gUri2 = new GraphUri("neo4j:./target/myDb2?node_auto_indexing=true");
    IdGraph<Neo4jGraph> graph2 = gUri2.createNewIdGraph(true);

    int tx = GraphTransaction.begin(graph);
    Vertex a = null, b = null;
    try {
      a = graph.addVertex("A");
      a.setProperty("prop", "value");
      b = graph.addVertex("B");
      graph.addEdge("E", a, b, "edgey");
      GraphTransaction.commit(tx);

      tx = GraphTransaction.begin(graph2);
      try {
        Vertex a2 = graph2.addVertex("A2");
        a2.setProperty("prop2", "value2");
        a2.setProperty("prop", a.getProperty("prop"));
        Vertex b2 = graph2.addVertex("B2");

        try{
          graph2.addEdge("E", a, b, "edgey2");
          fail("exception should be thrown since nodes a and b are not in graph2");
        } catch (RuntimeException re) {
        }
        
        graph2.addEdge("E", a2, b2, "edgey2");
        GraphTransaction.commit(tx);
      } catch (RuntimeException re) {
        GraphTransaction.rollback(tx);
        throw re;
      }
    } catch (RuntimeException re) {
      GraphTransaction.rollback(tx);
      throw re;
    }

    assertEquals(3, Iterables.size(graph.getVertices()));
    assertEquals(1, Iterables.size(graph.getEdges()));
    assertEquals(3, Iterables.size(graph2.getVertices()));
    assertEquals(1, Iterables.size(graph2.getEdges()));
    assertEquals("edgey", graph.getEdge("E").getLabel());
    assertEquals("edgey2", graph2.getEdge("E").getLabel());
    assertEquals("value", graph.getVertex("A").getProperty("prop"));
    assertEquals("value2", graph2.getVertex("A2").getProperty("prop2"));
    assertEquals("value",  graph2.getVertex("A2").getProperty("prop"));
    
//    System.out.println(GraphUtils.toString(graph));
//    System.out.println(GraphUtils.toString(graph2));
    graph.shutdown();
    graph2.shutdown();
  }

  @Test
  public void testUriReuse() throws IOException {
    //FileUtils.deleteDirectory(new File("target/myDb2")); // make sure graph doesn't exist

    GraphUri gUri = new GraphUri("neo4j:./target/myDb2?fileType=graphml");
    IdGraph<Neo4jGraph> graph = gUri.openIdGraph();

    try {
      gUri.openIdGraph(); // should not be able to connect to graph simultaneously
      Assert.fail();
    } catch (RuntimeException re) {
    }
    graph.shutdown();
  }

}
