package net.deelam.graphtools.graphfactories;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import net.deelam.graphtools.GraphTransaction;
import net.deelam.graphtools.GraphUri;
import net.deelam.graphtools.GraphUtils;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Iterables;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

public class IdGraphFactoryOrientdbTest {

  @BeforeClass
  public static void setUp() throws Exception {
    IdGraphFactoryOrientdb.register();
  }

  @Test
  public void testOrientInMemDB() throws IOException {
    GraphUri gUri = new GraphUri("orientdb:memory:myODb");
    IdGraph<OrientGraph> graph = gUri.createNewIdGraph(true);
    graph.shutdown();
  }

  @Test
  public void testOrientAbsoluteLocalDB() throws IOException {
    GraphUri gUri = new GraphUri("orientdb:plocal:/tmp/target/myPLocalODb");
    IdGraph<OrientGraph> graph = gUri.createNewIdGraph(true);
    graph.shutdown();
  }

  @Test
  public void testOrientRelativeLocalDB() {
    GraphUri gUri = new GraphUri("orientdb:plocal:./target/myPLocalODb");
    IdGraph<OrientGraph> graph = gUri.openIdGraph();
    graph.shutdown();
  }

  @Test
  public void testOrientUsingConf() {
    BaseConfiguration conf = new BaseConfiguration();
    conf.setProperty("node_auto_indexing", "false"); // FIXME: insert Orient setting that can be checked

    GraphUri gUri = new GraphUri("orientdb:memory:myODb", conf);
    IdGraph<OrientGraph> graph = gUri.openIdGraph();
    //		OrientGraph neo = graph.getBaseGraph();
    //		AutoIndexer<Node> autoIndexer = neo.getRawGraph().index().getNodeAutoIndexer(); // TODO: check if config took effect
    //		System.out.println(autoIndexer);
    graph.shutdown();
  }

  @Test
  public void testOrientUsingUriQuery() {
    {
      GraphUri gUri = new GraphUri("orientdb:plocal:./target/myODb?username=admin&password=admin");
      IdGraph<OrientGraph> graph = gUri.openIdGraph();
      graph.shutdown();
    }
    {
      GraphUri gUri2 = new GraphUri("orientdb:plocal:./target/myODb?username=admin&password=admin");
      IdGraph<OrientGraph> graph2 = gUri2.openIdGraph();
      graph2.shutdown();
    }
  }

  @Test
  public void testOrientUsingUriQueryDiffPwd() throws IOException {
    {
      GraphUri gUri =
          new GraphUri("orientdb:plocal:./target/myOwnODb");
      IdGraph<OrientGraph> graph = gUri.createNewIdGraph(true);
      graph.shutdown();
    }
    {
      GraphUri gUri2 =
          new GraphUri("orientdb:plocal:./target/myOwnODb?username=reader&password=reader");
      IdGraph<OrientGraph> graph2 = gUri2.openIdGraph();
      graph2.shutdown();
    }
  }

  @Test
  public void testTwoOrientUsingUriQuery() throws IOException {
    GraphUri gUri = new GraphUri("orientdb:plocal:./target/myODb");
    IdGraph<OrientGraph> graph = gUri.createNewIdGraph(true);
    GraphUri gUri2 = new GraphUri("orientdb:plocal:./target/myODb2");
    IdGraph<OrientGraph> graph2 = gUri2.createNewIdGraph(true);

    int tx = GraphTransaction.begin(graph);
    Vertex a=null,b=null;
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

        try {
          graph2.addEdge("E", a, b, "edgey2");
          GraphTransaction.commit(tx);
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

    GraphUri gUri = new GraphUri("orientdb:plocal:./target/myODb2");
    IdGraph<OrientGraph> graph = gUri.openIdGraph();

    try {
      gUri.openIdGraph(); // should be able to connect to graph simultaneously
    } catch (RuntimeException re) {
      Assert.fail();
    }
    graph.shutdown();
  }
  
  @Test
  public void testGraphReuse() throws IOException {
    FileUtils.deleteDirectory(new File("./target/orient-us500test2"));
    OrientGraph graph = new OrientGraph("plocal:./target/orient-us500test2");
    graph.addVertex("A");
    graph.addVertex("A"); // Id is ignored
    
    assertEquals(2, Iterables.size(graph.getVertices()));
    
   // hotfix: https://github.com/orientechnologies/orientdb/issues/5317#event-467569228
    //fix: graph.getRawGraph().getStorage().close(true, false); 
    graph.shutdown();

    FileUtils.deleteDirectory(new File("./target/orient-us500test2"));
    OrientGraph graph2 = new OrientGraph("plocal:./target/orient-us500test2");
    //bug in OrientGraph.shutdown(), change expectation from 2 to 0 when fixed  
    assertEquals(2, Iterables.size(graph2.getVertices()));
    graph2.shutdown();
  }

  @Test
  public void testGraphUriReuse() throws IOException {
    GraphUri gUri = new GraphUri("orientdb:plocal:./target/myODb4");
    IdGraph graph = gUri.createNewIdGraph(true);
    
    Vertex a2 = graph.addVertex("A2");
    a2.setProperty("prop2", "value2");
    Vertex b2 = graph.addVertex("B2");
    graph.addEdge("E", a2, b2, "edgey2");
    assertEquals(3, Iterables.size(graph.getVertices()));
    gUri.shutdown(graph);// OrientGraph.shutdown() is buggy; must use gUri.shutdown(graph) until get hotfix: https://github.com/orientechnologies/orientdb/issues/5317#event-467569228
    
    IdGraph graph2 = gUri.createNewIdGraph(true);
    assertEquals(1, Iterables.size(graph2.getVertices()));
    System.out.println(GraphUtils.toString(graph2));
    gUri.shutdown(graph2);
  }
  
}
