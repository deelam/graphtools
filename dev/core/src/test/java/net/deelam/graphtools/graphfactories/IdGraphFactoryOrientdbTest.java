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
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Iterables;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

@Ignore
public class IdGraphFactoryOrientdbTest {

  @BeforeClass
  public static void setUp() throws Exception {
    GraphTransaction.checkTransactionsClosed();
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

  // buggy Orient when working with multiple graphs: non-determinism causing failures @Test
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

      int tx2 = GraphTransaction.begin(graph2);
      try {
        Vertex a2 = graph2.addVertex("A2");
        a2.setProperty("prop2", "value2");
        a2.setProperty("prop", a.getProperty("prop"));
        Vertex b2 = graph2.addVertex("B2");

        graph2.addEdge("E", a, b, "edgey2");
        GraphTransaction.commit(tx2);
        fail("exception should be thrown since nodes a and b are not in graph2");
      } catch (RuntimeException re) {
        GraphTransaction.rollback(tx2); // this should occur
      }

      // retry with correct vertices
      tx2 = GraphTransaction.begin(graph2);
      try {
        Vertex a2, b2;
        if(false){
        // TODO: report that OrientGraph doesn't rollback added nodes
//        a2 = graph2.addVertex("A2");
//        a2.setProperty("prop2", "value2");
//        a2.setProperty("prop", a.getProperty("prop"));
//        b2 = graph2.addVertex("B2");
        }else{
          // workaround: just get the nodes
          a2 = graph2.getVertex("A2");
          b2 = graph2.getVertex("B2");
        }
        
        graph2.addEdge("E", a2, b2, "edgey2");
        GraphTransaction.commit(tx2);

      } catch (RuntimeException re) {
        GraphTransaction.rollback(tx2);
        throw re;
      }
    } catch (RuntimeException re) {
      GraphTransaction.rollback(tx);
      throw re;
    }
    GraphTransaction.checkTransactionsClosed();
    
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
      IdGraph graph2 = new GraphUri("orientdb:plocal:./target/myODb2").openIdGraph(); // should be able to connect to graph simultaneously
//      assertTrue(graph==graph2);
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
    gUri.shutdown();// OrientGraph.shutdown() is buggy; must use gUri.shutdown() until get hotfix: https://github.com/orientechnologies/orientdb/issues/5317#event-467569228
    
    IdGraph graph2 = gUri.createNewIdGraph(true);
    assertEquals(1, Iterables.size(graph2.getVertices()));
    System.out.println(GraphUtils.toString(graph2));
    gUri.shutdown();
  }
  
  //@Test
  public void testCopyGraphToGraph0() throws IOException {
    GraphUri gUri = new GraphUri("orientdb:plocal:./target/myODb5");
    IdGraph graph = gUri.createNewIdGraph(true);
    
    Vertex a2 = graph.addVertex("Paul Salda�a");
    a2.setProperty("prop2", "value2");
    Vertex b2 = graph.addVertex("B2");
    graph.addEdge("E", a2, b2, "edgey2");
    graph.commit();
    assertEquals(3, Iterables.size(graph.getVertices()));
    assertEquals(1, Iterables.size(graph.getEdges()));
    assertEquals(1, Iterables.size(a2.getEdges(Direction.OUT)));
    gUri.shutdown();
    
    IdGraph graph2 = gUri.openExistingIdGraph();
    System.out.println(GraphUtils.toString(graph2));
    assertEquals(3, Iterables.size(graph2.getVertices()));
    assertEquals(1, Iterables.size(graph2.getEdges()));
    assertEquals(1, Iterables.size(graph2.getVertex("Paul Salda�a").getEdges(Direction.OUT)));
    
    GraphUri dgUri = new GraphUri("orientdb:plocal:./target/destODb");
    IdGraph dgraph = dgUri.createNewIdGraph(true);
    Vertex a22 = graph2.getVertex("Paul Salda�a");
    assertEquals(1, Iterables.size(graph2.getVertex("Paul Salda�a").getEdges(Direction.OUT)));
    Vertex b22 = graph2.getVertex("B2");
    Vertex a4=dgraph.addVertex("A4:Paul Salda�a");
    assertEquals(1, Iterables.size(graph2.getVertex("Paul Salda�a").getEdges(Direction.OUT)));
    dgraph.getVertex("Paul Salda�a");
    assertEquals(1, Iterables.size(graph2.getVertex("Paul Salda�a").getEdges(Direction.OUT)));
    System.out.println(GraphUtils.toString(graph2));
    System.out.println(GraphUtils.toString(dgraph));
    dgUri.shutdown();
    
    gUri.shutdown();

  }
  
//  @Test
  public void testOperatingOnTwoGraphs() throws IOException {
    GraphUri gUri = new GraphUri("orientdb:plocal:./target/myODb5");
    gUri.delete();
    IdGraph //graph = gUri.createNewIdGraph(true);
    graph1=new IdGraph(new OrientGraph("plocal:./target/myODb5"));
    Vertex a2 = graph1.addVertex("Paul");
    a2.setProperty("prop2", "value2");
    Vertex b2 = graph1.addVertex("B2");
    graph1.addEdge("E", a2, b2, "edgey2");
    graph1.commit();
//    assertEquals(3, Iterables.size(graph1.getVertices()));
    assertEquals(1, Iterables.size(graph1.getEdges()));
    assertEquals(1, Iterables.size(a2.getEdges(Direction.OUT)));
//    System.out.println(GraphUtils.toString(graph));
    assertEquals(1, Iterables.size(graph1.getVertex("Paul").getEdges(Direction.OUT)));
    
    GraphUri dgUri = new GraphUri("orientdb:plocal:./target/destODb");
    dgUri.delete();
    IdGraph // dgraph = dgUri.createNewIdGraph(true);
    graph2=new IdGraph(new OrientGraph("plocal:./target/destODb"));
    
    Vertex paulNodeFromGraph1 = graph1.getVertex("Paul");
    assertEquals(1, Iterables.size(graph1.getVertex("Paul").getEdges(Direction.OUT)));
//    Vertex b22 = graph.getVertex("B2");
    
    Vertex a4=graph2.addVertex("A4:Paul");
    Vertex a5=graph2.addVertex("A5:B2");
    String edgeId="equiv"+a4.getId()+"->"+a5.getId();
    graph2.addEdge(edgeId, a4, a5, "equivalent");
    System.out.println(GraphUtils.toString(graph2));
    
    assertEquals(1, Iterables.size(graph1.getVertex("Paul").getEdges(Direction.OUT)));
    System.out.println("a22 edges="+Iterables.toString(paulNodeFromGraph1.getEdges(Direction.OUT)));
    assertEquals(1, Iterables.size(paulNodeFromGraph1.getEdges(Direction.OUT)));
    
    assertNotNull(graph2.getVertex("A4:Paul")); // <---- causes a22 to be confused

    System.out.println("a22 edges="+Iterables.toString(paulNodeFromGraph1.getEdges(Direction.OUT))); // <-- NPE here
    assertEquals(1, Iterables.size(paulNodeFromGraph1.getEdges(Direction.OUT)));
    
//    System.out.println("graph= "+GraphUtils.toString(graph));
//    System.out.println("dgraph= "+GraphUtils.toString(dgraph));
    dgUri.shutdown();
    gUri.shutdown();

  }
  
  
  //@Test
  public void testCopyGraphToGraph_OrientGraphOnly() throws IOException {
    // create new source graph
    FileUtils.deleteDirectory(new File("./target/srcGraph"));
    IdGraph srcGraph = new IdGraph(new OrientGraph("plocal:./target/srcGraph"));
    Vertex paul = srcGraph.addVertex("Paul");
    paul.setProperty("myProp", "myValue");
    Vertex bob = srcGraph.addVertex("Bob");
    srcGraph.addEdge("myEdgeId", paul, bob, "myEdgeLabel");
    srcGraph.commit();
    
/*    if(true){ // Check source graph
      //System.out.println(GraphUtils.toString(srcGraph));
      assertEquals(2, Iterables.size(srcGraph.getVertices()));
      assertEquals(1, Iterables.size(srcGraph.getEdges()));
      assertEquals(1, Iterables.size(srcGraph.getVertex("Paul").getEdges(Direction.OUT)));
      assertEquals("myValue", srcGraph.getVertex("Paul").getProperty("myProp"));
      assertEquals(1, paul.getPropertyKeys().size());
    }*/
    
    // open new destination graph
    FileUtils.deleteDirectory(new File("./target/destODb"));
    IdGraph destGraph = new IdGraph(new OrientGraph("plocal:./target/destODb"));
    
    Vertex destPaul=destGraph.addVertex("dest:Paul");
    Vertex destBob=destGraph.addVertex("dest:Bob");
    destGraph.addEdge("destEdgeId", destPaul, destBob, "destEdgeLabel");
    destGraph.commit();
    
/*    boolean checkDstGraph=true;
    if(checkDstGraph){ // Check source graph
      //System.out.println(GraphUtils.toString(destGraph));
      assertEquals(2, Iterables.size(destGraph.getVertices()));
      assertEquals(1, Iterables.size(destGraph.getEdges()));
      assertEquals(1, Iterables.size(destGraph.getVertex("dest:Paul").getEdges(Direction.OUT)));
    }
    */
    
    srcGraph.getVertex("Paul");
    srcGraph.commit();
    destPaul.setProperty("_destPaulProp", "destValue");
    srcGraph.commit();
    destGraph.commit();
    
    boolean makeSubsequentLineWork=!true;
    if (makeSubsequentLineWork){
      srcGraph.getVertex("Paul");
    }
    /* next line causes OrientElement.getProperty to throw 
       NPException if makeSubsequentLineWork=false
     */
    System.out.println("paul edges="+Iterables.toString(paul.getEdges(Direction.OUT)));

    // shutdown graphs
    ((OrientGraph)destGraph.getBaseGraph()).getRawGraph().getStorage().close(true, false); 
    destGraph.shutdown();
    ((OrientGraph)srcGraph.getBaseGraph()).getRawGraph().getStorage().close(true, false); 
    srcGraph.shutdown();
  }
  
}
