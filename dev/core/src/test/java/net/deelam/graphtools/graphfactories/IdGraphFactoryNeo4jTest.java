package net.deelam.graphtools.graphfactories;

import static org.junit.Assert.*;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

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
    GraphTransaction.checkTransactionsClosed();
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
  public void testNeo4jAbsoluteFileUri() throws IOException {
    GraphUri gUri = new GraphUri("neo4j:file:/tmp/target/myDb");
    gUri.delete();
    IdGraph<Neo4jGraph> graph = gUri.openIdGraph(Neo4jGraph.class);
    graph.shutdown();
  }

  @Test
  public void testNeo4jUsingConf() throws IOException {
    BaseConfiguration conf = new BaseConfiguration();
    conf.setProperty("node_auto_indexing", "false"); // TODO: insert neo4j setting that can be checked

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

      int tx2 = GraphTransaction.begin(graph2);
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

    GraphUri gUri = new GraphUri("neo4j:./target/myDb2?fileType=graphml");
    IdGraph<Neo4jGraph> graph = gUri.openIdGraph();

    try {
      gUri.openIdGraph(); // should not be able to connect to graph simultaneously
      Assert.fail();
    } catch (RuntimeException re) {
    }
    graph.shutdown();
  }

  
  @Test
  public void testNeo4jStorage() throws IOException {
    GraphUri gUri = new GraphUri("neo4j:./target/myDb");
    IdGraph<Neo4jGraph> graph = gUri.createNewIdGraph(true);
    Vertex one = graph.addVertex("ONE");
    
    one.setProperty("int", 1);
    assertEquals(Integer.class, one.getProperty("int").getClass());

    short s=23333;
    one.setProperty("short", s);
    assertEquals(Short.class, one.getProperty("short").getClass());

    one.setProperty("bool", true);
    assertEquals(Boolean.class, one.getProperty("bool").getClass());

    byte b=127;
    one.setProperty("byte", b);
    assertEquals(Byte.class, one.getProperty("byte").getClass());

    byte[] ba={ 5 };
    one.setProperty("byte[]", ba);
    assertEquals(ArrayList.class, one.getProperty("byte[]").getClass());
    assertEquals(1, ((List)one.getProperty("byte[]")).size());

    String[] sa={ "asdf" };
    System.out.println(sa);
    one.setProperty("s[]", sa);
    assertEquals(ArrayList.class, one.getProperty("s[]").getClass());
    assertEquals(1, ((List)one.getProperty("s[]")).size());

    char[] ca={ 'a' };
    System.out.println(ca);
    one.setProperty("c[]", ca);
    assertEquals(ArrayList.class, one.getProperty("c[]").getClass());
    assertEquals(1, ((List)one.getProperty("s[]")).size());

    List<String> al=new ArrayList();
    al.add("ok");
    al.add("123");
    assertFalse(al.getClass().isArray());
    one.setProperty("al", al); // Blueprints' Neo4jElement.tryConvertCollectionToArray()
    assertEquals(ArrayList.class, one.getProperty("al").getClass());
    assertEquals(2, ((List)one.getProperty("al")).size());
    assertEquals(String.class, ((List)one.getProperty("al")).get(0).getClass());

    String[] arr = al.toArray(new String[al.size()]);
    one.setProperty("arr", arr); // Blueprints' Neo4jElement.tryConvertCollectionToArray()
    assertEquals(ArrayList.class, one.getProperty("arr").getClass());
    assertEquals(2, ((List)one.getProperty("arr")).size());
    assertEquals(String.class, ((List)one.getProperty("arr")).get(0).getClass());
    
    one.setProperty("emptyArr", new String[]{});
    assertEquals(ArrayList.class, one.getProperty("emptyArr").getClass());
    assertEquals(0, ((List)one.getProperty("emptyArr")).size());

    try{
	    List<String> emptyAl=new ArrayList();
	    one.setProperty("emptyAl", emptyAl); // Blueprints' Neo4jElement.tryConvertCollectionToArray() returns null for empty arrays
	    fail();
    }catch(IllegalArgumentException e){    	
    }

    Object[] objArr=new Object[]{"asdf"};
    assertEquals(Object.class, objArr.getClass().getComponentType());
    
    Object[] dynArr=(Object[]) Array.newInstance(String.class, 2);
    assertEquals(String.class, dynArr.getClass().getComponentType());
    
    graph.shutdown();
  }
}
