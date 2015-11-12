package net.deelam.graphtools;

import java.io.File;
import java.io.IOException;

import net.deelam.graphtools.GraphUri;

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.tinkerpop.blueprints.impls.tg.TinkerGraph;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

public class GraphUriTest {

  @Before
  public void setUp() throws Exception {
    //		new File("testGraphs").mkdir();
  }

  @AfterClass
  public static void tearDown() throws IOException {
    //		FileUtils.deleteDirectory(new File("testGraphs"));
  }

  @Test
  public void testInMemGraph() {
    GraphUri gUri = new GraphUri("tinker:");
    IdGraph<TinkerGraph> graph = gUri.openIdGraph();
    graph.shutdown();
  }

  @Test
  public void testInMemGraph2() {
    GraphUri gUri = new GraphUri("tinker:/");
    IdGraph<TinkerGraph> graph = gUri.openIdGraph();
    graph.shutdown();
  }
  
  @Test
  public void testSavedGraph0() {
    GraphUri gUri = new GraphUri("tinker:./target/tGraph");
    IdGraph<TinkerGraph> graph = gUri.openIdGraph();
    graph.shutdown();
  }
  
  @Test
  public void testSavedGraph1() {
    GraphUri gUri = new GraphUri("tinker:///./target/tGraph");
    IdGraph<TinkerGraph> graph = gUri.openIdGraph();
    graph.shutdown();
  }

  @Test
  public void testSavedGraph2() {
    GraphUri gUri = new GraphUri("tinker:/tmp/target/tGraph");
    IdGraph<TinkerGraph> graph = gUri.openIdGraph();
    graph.shutdown();
  }
  
  @Test
  public void testSavedGraphml() {
    GraphUri gUri = new GraphUri("tinker:./target/tGraphML?fileType=graphml");
    IdGraph<TinkerGraph> graph = gUri.openIdGraph();
    graph.shutdown();
  }

  @Test
  public void testTwoSavedGraphs() {
    GraphUri gUri = new GraphUri("tinker:./target/tGraphML?fileType=graphml");
    IdGraph<TinkerGraph> graph = gUri.openIdGraph();

    GraphUri gUri2 = new GraphUri("tinker:./target/tGraphML2?fileType=graphml");
    IdGraph<TinkerGraph> graph2 = gUri2.openIdGraph();

    graph.shutdown();
    graph2.shutdown();
  }

  @Test
  public void testUriReuse() throws IOException {
    FileUtils.deleteDirectory(new File("target/tGraphMLReuse")); // make sure graph doesn't exist

    GraphUri gUri = new GraphUri("tinker:///./target/tGraphMLReuse?fileType=graphml");
    IdGraph<TinkerGraph> graph = gUri.openIdGraph();

    try {
      gUri.openIdGraph(); // should not be able to access new graph
      Assert.fail();
    } catch (RuntimeException re) {
    }
    graph.shutdown();
  }
}
