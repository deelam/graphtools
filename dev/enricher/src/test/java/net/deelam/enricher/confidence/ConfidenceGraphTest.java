package net.deelam.enricher.confidence;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

import net.deelam.graphtools.GraphUri;
import net.deelam.graphtools.GraphUtils;
import net.deelam.graphtools.graphfactories.IdGraphFactoryTinker;

public class ConfidenceGraphTest {

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    IdGraphFactoryTinker.register();
  }

  GraphUri guri;
  @Before
  public void setUp() throws Exception {
    guri=new GraphUri("tinker:/");
  }

  @After
  public void tearDown() throws Exception {
    guri.shutdown();
  }

  @Test
  public void testDatasourceConfid() {
    IdGraph<?> graph=guri.openIdGraph();
    ConfidenceGraph cGraph=new ConfidenceGraph(graph, "nodeType", "edgeType");
    cGraph.setDatasourceDefaultConfidence(60);
    
    Vertex v1 = graph.addVertex("one");
    v1.setProperty("weather", "sunny");
    assertEquals(60, cGraph.getConfidence(v1, "weather").intValue());
  }  
  
  @Test
  public void testDefaultPropertyConfid() {
    IdGraph<?> graph=guri.openIdGraph();
    ConfidenceGraph cGraph=new ConfidenceGraph(graph, "nodeType", "edgeType");
    cGraph.setNodeDefaultConfidence("PREDICTION", "weather", 70);
    
    Vertex v1 = graph.addVertex("one");
    v1.setProperty("nodeType", "PREDICTION");
    v1.setProperty("weather", "sunny");
    assertEquals(70, cGraph.getConfidence(v1, "weather").intValue());
  }
  
  @Test
  public void testDefaultNodeTypeConfid() {
    IdGraph<?> graph=guri.openIdGraph();
    ConfidenceGraph cGraph=new ConfidenceGraph(graph, "nodeType", "edgeType");
    cGraph.setNodeDefaultConfidence("PREDICTION", 70);
    
    Vertex v1 = graph.addVertex("one");
    v1.setProperty("nodeType", "PREDICTION");
    v1.setProperty("weather", "sunny");
    assertEquals(70, cGraph.getConfidence(v1, "weather").intValue());
  }
  
  @Test
  public void testAveraging() {
    IdGraph<?> graph=guri.openIdGraph();
    ConfidenceGraph cGraph=new ConfidenceGraph(graph, "nodeType", "edgeType");

    Vertex v1 = graph.addVertex("one");
    v1.setProperty("nodeType", "PREDICTION");
    v1.setProperty("weather", "clear");
    cGraph.setConfidence(v1, "weather", 20);
    
    Vertex v2 = graph.addVertex("two");
    v2.setProperty("nodeType", "PREDICTION");
    v2.setProperty("weather", "sunny");
    cGraph.setConfidence(v2, "weather", 80);
    
    Vertex v3 = graph.addVertex("three");
    v3.setProperty("nodeType", "PREDICTION");
    v3.setProperty("weather", "sunny");
    cGraph.setConfidence(v3, "weather", 80);
    
    cGraph.mergeConfidence(v1, v3, "weather");
    assertEquals(50, cGraph.getConfidence(v3, "weather").intValue());
    cGraph.mergeConfidence(v2, v3, "weather");
    assertEquals(60, cGraph.getConfidence(v3, "weather").intValue());
    
    cGraph.mergeConfidence(v2, v3, "weather");
    assertEquals((20+80+80+80)/4, cGraph.getConfidence(v3, "weather").intValue());
    
    //System.out.println(v2.getProperty("weather"));
//    System.out.println(cGraph.getConfidence(v3, "weather"));
    
//    System.out.println(GraphUtils.toString(graph));
  }

  @Test
  public void testMinConfidenceMerger() {
    IdGraph<?> graph=guri.openIdGraph();
    ConfidenceGraph cGraph=new ConfidenceGraph(graph, "nodeType", "edgeType");
    cGraph.setConfMerger(new MinConfidenceMerger(cGraph));

    Vertex v1 = graph.addVertex("one");
    v1.setProperty("nodeType", "PREDICTION");
    v1.setProperty("weather", "clear");
    cGraph.setConfidence(v1, "weather", 20);
    
    Vertex v2 = graph.addVertex("two");
    v2.setProperty("nodeType", "PREDICTION");
    v2.setProperty("weather", "sunny");
    cGraph.setConfidence(v2, "weather", 80);
    
    Vertex v3 = graph.addVertex("three");
    v3.setProperty("nodeType", "PREDICTION");
    v3.setProperty("weather", "sunny");
    cGraph.setConfidence(v3, "weather", 80);
    
    cGraph.mergeConfidence(v1, v3, "weather");
    assertEquals(20, cGraph.getConfidence(v3, "weather").intValue());
    cGraph.mergeConfidence(v2, v3, "weather");
    assertEquals(20, cGraph.getConfidence(v3, "weather").intValue());
  }
  
  
  @Test
  public void testMaxConfidenceMerger() {
    IdGraph<?> graph=guri.openIdGraph();
    ConfidenceGraph cGraph=new ConfidenceGraph(graph, "nodeType", "edgeType");
    cGraph.setConfMerger(new MaxConfidenceMerger(cGraph));

    Vertex v1 = graph.addVertex("one");
    v1.setProperty("nodeType", "PREDICTION");
    v1.setProperty("weather", "clear");
    cGraph.setConfidence(v1, "weather", 20);
    
    Vertex v2 = graph.addVertex("two");
    v2.setProperty("nodeType", "PREDICTION");
    v2.setProperty("weather", "sunny");
    cGraph.setConfidence(v2, "weather", 90);
    
    Vertex v3 = graph.addVertex("three");
    v3.setProperty("nodeType", "PREDICTION");
    v3.setProperty("weather", "sunny");
    cGraph.setConfidence(v3, "weather", 80);
    
    cGraph.mergeConfidence(v1, v3, "weather");
    assertEquals(80, cGraph.getConfidence(v3, "weather").intValue());
    cGraph.mergeConfidence(v2, v3, "weather");
    assertEquals(90, cGraph.getConfidence(v3, "weather").intValue());
  }
}
