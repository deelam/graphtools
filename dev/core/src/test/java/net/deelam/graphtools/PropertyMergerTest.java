package net.deelam.graphtools;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import net.deelam.graphtools.graphfactories.IdGraphFactoryTinker;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.tg.TinkerGraph;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

/**
 * @author dnlam, Created:Jan 20, 2016
 */
public class PropertyMergerTest {

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    IdGraphFactoryTinker.register();
  }

  GraphUri gUri;
  IdGraph<TinkerGraph> graph;
  @Before
  public void setUp() throws Exception {
    gUri = new GraphUri("tinker:/");
    graph = gUri.createNewIdGraph(true);
  }

  @After
  public void tearDown() throws Exception {
    gUri.shutdown();
  }

  @Test
  public void testJavaSetPM() throws IOException {
    JavaSetPropertyMerger pm=new JavaSetPropertyMerger();
    testPropMerger(pm, JavaSetPropertyMerger.SET_VALUE, JavaSetPropertyMerger.SET_SUFFIX, 0);
    
    Element toE2=graph.getVertex("toV2");
    assertEquals(2, ((Set) toE2.getProperty("prop"+pm.SET_SUFFIX)).size());
    
    Element toE=graph.getVertex("toV");
    assertEquals(2, ((Set) toE.getProperty("prop"+pm.SET_SUFFIX)).size());
  }

  @Test
  public void testJsonPM() throws IOException {
    PropertyMerger pm=new JsonPropertyMerger();
    testPropMerger(pm, JsonPropertyMerger.SET_VALUE, JsonPropertyMerger.SET_SUFFIX, 1);
  }
  
  private void testPropMerger(PropertyMerger pm, String SET_VALUE, String SET_SUFFIX, int extraProps) throws IOException {
    Vertex fromE=graph.addVertex("fromV");
    fromE.setProperty("prop", "1");
    Element toE=graph.addVertex("toV");
    pm.mergeProperties(fromE, toE);
    
    assertEquals(1, toE.getPropertyKeys().size());
    assertEquals("1", toE.getProperty("prop"));
    
    Element toE2=graph.addVertex("toV2");
    pm.mergeProperties(toE, toE2);
    
    assertEquals(1, toE2.getPropertyKeys().size());
    assertEquals("1", toE2.getProperty("prop"));
    
    pm.mergeProperties(toE2, fromE);
    assertEquals(1, fromE.getPropertyKeys().size());
    assertEquals("1", fromE.getProperty("prop"));
    
    // val+val -> Set
    
    toE2.setProperty("prop", "2");
    pm.mergeProperties(toE, toE2);
    assertEquals(2+extraProps, toE2.getPropertyKeys().size());
    assertEquals(SET_VALUE, toE2.getProperty("prop"));
    assertEquals(2, pm.getArrayProperty(toE2,"prop").length);
    assertEquals(2, pm.getArrayPropertySize(toE2,"prop"));

    // Set+val -> Set
    
    pm.mergeProperties(toE2, toE);
    assertEquals(2+extraProps, toE.getPropertyKeys().size());
    assertEquals(SET_VALUE, toE.getProperty("prop"));
    
    // Set+Set -> Set
    
    pm.mergeProperties(toE2, toE);
    assertEquals(2+extraProps, toE.getPropertyKeys().size());
    assertEquals(SET_VALUE, toE.getProperty("prop"));

    // test duplicate value doesn't change valueset
    toE2.setProperty("prop", "2");
    assertEquals(2+extraProps, toE2.getPropertyKeys().size());
    assertEquals(2, pm.getArrayProperty(toE2,"prop").length);
    assertEquals(2, pm.getArrayPropertySize(toE2,"prop"));
  }

}
