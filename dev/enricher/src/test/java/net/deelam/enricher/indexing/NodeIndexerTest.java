package net.deelam.enricher.indexing;

import java.io.IOException;

import net.deelam.enricher.indexing.domain.LocationIndexer;
import net.deelam.enricher.indexing.domain.PersonIndexer;
import net.deelam.graphtools.GraphUri;
import net.deelam.graphtools.graphfactories.IdGraphFactoryTinker;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.store.RAMDirectory;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

public class NodeIndexerTest {

  IdGraph<?> graph;

  @BeforeClass
  public static void beforeClass(){
    IdGraphFactoryTinker.register();
  }
  
  @Before
  public void setUp() throws Exception {
    GraphUri guri = new GraphUri("tinker:///./target/test-classes/us500test?fileType=graphml");
    graph=guri.openExistingIdGraph();
  }
  
  @After
  public void tearDown() {
    graph.shutdown();
  }

  @Test
  public void test() throws IOException, ConfigurationException, ParseException {
//    FileUtils.deleteDirectory(new File("target/us500index"));
    
    try(NodeIndexer indexer = new NodeIndexer(new RAMDirectory())){
      indexer.registerEntityIndexer(new PersonIndexer());
      indexer.registerEntityIndexer(new LocationIndexer());
      
      indexer.indexGraph(graph, "us500test");
      indexer.indexGraph(graph, "us500test2");
      indexer.listBySortedField("firstNameSorted", "STRING", "firstName", 20);
      indexer.listBySortedField("zipSorted", "INT", "zip",20 );
    }
  }
  
  @Test
  public void testWithIdMapper() throws IOException, ConfigurationException, ParseException {
    
  }  

}
