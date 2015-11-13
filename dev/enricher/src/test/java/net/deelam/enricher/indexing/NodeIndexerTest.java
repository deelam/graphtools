package net.deelam.enricher.indexing;

import java.io.IOException;

import net.deelam.enricher.indexing.domain.LocationIndexer;
import net.deelam.enricher.indexing.domain.PersonIndexer;
import net.deelam.graphtools.GraphUri;
import net.deelam.graphtools.graphfactories.IdGraphFactoryTinker;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.lucene.queryparser.classic.ParseException;
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
    graph=guri.openIdGraph();
  }
  
  @After
  public void tearDown() {
    graph.shutdown();
  }

  @Test
  public void test() throws IOException, ConfigurationException, ParseException {
//    FileUtils.deleteDirectory(new File("target/us500index"));
    
    try(NodeIndexer indexer = new NodeIndexer(null)){
      indexer.registerEntityIndexer(PersonIndexer.ENTITY_TYPE, new PersonIndexer());
      indexer.registerEntityIndexer(LocationIndexer.ENTITY_TYPE, new LocationIndexer());
      
      indexer.indexGraph(graph, "us500test");
      indexer.indexGraph(graph, "us500test2");
      indexer.list("firstNameSorted", "STRING", "firstName", 20);
      indexer.list("zipSorted", "INT", "zip",20 );
    }
  }

}
