package net.deelam.enricher.indexing;

import java.io.IOException;

import net.deelam.enricher.indexing.domain.LocationIndexer;
import net.deelam.enricher.indexing.domain.PersonIndexer;
import net.deelam.graphtools.GraphUri;
import net.deelam.graphtools.PrettyPrintXml;
import net.deelam.graphtools.graphfactories.IdGraphFactoryOrientdb;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.store.RAMDirectory;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.tinkerpop.blueprints.util.io.graphml.GraphMLWriter;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

//@Ignore
public class OrientNodeIndexerTest {

  IdGraph<?> graph;

  @BeforeClass
  public static void beforeClass(){
    IdGraphFactoryOrientdb.register();
  }
  
  @Before
  public void setUp() throws Exception {
    GraphUri guri = new GraphUri("orientdb:plocal:./target/test-classes/orient-us500test");
    graph=guri.openExistingIdGraph();
//    GraphMLWriter.outputGraph(graph, "orient-us500test.graphml");
//    PrettyPrintXml.prettyPrint("orient-us500test.graphml", "orient-us500test-pp.graphml");
  }
  
  @After
  public void tearDown() {
    graph.shutdown();
  }

  @Test
  public void test() throws IOException, ConfigurationException, ParseException {
    try(NodeIndexer indexer = new NodeIndexer(new RAMDirectory())){
      indexer.registerEntityIndexer(PersonIndexer.ENTITY_TYPE, new PersonIndexer());
      indexer.registerEntityIndexer(LocationIndexer.ENTITY_TYPE, new LocationIndexer());
      
      indexer.indexGraph(graph, "us500test");
      indexer.indexGraph(graph, "us500test2");
      indexer.list("firstNameSorted", "STRING", "firstName", 20);
      indexer.list("zipSorted", "INT", "zip",20 );
    }
  }

}
