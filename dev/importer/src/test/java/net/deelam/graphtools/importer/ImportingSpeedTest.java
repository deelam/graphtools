/**
 * 
 */
package net.deelam.graphtools.importer;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

import net.deelam.graphtools.GraphUri;
import net.deelam.graphtools.graphfactories.IdGraphFactoryNeo4j;
import net.deelam.graphtools.graphfactories.IdGraphFactoryOrientdb;
import net.deelam.graphtools.graphfactories.IdGraphFactoryTinker;
import net.deelam.graphtools.importer.csv.CsvBeanSourceDataFactory;
import net.deelam.graphtools.importer.domain.CompanyContactBean;
import net.deelam.graphtools.importer.domain.CompanyContactsCsvParser;
import net.deelam.graphtools.importer.domain.CompanyContactsEncoder;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.time.StopWatch;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

/**
 * @author deelam
 */
@Ignore
public class ImportingSpeedTest {

  static ImporterManager mgr = new ImporterManager();

  @BeforeClass
  public static void setup() throws IOException {
    IdGraphFactoryTinker.register();
    IdGraphFactoryNeo4j.register();
    IdGraphFactoryOrientdb.register();
    
    DefaultImporter<CompanyContactBean> importer1 = new DefaultImporter<CompanyContactBean>(
      new CompanyContactsEncoder(), new DefaultPopulator("telephoneCsv"));
    mgr.register("companyContactsCsv", new CsvBeanSourceDataFactory<CompanyContactBean>(
        new CompanyContactsCsvParser()), importer1);
    
    BufferedImporter<CompanyContactBean> importer2 = new BufferedImporter<CompanyContactBean>(
      new CompanyContactsEncoder(), new DefaultPopulator("telephoneCsv"));
    mgr.register("companyContactsCsvBuffered", new CsvBeanSourceDataFactory<CompanyContactBean>(
        new CompanyContactsCsvParser()), importer2);

    ConsolidatingImporter<CompanyContactBean> importer3 = new ConsolidatingImporter<CompanyContactBean>(
      new CompanyContactsEncoder(), new DefaultGraphRecordMerger(), new DefaultPopulator("telephoneCsv"));
    mgr.register("companyContactsCsvConsolidating", new CsvBeanSourceDataFactory<CompanyContactBean>(
        new CompanyContactsCsvParser()), importer3);

    if(!false){
      importer1.setCommitThreshold(Integer.MAX_VALUE);
      importer2.setBufferThreshold(Integer.MAX_VALUE);
      importer3.setBufferThreshold(100000000);
    }

  }

  File csvFile = new File(getClass().getResource("/us-50000.csv").getFile());
  StopWatch sw=new StopWatch();
  
  @Test
  public void tinkerImportTest() throws IOException, URISyntaxException {
    /// Tinker    
    sw.reset();sw.start();
    {
      IdGraph<?> graph = new GraphUri("tinker:./target/us500test?fileType=graphml")
        .createNewIdGraph(true);
      mgr.importFile("companyContactsCsv", csvFile, graph);
      graph.shutdown();
    }
    System.err.println("Tinker graphml: "+sw);

    sw.reset();sw.start();
    {
      IdGraph<?> graph = new GraphUri("tinker:./target/us500test?fileType=graphml")
      .createNewIdGraph(true);
//      mgr.importFile("companyContactsCsvConsolidating", csvFile, graph);
      graph.shutdown();
    }
    System.err.println("Tinker graphml Consolidating: "+sw);
  }
  
  @Test
  public void orientImportTest() throws IOException, URISyntaxException {
    /// Orient
    sw.reset();
    if(!false){
      IdGraph<?> graph = new GraphUri("orientdb:plocal:./target/orient-us500test")
      .createNewIdGraph(true);
      OrientGraph oGraph = ((OrientGraph) graph.getBaseGraph());
      oGraph.createEdgeType("hasDevice");
      oGraph.createEdgeType("inState");
      oGraph.createEdgeType("employeeAt");
      sw.start();
      mgr.importFile("companyContactsCsvConsolidating", csvFile, graph);
      graph.shutdown();
    }
    System.err.println("OrientDB Consolidating: "+sw);
  
    sw.reset();
    if(!false){
      FileUtils.deleteDirectory(new File("./target/orientNoTx-us500test"));
      OrientGraphFactory factory = new OrientGraphFactory("plocal:./target/orientNoTx-us500test", "admin", "admin");
      factory.declareIntent(new OIntentMassiveInsert());
      OrientGraphNoTx noGraph = factory.getNoTx();
      noGraph.createEdgeType("hasDevice");
      noGraph.createEdgeType("inState");
      noGraph.createEdgeType("employeeAt");

      IdGraph<?> graph = new IdGraph<>(noGraph);
      sw.start();
      mgr.importFile("companyContactsCsv", csvFile, graph);
      graph.shutdown();
    }
    System.err.println("OrientDB NoTx: "+sw);
    
    sw.reset();
    {
      IdGraph<?> graph = new GraphUri("orientdb:plocal:./target/orient-us500test")
        .createNewIdGraph(true);
      OrientGraph oGraph = ((OrientGraph) graph.getBaseGraph());
      //oGraph.getRawGraph().declareIntent(new OIntentMassiveInsert());
      oGraph.createEdgeType("hasDevice");
      oGraph.createEdgeType("inState");
      oGraph.createEdgeType("employeeAt");

      sw.start();
      mgr.importFile("companyContactsCsv", csvFile, graph);
      graph.shutdown();
    }
    System.err.println("OrientDB: "+sw);
  }

  @Test
  public void createEdgeTypeBug() throws IOException, URISyntaxException {
    // Submitted possible bug at https://github.com/orientechnologies/orientdb/issues/5317
    FileUtils.deleteDirectory(new File("./target/orient-us500test2"));
    OrientGraph graph = new OrientGraph("plocal:./target/orient-us500test2");
    graph.createVertexType("hasDevice");
    graph.shutdown(true);
    
    FileUtils.deleteDirectory(new File("./target/orient-us500test2"));
    OrientGraph graph2 = new OrientGraph("plocal:./target/orient-us500test2");
    graph2.createVertexType("hasDevice");
    graph2.shutdown();
  }
  @Test
  public void neo4jImportTest() throws IOException, URISyntaxException {

    /// Neo4j
    
    sw.reset();
    {
      IdGraph<?> graph = new GraphUri("neo4j:./target/neo-us500test")
        .createNewIdGraph(true);
      sw.start();
      mgr.importFile("companyContactsCsv", csvFile, graph);
      graph.shutdown();
    }
    System.err.println("Neo4j: "+sw);

    sw.reset();
    {
      IdGraph<?> graph = new GraphUri("neo4j:./target/neo-us500test")
      .createNewIdGraph(true);
      sw.start();
//      mgr.importFile("companyContactsCsvConsolidating", csvFile, graph);
      graph.shutdown();
    }
    System.err.println("Neo4j Consolidating: "+sw);
  }
}