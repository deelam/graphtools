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
      importer1.setCommitThreshold(10000);
      importer2.setBufferThreshold(10000);
      importer3.setBufferThreshold(10000);
    }

  }

  @Test
  public void importCsvFileTest() throws IOException, URISyntaxException {
    File csvFile = new File(getClass().getResource("/us-50000.csv").getFile());

    StopWatch sw=new StopWatch();
    
    /// Tinker
    
    sw.start();
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

    /// Orient
    
    {
    IdGraph<?> graph = new GraphUri("orientdb:plocal:./target/orient-us500test")
    .createNewIdGraph(true);
    graph.shutdown();
    }
    
    sw.reset();
    {
      IdGraph<?> graph = new GraphUri("orientdb:plocal:./target/orient-us500test")
      .createNewIdGraph(true);
      sw.start();
//      mgr.importFile("companyContactsCsvConsolidating", csvFile, graph);
      graph.shutdown();
    }
    System.err.println("OrientDB graphml Consolidating: "+sw);
  
    sw.reset();
    {
      IdGraph<?> graph = new GraphUri("orientdb:plocal:./target/orient-us500test")
        .createNewIdGraph(true);
      sw.start();
      mgr.importFile("companyContactsCsv", csvFile, graph);
      graph.shutdown();
    }
    System.err.println("OrientDB graphml: "+sw);

    /// Neo4j
    
    sw.reset();
    {
      IdGraph<?> graph = new GraphUri("neo4j:./target/neo-us500test")
        .createNewIdGraph(true);
      sw.start();
      mgr.importFile("companyContactsCsv", csvFile, graph);
      graph.shutdown();
    }
    System.err.println("Neo4j graphml: "+sw);

    sw.reset();
    {
      IdGraph<?> graph = new GraphUri("neo4j:./target/neo-us500test")
      .createNewIdGraph(true);
      sw.start();
//      mgr.importFile("companyContactsCsvConsolidating", csvFile, graph);
      graph.shutdown();
    }
    System.err.println("Neo4j graphml Consolidating: "+sw);
  }
}
