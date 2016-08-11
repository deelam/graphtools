/**
 * 
 */
package net.deelam.graphtools.importer;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

import net.deelam.graphtools.GraphRecordImpl;
import net.deelam.graphtools.GraphUri;
import net.deelam.graphtools.JavaSetPropertyMerger;
import net.deelam.graphtools.JsonPropertyMerger;
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

import edu.utexas.arlut.adidis.domain.DomainConstants;
import edu.utexas.arlut.adidis.domain.DomainConstants.PropKeysIds;

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

    mgr.register("companyContactsCsv",
        new CsvBeanSourceDataFactory<CompanyContactBean>(()->new CompanyContactsCsvParser()),
        new ImporterFactory() {
          @Override
          public Importer<CompanyContactBean> create(SourceData sd) {
            DefaultImporter<CompanyContactBean> importer = new DefaultImporter<CompanyContactBean>(
                new CompanyContactsEncoder(),
                new DefaultPopulator("telephoneCsv", new DefaultGraphRecordMerger(new JsonPropertyMerger())),
                new GraphRecordImpl.Factory());
            importer.setCommitThreshold(Integer.MAX_VALUE);
            return importer;
          }
        });

    mgr.register("companyContactsCsvBuffered",
        new CsvBeanSourceDataFactory<CompanyContactBean>(()->new CompanyContactsCsvParser()),
        new ImporterFactory() {
          @Override
          public Importer<CompanyContactBean> create(SourceData sd) {
            BufferedImporter<CompanyContactBean> importer = new BufferedImporter<CompanyContactBean>(
                new CompanyContactsEncoder(),
                new DefaultPopulator("telephoneCsv", new DefaultGraphRecordMerger(new JsonPropertyMerger())),
                new GraphRecordImpl.Factory());
            importer.setBufferThreshold(Integer.MAX_VALUE);
            return importer;
          }
        });

    mgr.register("companyContactsCsvConsolidating",
        new CsvBeanSourceDataFactory<CompanyContactBean>(()->new CompanyContactsCsvParser()),
        new ImporterFactory() {
          @Override
          public Importer<CompanyContactBean> create(SourceData sd) {
            ConsolidatingImporter<CompanyContactBean> importer = new ConsolidatingImporter<CompanyContactBean>(
                new CompanyContactsEncoder(),
                new DefaultPopulator("telephoneCsv", new DefaultGraphRecordMerger(new JsonPropertyMerger())),
                new GraphRecordImpl.Factory());
            importer.setBufferThreshold(100000000);
            return importer;
          }
        });

  }

  File csvFile = new File(getClass().getResource("/us-50000.csv").getFile());
  StopWatch sw = new StopWatch();

  @Test
  public void tinkerImportTest() throws IOException, URISyntaxException {
    /// Tinker    
    sw.reset();
    sw.start();
    {
      GraphUri graphUri = new GraphUri("tinker:./target/us500test?fileType=graphml");
      mgr.importFile("companyContactsCsv", csvFile, graphUri);
    }
    System.err.println("Tinker graphml: " + sw);

    sw.reset();
    sw.start();
    {
      IdGraph<?> graph = new GraphUri("tinker:./target/us500test?fileType=graphml")
          .createNewIdGraph(true);
      //      mgr.importFile("companyContactsCsvConsolidating", csvFile, graph);
      graph.shutdown();
    }
    System.err.println("Tinker graphml Consolidating: " + sw);
  }

  @Test
  public void orientImportTest() throws IOException, URISyntaxException {
    /// Orient
    sw.reset();
    final GraphUri graphUri = new GraphUri("orientdb:plocal:./target/orient-us500test");
    if (!false) {
      IdGraph<?> graph = graphUri.createNewIdGraph(true);
      OrientGraph oGraph = ((OrientGraph) graph.getBaseGraph());
      oGraph.createEdgeType("hasDevice");
      oGraph.createEdgeType("inState");
      oGraph.createEdgeType("employeeAt");
      oGraph.shutdown();
      sw.start();
      mgr.importFile("companyContactsCsvConsolidating", csvFile, graphUri);
    }
    System.err.println("OrientDB Consolidating: " + sw);

    sw.reset();
    if (!false) {
      FileUtils.deleteDirectory(new File("./target/orientNoTx-us500test"));
      OrientGraphFactory factory = new OrientGraphFactory("plocal:./target/orientNoTx-us500test", "admin", "admin");
      factory.declareIntent(new OIntentMassiveInsert());
      OrientGraphNoTx noGraph = factory.getNoTx();
      noGraph.createEdgeType("hasDevice");
      noGraph.createEdgeType("inState");
      noGraph.createEdgeType("employeeAt");
      noGraph.shutdown();

      IdGraph<?> graph = new IdGraph<>(noGraph);
      sw.start();
      mgr.importFile("companyContactsCsv", csvFile, graphUri);
    }
    System.err.println("OrientDB NoTx: " + sw);

    sw.reset();
    {
      IdGraph<?> graph = graphUri.createNewIdGraph(true);
      OrientGraph oGraph = ((OrientGraph) graph.getBaseGraph());
      //oGraph.getRawGraph().declareIntent(new OIntentMassiveInsert());
      oGraph.createEdgeType("hasDevice");
      oGraph.createEdgeType("inState");
      oGraph.createEdgeType("employeeAt");
      oGraph.shutdown();

      sw.start();
      mgr.importFile("companyContactsCsv", csvFile, graphUri);
    }
    System.err.println("OrientDB: " + sw);
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
      GraphUri graphUri = new GraphUri("neo4j:./target/neo-us500test");
      sw.start();
      graphUri.setOpenHook(g->{
        //graphUri.createIndices(DomainConstants.getPropertyKeys(PropKeysIds.INGEST));
      });
      mgr.importFile("companyContactsCsv", csvFile, graphUri);
    }
    System.err.println("Neo4j: " + sw);

    sw.reset();
    {
      IdGraph<?> graph = new GraphUri("neo4j:./target/neo-us500test")
          .createNewIdGraph(true);
      sw.start();
      //      mgr.importFile("companyContactsCsvConsolidating", csvFile, graph);
      graph.shutdown();
    }
    System.err.println("Neo4j Consolidating: " + sw);
  }
}
