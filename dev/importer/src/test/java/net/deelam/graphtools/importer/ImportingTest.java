package net.deelam.graphtools.importer;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

import net.deelam.graphtools.GraphRecordImpl;
import net.deelam.graphtools.GraphUri;
import net.deelam.graphtools.JavaSetPropertyMerger;
import net.deelam.graphtools.graphfactories.IdGraphFactoryOrientdb;
import net.deelam.graphtools.graphfactories.IdGraphFactoryTinker;
import net.deelam.graphtools.importer.csv.CsvBeanSourceDataFactory;
import net.deelam.graphtools.importer.domain.CompanyContactBean;
import net.deelam.graphtools.importer.domain.CompanyContactsCsvParser;
import net.deelam.graphtools.importer.domain.CompanyContactsEncoder;

import org.apache.commons.lang.time.StopWatch;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

/**
 * @author deelam
 */
public class ImportingTest {

  static ImporterManager mgr = new ImporterManager();

  @BeforeClass
  public static void setup() throws IOException {
    IdGraphFactoryTinker.register();
    IdGraphFactoryOrientdb.register();
    mgr.register("companyContactsCsv",
        new CsvBeanSourceDataFactory<CompanyContactBean>(()->new CompanyContactsCsvParser()),
        new ImporterFactory() {
          @Override
          public Importer<CompanyContactBean> create(SourceData sd) {
            return new DefaultImporter<CompanyContactBean>(
                new CompanyContactsEncoder(),
                new DefaultPopulator("telephoneCsv", new DefaultGraphRecordMerger(new JavaSetPropertyMerger())),
                new GraphRecordImpl.Factory()
            );
          }
        });


    mgr.register("companyContactsCsvConsolidating", new CsvBeanSourceDataFactory<CompanyContactBean>(()->new CompanyContactsCsvParser()), 
        new ImporterFactory() {
          @Override
          public Importer<CompanyContactBean> create(SourceData sd) {
            ConsolidatingImporter<CompanyContactBean> importer = new ConsolidatingImporter<CompanyContactBean>(
                new CompanyContactsEncoder(),
                new DefaultPopulator("telephoneCsv", new DefaultGraphRecordMerger(new JavaSetPropertyMerger())),
                new GraphRecordImpl.Factory()
                );
            importer.setBufferThreshold(100000000);
            return importer;
          }
        });
  }

  @Test
  public void importCsvFileTest() throws IOException, URISyntaxException {
    File csvFile = new File(getClass().getResource("/us-500.csv").getFile());

    // create new graph
    GraphUri graphUri = new GraphUri("tinker:///./target/us500test?fileType=graphml");
    mgr.importFile("companyContactsCsv", csvFile, graphUri);
  }

  //@Test
  public void orientImportTest() throws Exception {
    StopWatch sw = new StopWatch();
    GraphUri graphUri = new GraphUri("orientdb:plocal:./target/orient-us500test");
    IdGraph<?> graph = graphUri.createNewIdGraph(true);
    OrientGraph oGraph = ((OrientGraph) graph.getBaseGraph());
    oGraph.createEdgeType("hasDevice");
    oGraph.createEdgeType("inState");
    oGraph.createEdgeType("employeeAt");
    oGraph.shutdown();

    sw.start();
    File csvFile = new File(getClass().getResource("/us-500.csv").getFile());
    mgr.importFile("companyContactsCsvConsolidating", csvFile, graphUri);

    //GraphExporter.exportGraphml(graph, "orient-us500test.graphml", true);

  }
}
