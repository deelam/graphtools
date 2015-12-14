package net.deelam.graphtools.importer;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

import net.deelam.graphtools.GraphUri;
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
        new CsvBeanSourceDataFactory<CompanyContactBean>(new CompanyContactsCsvParser()),
        new DefaultImporter<CompanyContactBean>(
            new CompanyContactsEncoder(),
            new DefaultPopulator("telephoneCsv", new DefaultGraphRecordMerger(new JavaSetPropertyMerger()))
        ));


    ConsolidatingImporter<CompanyContactBean> importer3 =
        new ConsolidatingImporter<CompanyContactBean>(
            new CompanyContactsEncoder(),
            new DefaultPopulator("telephoneCsv", new DefaultGraphRecordMerger(new JavaSetPropertyMerger()))
            );

    mgr.register("companyContactsCsvConsolidating", new CsvBeanSourceDataFactory<CompanyContactBean>(
        new CompanyContactsCsvParser()), importer3);
    importer3.setBufferThreshold(100000000);
  }

  @Test
  public void importCsvFileTest() throws IOException, URISyntaxException {
    File csvFile = new File(getClass().getResource("/us-500.csv").getFile());

    // create new graph
    IdGraph<?> graph = new GraphUri("tinker:///./target/us500test?fileType=graphml")
        .createNewIdGraph(true);
    mgr.importFile("companyContactsCsv", csvFile, graph);

    // close graph
    graph.shutdown();
  }

  @Test
  public void orientImportTest() throws Exception {
    StopWatch sw = new StopWatch();

    IdGraph<?> graph = new GraphUri("orientdb:plocal:./target/orient-us500test")
        .createNewIdGraph(true);
    OrientGraph oGraph = ((OrientGraph) graph.getBaseGraph());
    oGraph.createEdgeType("hasDevice");
    oGraph.createEdgeType("inState");
    oGraph.createEdgeType("employeeAt");
    sw.start();
    File csvFile = new File(getClass().getResource("/us-500.csv").getFile());
    mgr.importFile("companyContactsCsvConsolidating", csvFile, graph);

    //GraphExporter.exportGraphml(graph, "orient-us500test.graphml", true);

    graph.shutdown();
  }
}
