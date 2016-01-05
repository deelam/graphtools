package net.deelam;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

import net.deelam.graphtools.GraphUri;
import net.deelam.graphtools.JsonPropertyMerger;
import net.deelam.graphtools.graphfactories.IdGraphFactoryNeo4j;
import net.deelam.graphtools.graphfactories.IdGraphFactoryTinker;
import net.deelam.graphtools.importer.ConsolidatingImporter;
import net.deelam.graphtools.importer.DefaultGraphRecordMerger;
import net.deelam.graphtools.importer.DefaultPopulator;
import net.deelam.graphtools.importer.ImporterManager;
import net.deelam.graphtools.importer.csv.CsvBeanSourceDataFactory;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.tinkerpop.blueprints.util.io.graphml.GraphMLWriter;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

/**
 * @author deelam
 */
public class ImportClassDepCsv {
  
  public static void main(String[] args) throws IOException, URISyntaxException {
    ImportClassDepCsv me = new ImportClassDepCsv();
    File csvFile = new File(args[0]);
    if(!true){
    //File csvFile = new File(me.getResource("/classdep.csv").getFile());
    me.importCsvFile(csvFile);
    }
    
    GraphUri srcGraphUri=new GraphUri("neo4j:./neo-"+csvFile);
    GraphUri facetGraphUri=new GraphUri("neo4j:./neo-facet"+csvFile);
    facetSvc.createFacet(IdentityFaceter.facetName, srcGraphUri, facetGraphUri);

    IdGraph graph = facetGraphUri.openExistingIdGraph();
    GraphMLWriter.outputGraph(graph, csvFile+"-facet.graphml");
    facetGraphUri.shutdown();

  }

  static ImporterManager mgr = new ImporterManager();
  static FacetingService facetSvc;
  static{
    IdGraphFactoryTinker.register();
    IdGraphFactoryNeo4j.register();

    ConsolidatingImporter<ClassDepBean> importer3 =
        new ConsolidatingImporter<ClassDepBean>(
            new ClassDepEncoder(),
            new DefaultPopulator("classDepCsv", new DefaultGraphRecordMerger(new JsonPropertyMerger()))
            );

    mgr.register("ClassDepCsv", new CsvBeanSourceDataFactory<ClassDepBean>(
        new ClassDepCsvParser()), importer3);
    importer3.setBufferThreshold(100000000);
    
    //
    
    Injector injector=Guice.createInjector(new AbstractModule(){
      @Override
      protected void configure() {
        
      }
    });
    
    facetSvc = injector.getInstance(FacetingService.class);
    facetSvc.registerFaceter(injector.getInstance(IdentityFaceter.class));
  }

  public void importCsvFile(File csvFile) throws IOException, URISyntaxException {
    // create new graph
    GraphUri gUri = new GraphUri("neo4j:./neo-"+csvFile);
     IdGraph<?> graph = gUri.createNewIdGraph(true);
    mgr.importFile("ClassDepCsv", csvFile, graph);

    GraphMLWriter.outputGraph(graph, csvFile+".graphml");
    
    // close graph
    gUri.shutdown();
  }

}
