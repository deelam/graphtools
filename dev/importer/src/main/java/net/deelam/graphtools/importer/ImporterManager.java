package net.deelam.graphtools.importer;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import lombok.AllArgsConstructor;
import net.deelam.graphtools.GraphUri;

import com.google.common.base.Preconditions;

/**
 * 
 * @author deelam
 */
public class ImporterManager {
  
  @AllArgsConstructor
  private static class ImporterFactories {
    SourceDataFactory sourceDataFactory;

    ImporterFactory importerFactory;
  }
  
  private Map<String, ImporterFactories> registry = new HashMap<>();

  public ImporterFactories register(String id, SourceDataFactory sdFactory, ImporterFactory importerFactory) {
    return registry.put(id, new ImporterFactories(sdFactory, importerFactory));
  }

  public Set<String> getIngesterList() {
    return registry.keySet();
  }

  SourceData sData =null;
  public int getPercentProcessed(){
    if(sData==null)
      return -1;
    return sData.getPercentProcessed();
  }
  
  @SuppressWarnings({"unchecked"})
  public void importFile(String ingesterId, File file, GraphUri graphUri) throws IOException {
    // get ingester
    ImporterFactories factories = registry.get(ingesterId);
    Preconditions.checkNotNull(factories, "importerFactory not registered: " + ingesterId);
    
    sData = factories.sourceDataFactory.createFrom(file);
    final ImporterFactory importerF = factories.importerFactory;
    
    importData(sData, importerF.create(), graphUri);
    sData=null;
  }

  @SuppressWarnings({"unchecked"})
  public void importReadable(String ingesterId, Readable readable, GraphUri graphUri) throws IOException {
    // get ingester
    ImporterFactories factories = registry.get(ingesterId);
    Preconditions.checkNotNull(factories, "importerFactory not registered: " + ingesterId);
    
    sData = factories.sourceDataFactory.createFrom(readable);
    final ImporterFactory importerF = factories.importerFactory;
    
    importData(sData, importerF.create(), graphUri);
    sData=null;
  }
  
  public <B> void importData(SourceData<B> sData, final Importer<B> importer, GraphUri graphUri)
      throws IOException {
    Preconditions.checkNotNull(graphUri);

    // apply ingester on graph
    importer.importFile(sData, graphUri);
  }
}
