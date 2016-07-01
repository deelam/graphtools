package net.deelam.graphtools.importer;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.deelam.graphtools.GraphUri;

import com.google.common.base.Preconditions;

/**
 * Thread-safe
 * @author deelam
 */
@Slf4j
public class ImporterManager {
  
  @AllArgsConstructor
  private static class Factories {
    SourceDataFactory sourceDataFactory;

    ImporterFactory importerFactory;
  }
  
  private Map<String, Factories> registry = new HashMap<>();

  public Factories register(String id, SourceDataFactory sdFactory, ImporterFactory importerFactory) {
    return registry.put(id, new Factories(sdFactory, importerFactory));
  }

  public Set<String> getIngesterList() {
    return registry.keySet();
  }

  private Map<File,SourceData> openSourceDatas=new HashMap<>();
  
  public int getPercentProcessed(File currFile){
    SourceData sData = openSourceDatas.get(currFile);
    if(sData==null){
      //log.warn("No SourceData found for file={}", currFile);
      return -1; // FIXME: either done or not started
    }
    return sData.getPercentProcessed();
  }
  
  @SuppressWarnings({"unchecked"})
  public void importFile(String ingesterId, File file, GraphUri graphUri) throws IOException {
    // get ingester
    Factories factories = registry.get(ingesterId);
    Preconditions.checkNotNull(factories, "importerFactory not registered: " + ingesterId);
    
    SourceData sData = factories.sourceDataFactory.createFrom(file);
    if(openSourceDatas.put(file, sData)!=null)
      log.warn("Overrode sourceData for file={}", file);
    final ImporterFactory importerF = factories.importerFactory;
    Importer importer=importerF.create();
    
//    log.info("{}  {}", sData.toString(), importer);
    importData(sData, importer, graphUri);
    sData=null;
    openSourceDatas.remove(file);
  }

  @SuppressWarnings({"unchecked"})
  public void importReadable(String ingesterId, Readable readable, GraphUri graphUri) throws IOException {
    // get ingester
    Factories factories = registry.get(ingesterId);
    Preconditions.checkNotNull(factories, "importerFactory not registered: " + ingesterId);
    
    SourceData sData = factories.sourceDataFactory.createFrom(readable);
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
