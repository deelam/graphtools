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
  
  static SourceData COMPLETED_SD=new SourceData<Object>() {
    @Override
    public Object getNextRecord() throws IOException {
      return null;
    }
    @Override
    public int getPercentProcessed() {
      return 100;
    }};
  
  public int getPercentProcessed(File currFile){
    SourceData sData = openSourceDatas.get(currFile);
    if(sData==null){
      //log.warn("No SourceData found for file={}", currFile);
      return -1;
    }
    return sData.getPercentProcessed();
  }
  
  @SuppressWarnings({"unchecked"})
  public void importFile(String ingesterId, File file, GraphUri graphUri) throws IOException {
    // get ingester
    Factories factories = registry.get(ingesterId);
    Preconditions.checkNotNull(factories, "importerFactory not registered: " + ingesterId);
    
    SourceData sData = factories.sourceDataFactory.createFrom(file);
    SourceData prevSD = openSourceDatas.put(file, sData);
    if(prevSD!=null && prevSD!=COMPLETED_SD)
      log.warn("Overrode sourceData for incompletely-read file={}", file);
    final ImporterFactory importerF = factories.importerFactory;
    Importer importer=importerF.create(sData);
    
//    log.info("{}  {}", sData.toString(), importer);
    importData(sData, importer, graphUri);
    sData=null;
    openSourceDatas.put(file, COMPLETED_SD);
  }

  @SuppressWarnings({"unchecked"})
  public void importReadable(String ingesterId, Readable readable, GraphUri graphUri) throws IOException {
    // get ingester
    Factories factories = registry.get(ingesterId);
    Preconditions.checkNotNull(factories, "importerFactory not registered: " + ingesterId);
    
    SourceData sData = factories.sourceDataFactory.createFrom(readable);
    final ImporterFactory importerF = factories.importerFactory;
    
    importData(sData, importerF.create(sData), graphUri);
    sData=null;
  }
  
  public <B> void importData(SourceData<B> sData, final Importer<B> importer, GraphUri graphUri)
      throws IOException {
    Preconditions.checkNotNull(graphUri);

    // apply ingester on graph
    importer.importFile(sData, graphUri);
  }
}
