package net.deelam.graphtools.importer;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import net.deelam.graphtools.GraphUri;
import lombok.AllArgsConstructor;

import com.google.common.base.Preconditions;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

/**
 * 
 * @author deelam
 */
public class ImporterManager {
  @AllArgsConstructor
  public static class Ingester {
    String id;

    SourceDataFactory sdFactory;

    @SuppressWarnings("rawtypes")
    Importer importer;
  }

  private Map<String, Ingester> registry = new HashMap<>();

  public Ingester register(String id, SourceDataFactory sdFactory, Importer<?> importer) {
    return registry.put(id, new Ingester(id, sdFactory, importer));
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
  
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void importFile(String ingesterId, File file, GraphUri graphUri) throws IOException {
    // get ingester
    Ingester ingester = registry.get(ingesterId);
    Preconditions.checkNotNull(ingester, "ingester not registered: " + ingesterId);
    
    sData = ingester.sdFactory.createFrom(file);
    final Importer importer = ingester.importer;
    
    importData(sData, importer, graphUri);
    sData=null;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public void importReadable(String ingesterId, Readable readable, GraphUri graphUri) throws IOException {
    // get ingester
    Ingester ingester = registry.get(ingesterId);
    Preconditions.checkNotNull(ingester, "ingester not registered: " + ingesterId);
    
    sData = ingester.sdFactory.createFrom(readable);
    final Importer importer = ingester.importer;
    
    importData(sData, importer, graphUri);
    sData=null;
  }
  
  public <B> void importData(SourceData<B> sData, final Importer<B> importer, GraphUri graphUri)
      throws IOException {
    Preconditions.checkNotNull(graphUri);

    // apply ingester on graph
    importer.importFile(sData, graphUri);
  }
}
