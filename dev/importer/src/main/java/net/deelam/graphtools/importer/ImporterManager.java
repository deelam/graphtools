package net.deelam.graphtools.importer;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import lombok.AllArgsConstructor;
import net.deelam.graphtools.GraphUri;

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

  @SuppressWarnings("unchecked")
  public void importFile(String ingesterId, File file, GraphUri destName) throws IOException {
    // get ingester
    Ingester ingester = registry.get(ingesterId);
    Preconditions.checkNotNull(ingester, "ingester not registered: " + ingesterId);

    // create graph
    IdGraph<?> graph = destName.openIdGraph();
    Preconditions.checkNotNull(graph, "Could not open graph: " + graph);

    // apply ingester on graph
    SourceData<?> sData = ingester.sdFactory.createFrom(file);
    ingester.importer.importFile(sData, graph);

    // close graph
    graph.shutdown();
  }
}