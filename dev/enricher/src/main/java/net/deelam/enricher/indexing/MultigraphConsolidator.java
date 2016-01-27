package net.deelam.enricher.indexing;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.deelam.graphtools.GraphTransaction;
import net.deelam.graphtools.GraphUri;
import net.deelam.graphtools.GraphUtils;
import net.deelam.graphtools.PropertyMerger;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

/**
 * Retrieves a node/edge by their ID from given graphId (both stored/provided by NodeIndexer),
 * and adds it to the outputGraph with a new id prefixed with the graphId.
 * 
 * If origIdPropKey is set, the original id is saved under the origIdPropKey.
 * 
 * If srcGraphPropKey is set, a property will be added to each node/edge with the graphId.
 * 
 * If a graphIdMapper is set, a short graphId will be used as the prefix and property value.
 * If graphIdMapper.getShortId(graphId)==null, the graphIdMapper will create a short graphId to be used.
 * 
 * If desired, the graphIdMapper can be used later to interpret the srcGraphPropKey property.
 * 
 * @author dlam
 */
@Slf4j
public class MultigraphConsolidator implements AutoCloseable {

  private final GraphUri dstGraphUri;
  
  @Getter
  private IdGraph<?> graph;

  private final Map<String, IdGraph<?>> srcGraphs = new HashMap<>();
  private final Map<String, GraphUri> srcGraphUris = new HashMap<>();

  @Override
  public void close() throws IOException {
    if (graphIdMapper != null) {
      saveIdMapperAsGraphMetaData();
      graphIdMapper.close();
    }
    dstGraphUri.shutdown();
    for (GraphUri gUri : new HashSet<>(srcGraphUris.values())) {
      gUri.shutdown();
    }
  }

  //  public MultigraphConsolidator(IdGraph<?> idGraph, PropertyMerger pmerger) throws IOException {
  //    
  //  }

  public MultigraphConsolidator(GraphUri graphUri) throws IOException {
    dstGraphUri=graphUri;
    this.graph = dstGraphUri.openExistingIdGraph(); // graph must be initially closed so that close() can call shutdown()
    merger = dstGraphUri.createPropertyMerger();

    srcGraphIdPropKey = GraphUtils.getMetaData(graph, SRCGRAPHID_PROPKEY);
    origIdPropKey = GraphUtils.getMetaData(graph, ORIGID_PROPKEY);

    loadIdMapperFromGraphMetaData();
  }

  private static final String SRCGRAPHID_PROPKEY = "_SRCGRAPHID_PROPKEY_";
  @Getter
  private String srcGraphIdPropKey;

  public void setSrcGraphIdPropKey(String srcGraphIdPropKey) {
    this.srcGraphIdPropKey = srcGraphIdPropKey;
    GraphUtils.setMetaData(graph, SRCGRAPHID_PROPKEY, srcGraphIdPropKey);
  }

  public String getSrcGraphId(Vertex v) {
    String shortGraphId = v.getProperty(srcGraphIdPropKey);
    return graphIdMapper.longId(shortGraphId);
  }

  private static final String ORIGID_PROPKEY = "_ORIGID_PROPKEY_";
  @Getter
  private String origIdPropKey;

  public void setOrigIdPropKey(String origIdPropKey) {
    this.origIdPropKey = origIdPropKey;
    GraphUtils.setMetaData(graph, ORIGID_PROPKEY, origIdPropKey);
  }

  public String getOrigId(Vertex v) {
    return v.getProperty(origIdPropKey);
  }

  @Getter
  private IdMapper graphIdMapper = null;

  private String getShortGraphId(String graphId) {
    return (graphIdMapper == null) ? graphId : graphIdMapper.shortId(graphId);
  }

  public void setGraphIdMapper(IdMapper graphIdMapper) {
    this.graphIdMapper = graphIdMapper;
    saveIdMapperAsGraphMetaData();
  }

  @Setter
  private boolean useFileBasedIdMapper = true;

  private static final String GRAPHID_MAP_FILE = "_GRAPHID_MAP_FILE";
  private static final String GRAPHID_MAP_SIZE = "_GRAPHID_MAP_SIZE";
  private static final String GRAPHID_MAP_ENTRY_PREFIX = "_GRAPHID_MAP_ENTRY_";

  private void saveIdMapperAsGraphMetaData() {
    int tx = GraphTransaction.begin(graph);
    try {
      if (useFileBasedIdMapper) {
        GraphUtils.setMetaData(graph, GRAPHID_MAP_FILE, graphIdMapper.getFilename());
      } else {
        // save mapper in META_DATA node
        Vertex mdV = GraphUtils.getMetaDataNode(graph);
        mdV.setProperty(GRAPHID_MAP_SIZE, graphIdMapper.getShortIdSet().size());
        for (String k : graphIdMapper.getShortIdSet()) {
          mdV.setProperty(GRAPHID_MAP_ENTRY_PREFIX + k, graphIdMapper.longId(k));
        }
      }
      GraphTransaction.commit(tx);
    } catch (RuntimeException re) {
      GraphTransaction.rollback(tx);
      throw re;
    }
  }

  private void loadIdMapperFromGraphMetaData() throws FileNotFoundException, IOException {
    int tx = GraphTransaction.begin(graph);
    try {
      if (useFileBasedIdMapper) {
        String graphIdMapFile = GraphUtils.getMetaData(graph, GRAPHID_MAP_FILE);
        if (graphIdMapFile != null) {
          if (new File(graphIdMapFile).exists()) {
            log.info("Using graphIdMapFile={}", graphIdMapFile);
            graphIdMapper = IdMapper.newFromFile(graphIdMapFile);
          } else {
            log.warn(
                "Could not find graphIdMapFile={}.  Fix this or call setGraphIdMapper() to override with your own.",
                graphIdMapFile);
          }
        }
      } else {
        // load mapper from META_DATA node if it exists
        Integer graphIdMapSize = GraphUtils.getMetaData(graph, GRAPHID_MAP_SIZE);
        if (graphIdMapSize != null) {
          Vertex mdV = GraphUtils.getMetaDataNode(graph);
          graphIdMapper = new IdMapper();
          for (String k : mdV.getPropertyKeys()) {
            if (k.startsWith(GRAPHID_MAP_ENTRY_PREFIX)) {
              String shortId = k.substring(GRAPHID_MAP_ENTRY_PREFIX.length() + 1);
              String longId = mdV.getProperty(k);
              graphIdMapper.put(shortId, longId);
            }
          }
        }
      }
      GraphTransaction.commit(tx);
    } catch (RuntimeException re) {
      GraphTransaction.rollback(tx);
      throw re;
    }
  }

  private IdGraph<?> getGraph(String graphId) {
    IdGraph<?> graph = srcGraphs.get(graphId); // for lookup efficiency
    if (graph != null)
      return graph;

    String shortGraphId = getShortGraphId(graphId);
    graph = srcGraphs.get(shortGraphId);
    if (graph == null) {
      try {
        GraphUri graphUri = new GraphUri(graphId);
        graph = graphUri.openExistingIdGraph();
        srcGraphUris.put(shortGraphId, graphUri); //so that graph can be closed
        addSrcGraph(graphUri, graphId, shortGraphId, graph);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    return graph;
  }

  private void addSrcGraph(GraphUri graphUri, String graphId, String shortGraphId, IdGraph<?> graph) {
    IdGraph<?> existingGraph = srcGraphs.put(shortGraphId, graph);
    if (existingGraph != null) {
      log.warn("Overriding existing graph: {} with shortGraphId={}", graph, shortGraphId);
    }
    srcGraphs.put(graphId, graph); // for lookup efficiency, so a shortGraphId is not created each time
  }

  /**
   * Some graph implementations cannot handle multiple connections,
   * so this registers an existing graph connection to be used by this class 
   */
  public void registerGraph(GraphUri graphUri) {
    String graphId = graphUri.asString();
    String shortGraphId = getShortGraphId(graphId);
    IdGraph<?> graph = srcGraphs.get(shortGraphId);
    if (graph == null) {
      try {
        if(graphUri.isOpen()){
          graph = graphUri.getGraph();
          // do not close graph since I didn't open it
        } else {
          graph = graphUri.openExistingIdGraph();//so that graph can be closed
          srcGraphUris.put(shortGraphId, graphUri);
        }
        addSrcGraph(graphUri, graphId, shortGraphId, graph);
      } catch (FileNotFoundException e) {
        e.printStackTrace();
      }
    } else {
      log.warn("Graph already registered: {} with shortGraphId={}", graphUri, shortGraphId);
    }
  }

  public Vertex getExternalVertex(String nodeId, String graphId) {
    IdGraph<?> g = getGraph(graphId);
    Vertex v = g.getVertex(nodeId);
    return v;
  }

  private Edge getExternalEdge(String edgeId, String graphId) {
    IdGraph<?> g = getGraph(graphId);
    Edge e = g.getEdge(edgeId);
    return e;
  }

  public Vertex importVertex(Vertex v, String graphId) {
    return importVertex((String) v.getId(), graphId);
  }

  public Vertex importVertex(String nodeId, String graphId) {
    Vertex v = getExternalVertex(nodeId, graphId);
    checkNotNull(v, "Cannot find nodeId=" + nodeId + " in graph=" + graphId);
    String shortGraphId = getShortGraphId(graphId);
    return importVertexUsingShortId(v, shortGraphId);
  }

  public Edge importEdge(Edge e, String graphId) {
    return importEdge((String) e.getId(), graphId);
  }

  public Edge importEdge(String edgeId, String graphId) {
    Edge e = getExternalEdge(edgeId, graphId);
    checkNotNull(e, "Cannot find edgeId=" + edgeId + " in graph=" + graphId);
    String shortGraphId = getShortGraphId(graphId);
    return importEdgeUsingShortId(e, shortGraphId, null, null);
  }

  /**
   * import all edge info and provenance, excepts uses different vertices
   * @param e
   * @param shortGraphId
   * @param importedOutV some vertex already in graph
   * @param importedInV some vertex already in graph
   * @return
   */
  public Edge importEdgeWithDifferentVertices(String edgeId, String graphId, Vertex importedOutV, Vertex importedInV) {
    Edge e = getExternalEdge(edgeId, graphId);
    checkNotNull(e, "Cannot find edgeId=" + edgeId + " in graph=" + graphId);
    String shortGraphId = getShortGraphId(graphId);
    return importEdgeUsingShortId(e, shortGraphId, importedOutV, importedInV);
  }

  //  public void addEdge(String edgeId, Vertex nodeOut, Vertex nodeIn, String edgeLabel) {
  //    Edge equivEdge = equivGraph.getEdge(edgeId);
  //    if (equivEdge == null) {
  //      equivEdge = equivGraph.addEdge(edgeId, nodeOut, nodeIn, edgeLabel);
  //    }
  //  }

  private final PropertyMerger merger;

  public boolean useOrigId = false; // used to import from another graph created by MultigraphConsolidator // TODO: design better useOrigId

  private Vertex importVertexUsingShortId(Vertex v, String shortGraphId) {
    String newId = (String) ((useOrigId) ? v.getId() : shortGraphId + ":" + v.getId());
    Vertex newV = graph.getVertex(newId);
    if (newV == null) {
      newV = graph.addVertex(newId);
      setNewProperties(v, shortGraphId, newV);
    }
    merger.mergeProperties(v, newV);
    return newV;
  }

  public Vertex getVertex(String nodeId, String graphId) {
    String shortGraphId = getShortGraphId(graphId);
    String localId = shortGraphId + ":" + nodeId;
    Vertex existingV = graph.getVertex(localId);
    return existingV;
  }

  public Edge getEdge(String edgeId, String graphId) {
    String shortGraphId = getShortGraphId(graphId);
    String localId = shortGraphId + ":" + edgeId;
    Edge existingE = graph.getEdge(localId);
    return existingE;
  }

  private Edge importEdgeUsingShortId(Edge e, String shortGraphId, Vertex outV, Vertex inV) {
    String newId = (String) ((useOrigId) ? e.getId() : shortGraphId + ":" + e.getId());
    Edge newE = graph.getEdge(newId);
    if (newE == null) {
      if (outV == null)
        outV = importVertexUsingShortId(e.getVertex(Direction.OUT), shortGraphId);
      if (inV == null)
        inV = importVertexUsingShortId(e.getVertex(Direction.IN), shortGraphId);
      newE = graph.addEdge(newId, outV, inV, e.getLabel());
      setNewProperties(e, shortGraphId, newE);
    }
    merger.mergeProperties(e, newE);
    return newE;
  }

  private void setNewProperties(Element v, String shortGraphId, Element newV) {
    if (origIdPropKey != null) {
      String origId = v.getProperty(origIdPropKey); // use original id if possible
      if (origId == null)
        origId = v.getProperty(IdGraph.ID); // If v instanceof IdElement, getProperty(IdGraph.ID) returns null
      if (origId == null)
        origId = (String) v.getId();

      newV.setProperty(origIdPropKey, origId);
    }

    if (srcGraphIdPropKey != null)
      newV.setProperty(srcGraphIdPropKey, shortGraphId);
  }

  ///

  public void addNeighborsOf(String nodeStringId, String graphId, int hops) {
    Vertex v = getExternalVertex(nodeStringId, graphId);
    Vertex newV = importVertex(nodeStringId, graphId);
    addNeighborsOf(v, newV, graphId, hops);
  }

  private void addNeighborsOf(Vertex parent, Vertex newParent, String graphId, int hops) {
    // TODO: 2: improve traversal to stop when neighbor already visited at node.hops>hops
    if (hops == 0)
      return;
    checkNotNull(parent);
    log.debug("Adding neighbor {}: {}", hops, newParent);
    for (Edge e : parent.getEdges(Direction.OUT)) {
      Vertex v = e.getVertex(Direction.IN);
      Vertex newV = importVertex((String) v.getId(), graphId);
      importEdge((String) e.getId(), graphId);
      //            GraphImportUtils.importEdge(equivGraph, e, graphId+":"+e.getId(), Direction.OUT, newParent, newV);

      addNeighborsOf(v, newV, graphId, hops - 1);
    }
    for (Edge e : parent.getEdges(Direction.IN)) {
      Vertex v = e.getVertex(Direction.OUT);
      Vertex newV = importVertex((String) v.getId(), graphId);
      importEdge((String) e.getId(), graphId);
      //            GraphImportUtils.importEdge(equivGraph, e, graphId+":"+e.getId(), Direction.IN, newParent, newV);

      addNeighborsOf(v, newV, graphId, hops - 1);
    }
  }

  ///

  public void importGraph(String srcGraphId, int commitFreq) {
    IdGraph<?> from = getGraph(srcGraphId);
    String shortGraphId = getShortGraphId(srcGraphId);

    // TODO: 3: currently copies METADATA nodes from source graphs, which can make graph dirty

    int tx = GraphTransaction.begin(graph, commitFreq);
    try {
      for (final Vertex fromVertex : from.getVertices()) {
        importVertexUsingShortId(fromVertex, shortGraphId);
        GraphTransaction.commitIfFull(tx);
      }

      for (final Edge fromEdge : from.getEdges()) {
        importEdgeUsingShortId(fromEdge, shortGraphId, null, null);
        GraphTransaction.commitIfFull(tx);
      }

      GraphTransaction.commit(tx);
    } catch (RuntimeException re) {
      GraphTransaction.rollback(tx);
      throw re;
    }
  }

}
