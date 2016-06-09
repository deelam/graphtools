package net.deelam.enricher.faceting;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.mutable.MutableLong;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.deelam.enricher.indexing.IdMapper;
import net.deelam.graphtools.GraphTransaction;
import net.deelam.graphtools.GraphUri;
import net.deelam.graphtools.GraphUtils;
import net.deelam.graphtools.PropertyMerger;

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

  //private final Map<String, IdGraph<?>> srcGraphs = new HashMap<>();
  private final LoadingCache<String, IdGraph<?>> srcGraphs;

  private final Map<String, GraphUri> srcGraphUrisToShutdown = new HashMap<>();
  private final Map<String, MutableLong> srcGraphNodeImportCount = new HashMap<>();
  private final Map<String, MutableLong> srcGraphEdgeImportCount = new HashMap<>();

  @Override
  public void close() throws IOException {
    if (graphIdMapper != null) {
      saveIdMapperAsGraphMetaData();
      graphIdMapper.close();
    }
    {
      Vertex mdV = GraphUtils.getMetaDataNode(graph);
      for(Entry<String, MutableLong> e:srcGraphNodeImportCount.entrySet()){
        mdV.setProperty("importedNodes_"+e.getKey(), srcGraphNodeImportCount.get(e.getKey()).getValue());
        mdV.setProperty("importedEdges_"+e.getKey(), srcGraphEdgeImportCount.get(e.getKey()).getValue());
      }
    }
    dstGraphUri.shutdown();
    
    srcGraphs.invalidateAll();
    srcGraphs.cleanUp();
    
    for (GraphUri gUri : new HashSet<>(srcGraphUrisToShutdown.values())) {
      gUri.shutdown();
    }
  }

  //  public MultigraphConsolidator(IdGraph<?> idGraph, PropertyMerger pmerger) throws IOException {
  //    
  //  }
  public MultigraphConsolidator(GraphUri graphUri) throws IOException {
    this(graphUri, false);
  }
  
  private OriginalNodeCodec origNodeCodec=null;
      
  public MultigraphConsolidator(GraphUri graphUri, boolean shouldAlreadyExist) throws IOException {
    srcGraphs = CacheBuilder.newBuilder()
        .maximumSize(100)  // TODO: 0: make this configurable
        .removalListener(notification -> {
          GraphUri guri = srcGraphUrisToShutdown.remove(notification.getKey());
          log.debug("cache element removal: {}={}", notification.getKey(), guri);
          if(guri!=null)
            guri.shutdown();
          else {
            IdGraph<?> graph = (IdGraph<?>) notification.getValue();
            log.info("Not shutting down graph probably because registerGraph() was used for graph={}", graph);
          }
        })
        .build(
            new CacheLoader<String, IdGraph<?>>() {
              public IdGraph<?> load(String key) throws IOException {
                GraphUri graphUri = new GraphUri(key);
                IdGraph<?> graph = graphUri.openIdGraph();
                return graph;
              }
            });
    
    dstGraphUri=graphUri;
    merger = dstGraphUri.createPropertyMerger();
    if(shouldAlreadyExist || dstGraphUri.exists()){
      graph = dstGraphUri.getOrOpenGraph(); //openExistingIdGraph(); // graph will be open here so that close() can call shutdown()
      String origIdPropKey = GraphUtils.getMetaData(graph, ORIGID_PROPKEY);
      if(origIdPropKey!=null){
        origNodeCodec=new OriginalNodeCodec(origIdPropKey, graph, merger);
      }
    } else {
      graph=dstGraphUri.createNewIdGraph(false);
      GraphUtils.setMetaData(graph, GraphUtils.GRAPHBUILDER_PROPKEY, this.getClass().getSimpleName());
      //setOrigIdPropKey("_origId"); //default value
    }

    //    srcGraphIdPropKey = GraphUtils.getMetaData(graph, SRCGRAPHID_PROPKEY);

    loadIdMapperFromGraphMetaData();
  }
  
//  @Deprecated
//  private static final String SRCGRAPHID_PROPKEY = "_SRCGRAPHID_PROPKEY_";
//  @Deprecated
//  @Getter
//  private String srcGraphIdPropKey;
//
  @Deprecated
  public void setSrcGraphIdPropKey(String srcGraphIdPropKey) {
//    this.srcGraphIdPropKey = srcGraphIdPropKey;
//    GraphUtils.setMetaData(graph, SRCGRAPHID_PROPKEY, srcGraphIdPropKey);
  }

  private static final String ORIGID_PROPKEY = "_ORIGID_PROPKEY_";
  public void setOrigIdPropKey(String origIdPropKey) {
    if(origNodeCodec!=null){
      throw new IllegalStateException("OrigIdPropKey already set to "+origNodeCodec.getOrigIdPropKey());
    }
    if(origIdPropKey==null){
      origNodeCodec=null;
    } else {
      origNodeCodec=new OriginalNodeCodec(origIdPropKey, graph, merger);
      GraphUtils.setMetaData(graph, ORIGID_PROPKEY, origIdPropKey);
    }
  }
  
  //

  public String getOrigId(Vertex v) {
    if(origNodeCodec==null){
      log.warn("Original node id was not stored when graph was created!");
      return null;
    }
    return origNodeCodec.getOrigId(v);
  }
  
  public String getSrcGraphId(Vertex v) {
    if(origNodeCodec==null){
      log.warn("Original node id was not stored when graph was created!");
      return null;
    }
    return origNodeCodec.getSrcGraphId(v, graphIdMapper);
  }

  private void setOrigNodeProperties(Element v, String shortGraphId, Element newV) {
    if (origNodeCodec != null) {
      String origId = null; //v.getProperty(origIdPropKey); // use original id if possible
      if (origId == null)
        origId = v.getProperty(IdGraph.ID); // If v instanceof IdElement, getProperty(IdGraph.ID) returns null
      if (origId == null)
        if(v.getId() instanceof String)
          origId = (String) v.getId();
        else{
          log.warn("Element id is not a String: {} is of class={}", v.getId(), v.getId().getClass());
          origId = v.getId().toString();
        }
      origNodeCodec.setOrigId(newV, origId, shortGraphId);
    }
  }
  
  ///

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
      } 
      {
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
      } else
      { // TODO why doesn't this work?
        // load mapper from META_DATA node if it exists
        Integer graphIdMapSize = GraphUtils.getMetaData(graph, GRAPHID_MAP_SIZE);
        if (graphIdMapSize != null) {
          Vertex mdV = GraphUtils.getMetaDataNode(graph);
          if (graphIdMapper == null)
            graphIdMapper = new IdMapper();
          for (String k : mdV.getPropertyKeys()) {
            if (k.startsWith(GRAPHID_MAP_ENTRY_PREFIX)) {
              String shortId = k.substring(GRAPHID_MAP_ENTRY_PREFIX.length());
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
//    try{
      IdGraph<?> graph = srcGraphs.getIfPresent(graphId); // in case graphId is already a shortGraphId
      if (graph != null)
        return graph;
  
      String shortGraphId = getShortGraphId(graphId);
      graph = srcGraphs.getIfPresent(shortGraphId);
      if (graph == null) {
        try {
          GraphUri graphUri = new GraphUri(graphId).readOnly();
          graph = graphUri.openExistingIdGraph();
          srcGraphUrisToShutdown.put(shortGraphId, graphUri); //so that graph can be closed
          addSrcGraph(graphUri, graphId, shortGraphId, graph);
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
//    }catch(ExecutionException ee){
//      throw new RuntimeException(ee);
//    }
    return graph;
  }

  private void addSrcGraph(GraphUri graphUri, String graphId, String shortGraphId, IdGraph<?> graph) {
    IdGraph<?> existingGraph = srcGraphs.getIfPresent(shortGraphId);
    srcGraphs.put(shortGraphId, graph);
    if (existingGraph != null) {
      log.warn("Overriding existing graph: {} with shortGraphId={}", graph, shortGraphId);
    }
    
    if(!srcGraphNodeImportCount.containsKey(shortGraphId)){
      srcGraphNodeImportCount.put(shortGraphId, new MutableLong(0));
      srcGraphEdgeImportCount.put(shortGraphId, new MutableLong(0));
      addEdgeFromSrcMetaDataNode(shortGraphId);
      importMetadataSubgraph(graphIdMapper.longId(shortGraphId));
    }
  }

  private void addEdgeFromSrcMetaDataNode(String shortSrcGraphId) {
    Vertex mdV = GraphUtils.getMetaDataNode(graph);
    Vertex srcGraphMdV=GraphUtils.getMetaDataNode(getGraph(shortSrcGraphId));
    String srcGraphUri = srcGraphMdV.getProperty(GraphUtils.GRAPHURI_PROP);
    Vertex importedSrcGraphMdV = importVertexWithId(srcGraphMdV, shortSrcGraphId, srcGraphUri);
    
    String edgeId=srcGraphUri+"->"+mdV.getProperty(GraphUtils.GRAPHURI_PROP);
    if(graph.getEdge(edgeId)==null){
      Edge importedEdge = graph.addEdge(edgeId, importedSrcGraphMdV, mdV, "inputGraphTo");
      importedEdge.setProperty(GraphUtils.GRAPH_METADATA_PROP, true);
    }
  }

  private void importMetadataSubgraph(String srcGraphId) {
    Vertex srcGraphMdV = GraphUtils.getMetaDataNode(getGraph(srcGraphId));
    String shortGraphId = getShortGraphId(srcGraphId);

    int tx = GraphTransaction.begin(graph);
    try {
      String graphUri = srcGraphMdV.getProperty(GraphUtils.GRAPHURI_PROP);
      Vertex importedMdV = importVertexWithId(srcGraphMdV, shortGraphId, graphUri);

      importSubgraphUsingOrigId(srcGraphMdV, srcGraphId, importedMdV);
      GraphTransaction.commit(tx);
    } catch (RuntimeException re) {
      GraphTransaction.rollback(tx);
      throw re;
    }
  }
  
  /**
   * Some graph implementations cannot handle multiple connections,
   * so this registers an existing graph connection to be used by this class 
   */
  public void registerGraph(GraphUri graphUri) {
    String graphId = graphUri.asString();
    String shortGraphId = getShortGraphId(graphId);
    IdGraph<?> graph = srcGraphs.getIfPresent(shortGraphId);
    if (graph == null) {
      try {
        if(graphUri.isOpen()){
          graph = graphUri.getGraph();
          // do not close graph since I didn't open it
        } else {
          graph = graphUri.openExistingIdGraph();
          srcGraphUrisToShutdown.put(shortGraphId, graphUri); //so that graph can be closed
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
  private static final String GRAPH_ID_SEP = ":";

  private Vertex importVertexUsingShortId(Vertex v, String shortGraphId) {
    Object newId = ((useOrigId) ? v.getId() : genNewId(shortGraphId, v.getId()));
    return importVertexWithId(v, shortGraphId, newId);
  }
  
  private Vertex importVertexWithId(Vertex v, String shortGraphId, Object newId) {
    Vertex newV = graph.getVertex((String) newId);
    if (newV == null) {
      newV = graph.addVertex(newId);
      final MutableLong mutableLong = srcGraphNodeImportCount.get(shortGraphId);
      if(mutableLong!=null) mutableLong.increment();
      else log.error("{} not found in {}", shortGraphId, srcGraphNodeImportCount);
      setOrigNodeProperties(v, shortGraphId, newV);
    }
    merger.mergeProperties(v, newV);
    return newV;
  }

  private String genNewId(String shortGraphId, Object nodeId) {
    return shortGraphId + GRAPH_ID_SEP + nodeId;
  }

  public Vertex getVertex(String nodeId, String graphId) {
    checkNotNull(graphId, nodeId);
    String localId = genNewId(getShortGraphId(graphId), nodeId);
    Vertex existingV = graph.getVertex(localId);
    return existingV;
  }

  public Edge getEdge(String edgeId, String graphId) {
    String localId = genNewId(getShortGraphId(graphId), edgeId);
    Edge existingE = graph.getEdge(localId);
    return existingE;
  }

  private Edge importEdgeUsingShortId(Edge e, String shortGraphId, Vertex outV, Vertex inV) {
    Object newId = ((useOrigId) ? e.getId() : genNewId(shortGraphId, e.getId()));
    return importEdgeWithId(e, shortGraphId, outV, inV, newId);
  }
  private Edge importEdgeWithId(Edge e, String shortGraphId, Vertex outV, Vertex inV, Object newId) {
    Edge newE = graph.getEdge((String) newId);
    if (newE == null) {
      if (outV == null)
        outV = importVertexUsingShortId(e.getVertex(Direction.OUT), shortGraphId);
      if (inV == null)
        inV = importVertexUsingShortId(e.getVertex(Direction.IN), shortGraphId);
      newE = graph.addEdge(newId, outV, inV, e.getLabel());
      srcGraphEdgeImportCount.get(shortGraphId).increment();
      setOrigNodeProperties(e, shortGraphId, newE);
    }
    merger.mergeProperties(e, newE);
    return newE;
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
    IdGraph<?> fromGraph = getGraph(srcGraphId);
    String shortGraphId = getShortGraphId(srcGraphId);

    int tx = GraphTransaction.begin(graph, commitFreq);
    try {
      for (final Vertex fromVertex : fromGraph.getVertices()) {
        // skip METADATA nodes from source graphs, which can make graph dirty
        if(fromVertex.getProperty(GraphUtils.GRAPH_METADATA_PROP)==null){
          //Vertex newV = 
              importVertexUsingShortId(fromVertex, shortGraphId);
          //log.info("Importing node: {}", newV);
          GraphTransaction.commitIfFull(tx);
        }else{
          log.debug("Skipping metadata node: {}", fromVertex);
        }
      }

      for (final Edge fromEdge : fromGraph.getEdges()) {
        if(fromEdge.getProperty(GraphUtils.GRAPH_METADATA_PROP)==null){
          importEdgeUsingShortId(fromEdge, shortGraphId, null, null);
          GraphTransaction.commitIfFull(tx);
        }else{
          log.debug("Skipping metadata edge: {}", fromEdge);
        }
      }

      GraphTransaction.commit(tx);
    } catch (RuntimeException re) {
      GraphTransaction.rollback(tx);
      throw re;
    }
  }
  
  public void importSubgraphUsingOrigId(Vertex rootV, String srcGraphId, Vertex importedRootV) {
    String shortGraphId = getShortGraphId(srcGraphId);
    int tx = GraphTransaction.begin(graph);
    try {
      if(importedRootV==null){
        importedRootV=importVertexWithId(rootV, shortGraphId, rootV.getId());
      }
      recursiveImport(rootV, shortGraphId, importedRootV, rootV);
      GraphTransaction.commit(tx);
    } catch (RuntimeException re) {
      GraphTransaction.rollback(tx);
      throw re;
    }
  }
  
  private void recursiveImport(Vertex v, String shortGraphId, Vertex importedV, Vertex rootV) {
    for (Direction dir : GraphUtils.BOTHDIR)
      for (Edge e : v.getEdges(dir)) {
        Vertex oppV = e.getVertex(dir.opposite());
        boolean alreadyVisited=(graph.getVertex(oppV.getId())!=null);
        if(!rootV.equals(oppV)){
          Vertex importedOppV=importVertexWithId(oppV, shortGraphId, oppV.getId());
          if(dir==Direction.OUT)
            importEdgeWithId(e, shortGraphId, importedV, importedOppV, e.getId());
          else
            importEdgeWithId(e, shortGraphId, importedOppV, importedV, e.getId());
          
          if(!alreadyVisited)
            recursiveImport(oppV, shortGraphId, importedOppV, rootV);
        }
      }
  }

/*  public static void main(String[] args) throws ExecutionException {
    
    IdGraphFactoryNeo4j.register();
    ArrayList<GraphUri> uris=new ArrayList<>();
    for(int i=0; i<15; ++i){
      GraphUri uri = new GraphUri("neo4j:neo"+i);
      uris.add(uri);
    }

    for(GraphUri uri:uris){
      uri.openIdGraph();
      //graphs.get(uri.asString());
      uri.shutdown();
    }

    graphs.invalidateAll();
    graphs.cleanUp();
  }*/
}
