package net.deelam.enricher.indexing;

import static com.google.common.base.Preconditions.*;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import net.deelam.graphtools.GraphUri;
import net.deelam.graphtools.GraphUtils;
import net.deelam.graphtools.JsonPropertyMerger;
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

  @Getter
  private IdGraph<?> graph;

  private final Map<String, IdGraph<?>> graphs = new HashMap<>();

  @Override
  public void close() throws Exception {
    //log.debug("Shutting down: {}", graph);
    //graph.shutdown();
    if(graphIdMapper!=null)
      graphIdMapper.close();
    for (IdGraph<?> g : new HashSet<>(graphs.values())) {
      g.shutdown();
    }
  }

  public MultigraphConsolidator(IdGraph<?> idGraph) throws IOException {
    this.graph = idGraph;
    
    srcGraphIdPropKey=GraphUtils.getMetaData(graph, SRCGRAPHID_PROPKEY);
    origIdPropKey=GraphUtils.getMetaData(graph, ORIGID_PROPKEY);
    
    String graphIdMapFile=GraphUtils.getMetaData(graph, GRAPHID_MAP_FILE);
    if(graphIdMapFile!=null){
      if(new File(graphIdMapFile).exists()){
        log.info("Using graphIdMapFile={}", graphIdMapFile);
        graphIdMapper=new IdMapper(graphIdMapFile);
      }else{
        log.warn("Could not find graphIdMapFile={}.  Fix this or call setGraphIdMapper() to override with your own.", graphIdMapFile);
      }
    }
  }

  private static final String SRCGRAPHID_PROPKEY = "_SRCGRAPHID_PROPKEY_";
  private String srcGraphIdPropKey;
  public void setSrcGraphIdPropKey(String srcGraphIdPropKey) {
    this.srcGraphIdPropKey = srcGraphIdPropKey;
    GraphUtils.setMetaData(graph, SRCGRAPHID_PROPKEY, srcGraphIdPropKey);
  }

  public String getSrcGraphId(Vertex v){
    String shortGraphId = v.getProperty(srcGraphIdPropKey);
    return graphIdMapper.longId(shortGraphId);
  }
  
  private static final String ORIGID_PROPKEY = "_ORIGID_PROPKEY_";
  private String origIdPropKey;
  public void setOrigIdPropKey(String origIdPropKey) {
    this.origIdPropKey = origIdPropKey;
    GraphUtils.setMetaData(graph, ORIGID_PROPKEY, origIdPropKey);
  }
  
  public String getOrigId(Vertex v){
    return v.getProperty(origIdPropKey);
  }
  
  private static final String GRAPHID_MAP_FILE="_GRAPHID_MAP_";
  private IdMapper graphIdMapper = null;
  public void setGraphIdMapper(IdMapper graphIdMapper) {
    this.graphIdMapper = graphIdMapper;
    GraphUtils.setMetaData(graph, GRAPHID_MAP_FILE, graphIdMapper.getFilename());
  }

  private IdGraph<?> getGraph(String graphId) {
    String shortGraphId=graphId;
    if (graphIdMapper != null) {
      shortGraphId = graphIdMapper.shortId(graphId);
    }
    IdGraph<?> graph = graphs.get(shortGraphId);
    if (graph == null) {
      try {
        graph = new GraphUri(graphId).openExistingIdGraph();
        graphs.put(shortGraphId, graph);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    return graph;
  }

  public Vertex getVertex(String nodeId, String graphId) {
    IdGraph<?> g = getGraph(graphId);
    Vertex v = g.getVertex(nodeId);
    return v;
  }

  private Edge getEdge(String edgeId, String graphId) {
    IdGraph<?> g = getGraph(graphId);
    Edge e = g.getEdge(edgeId);
    return e;
  }
  
  public Vertex importVertex(String nodeId, String graphId) {
    Vertex v = getVertex(nodeId, graphId);
    checkNotNull(v, "Cannot find nodeId=" + nodeId + " in graph=" + graphId);
    String shortGraphId = (graphIdMapper == null) ? graphId : graphIdMapper.shortId(graphId);
    return importVertex(v, shortGraphId);
  }

  public Edge importEdge(String edgeId, String graphId) {
    Edge e = getEdge(edgeId, graphId);
    checkNotNull(e, "Cannot find edgeId=" + edgeId + " in graph=" + graphId);
    String shortGraphId = (graphIdMapper == null) ? graphId : graphIdMapper.shortId(graphId);
    return importEdge(e, shortGraphId);
  }

//  public void addEdge(String edgeId, Vertex nodeOut, Vertex nodeIn, String edgeLabel) {
//    Edge equivEdge = equivGraph.getEdge(edgeId);
//    if (equivEdge == null) {
//      equivEdge = equivGraph.addEdge(edgeId, nodeOut, nodeIn, edgeLabel);
//    }
//  }

  private PropertyMerger merger = new JsonPropertyMerger();

  private Vertex importVertex(Vertex v, String shortGraphId) {
    String newId = shortGraphId + ":" + v.getId();
    Vertex newV = graph.getVertex(newId);
    if (newV == null) {
      newV = graph.addVertex(newId);
      setNewProperties(v, shortGraphId, newV);
    }
    merger.mergeProperties(v, newV);
    return newV;
  }

  private Edge importEdge(Edge e, String shortGraphId) {
    String newId = shortGraphId + ":" + e.getId();
    Edge newE = graph.getEdge(newId);
    if (newE == null) {
      Vertex outV=importVertex(e.getVertex(Direction.OUT), shortGraphId);
      Vertex inV=importVertex(e.getVertex(Direction.IN), shortGraphId);
      newE = graph.addEdge(newId, outV, inV, e.getLabel());
      setNewProperties(e, shortGraphId, newE);
    }
    merger.mergeProperties(e, newE);
    return newE;
  }

  private void setNewProperties(Element v, String shortGraphId, Element newV) {
    if(origIdPropKey!=null){
      String origId = v.getProperty(IdGraph.ID); // If v instanceof IdElement, getProperty(IdGraph.ID) returns null
      if (origId == null)
        origId = v.getProperty(origIdPropKey);
      if (origId == null)
        origId = (String) v.getId();
      
      newV.setProperty(origIdPropKey, origId);
    }
    
    if (srcGraphIdPropKey != null)
      newV.setProperty(srcGraphIdPropKey, shortGraphId);
  }

  ///

  public void addNeighborsOf(String nodeStringId, String graphId, int hops) {
    Vertex v = getVertex(nodeStringId, graphId);
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
}
