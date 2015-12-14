package net.deelam.enricher.indexing;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.deelam.graphtools.GraphUri;
import net.deelam.graphtools.importer.JsonPropertyMerger;
import net.deelam.graphtools.importer.PropertyMerger;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

/**
 * Retrieves a node/edge by their ID from given graphId (both stored/provided by NodeIndexer),
 * and adds it to the outputGraph with a new id prefixed with the graphId.
 * The original id is saved under the origIdPropKey, which is settable.
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
  private IdGraph<?> equivGraph;

  private final Map<String, IdGraph<?>> graphs = new HashMap<>();

  @Override
  public void close() {
    log.debug("Shutting down: {}", equivGraph);
    equivGraph.shutdown();
    for (IdGraph<?> g : new HashSet<>(graphs.values())) {
      g.shutdown();
    }
  }

  public MultigraphConsolidator(IdGraph<?> graph) throws IOException {
    equivGraph = graph;
  }

  /**
   * If graphIdMapper is set, either the long or short graph ID can be used
   */
  @Setter
  private IdMapper graphIdMapper = null;

  private IdGraph<?> getGraph(String graphUriStr) {
    IdGraph<?> graph = graphs.get(graphUriStr);
    if (graph == null) {
      try {
        graph = new GraphUri(graphUriStr).openExistingIdGraph();
        graphs.put(graphUriStr, graph);

        if (graphIdMapper != null) {
          String shortGraphUriStr = graphIdMapper.shortId(graphUriStr);
          graphs.put(shortGraphUriStr, graph);
        }
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

  @Setter
  private String srcGraphIdPropKey = null;

  @Setter
  private String origIdPropKey = "__origId";
  
  private PropertyMerger merger = new JsonPropertyMerger();

  private Vertex importVertex(Vertex v, String shortGraphId) {
    String newId = shortGraphId + ":" + v.getId();
    Vertex newV = equivGraph.getVertex(newId);
    if (newV == null) {
      newV = equivGraph.addVertex(newId);
      setNewProperties(v, shortGraphId, newV);
    }
    merger.mergeProperties(v, newV);
    return newV;
  }

  private Edge importEdge(Edge e, String shortGraphId) {
    String newId = shortGraphId + ":" + e.getId();
    Edge newE = equivGraph.getEdge(newId);
    if (newE == null) {
      Vertex outV=importVertex(e.getVertex(Direction.OUT), shortGraphId);
      Vertex inV=importVertex(e.getVertex(Direction.IN), shortGraphId);
      newE = equivGraph.addEdge(newId, outV, inV, e.getLabel());
      setNewProperties(e, shortGraphId, newE);
    }
    merger.mergeProperties(e, newE);
    return newE;
  }

  private void setNewProperties(Element v, String shortGraphId, Element newV) {
    String origId = v.getProperty(IdGraph.ID);
    if (origId == null)
      origId = v.getProperty(origIdPropKey);
    if (origId == null)
      origId = (String) v.getId();
    newV.setProperty(origIdPropKey, origId);
    
    if (srcGraphIdPropKey != null)
      newV.setProperty(srcGraphIdPropKey, shortGraphId);
  }

}
