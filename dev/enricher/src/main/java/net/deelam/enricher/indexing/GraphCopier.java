package net.deelam.enricher.indexing;

import static com.google.common.base.Preconditions.*;

import java.io.FileNotFoundException;
import java.io.IOException;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import net.deelam.graphtools.GraphTransaction;
import net.deelam.graphtools.GraphUri;
import net.deelam.graphtools.PropertyMerger;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

/**
 * @author dlam
 */
@Slf4j
public class GraphCopier implements AutoCloseable {

  @Getter
  private IdGraph<?> graph;

  private final GraphUri srcGraphUri;
  
  private final IdGraph<?> srcGraph;

  @Override
  public void close() throws IOException {
    //log.debug("Shutting down: {}", graph);
    srcGraphUri.shutdown();
  }

  public GraphCopier(GraphUri srcGraphUri, GraphUri dstGraphUri) throws IOException {
    this.srcGraphUri=srcGraphUri;
//    this.dstGraphUri=dstGraphUri;
    
    srcGraph = srcGraphUri.getOrOpenGraph();
    this.graph = dstGraphUri.getOrOpenGraph();
    merger = dstGraphUri.createPropertyMerger();
  }

  public Vertex importVertex(String nodeId) {
    Vertex v = srcGraph.getVertex(nodeId);
    checkNotNull(v, "Cannot find nodeId=" + nodeId + " in srcGraph");
    return importVertex(v);
  }

  public Edge importEdge(Edge e) {
    return importEdge(e, null, null);
  }

  public Edge importEdge(String edgeId) {
    Edge e = srcGraph.getEdge(edgeId);
    checkNotNull(e, "Cannot find edgeId=" + edgeId + " in srcGraph");
    return importEdge(e, null, null);
  }

  /**
   * import all edge info and provenance, excepts uses different vertices
   * @param importedOutV some vertex already in graph
   * @param importedInV some vertex already in graph
   * @param e
   * @param shortGraphId
   * @return
   */
  public Edge importEdgeWithDifferentVertices(String edgeId, Vertex importedOutV, Vertex importedInV) {
    Edge e = srcGraph.getEdge(edgeId);
    checkNotNull(e, "Cannot find edgeId=" + edgeId + " in srcGraph");
    return importEdge(e, importedOutV, importedInV);
  }

  private final PropertyMerger merger;

  public Vertex importVertex(Vertex v) {
    String nodeId = (String) v.getId();
    Vertex newV = graph.getVertex(nodeId);
    if (newV == null) {
      newV = graph.addVertex(nodeId);
    }
    merger.mergeProperties(v, newV);
    return newV;
  }

  public Edge importEdge(Edge e, Vertex outV, Vertex inV) {
    String newId = (String) e.getId();
    Edge newE = graph.getEdge(newId);
    if (newE == null) {
      if (outV == null)
        outV = importVertex(e.getVertex(Direction.OUT));
      if (inV == null)
        inV = importVertex(e.getVertex(Direction.IN));
      newE = graph.addEdge(newId, outV, inV, e.getLabel());
    }
    merger.mergeProperties(e, newE);
    return newE;
  }

  ///

  public void addNeighborsOf(String nodeStringId, String graphId, int hops) {
    Vertex v = srcGraph.getVertex(nodeStringId);
    Vertex newV = importVertex(nodeStringId);
    addNeighborsOf(v, newV, hops);
  }

  private void addNeighborsOf(Vertex parent, Vertex newParent, int hops) {
    // TODO: 2: improve traversal to stop when neighbor already visited at node.hops>hops
    if (hops == 0)
      return;
    checkNotNull(parent);
    log.debug("Adding neighbor {}: {}", hops, newParent);
    for (Edge e : parent.getEdges(Direction.OUT)) {
      Vertex v = e.getVertex(Direction.IN);
      Vertex newV = importVertex((String) v.getId());
      importEdge((String) e.getId());
      //            GraphImportUtils.importEdge(equivGraph, e, graphId+":"+e.getId(), Direction.OUT, newParent, newV);

      addNeighborsOf(v, newV, hops - 1);
    }
    for (Edge e : parent.getEdges(Direction.IN)) {
      Vertex v = e.getVertex(Direction.OUT);
      Vertex newV = importVertex((String) v.getId());
      importEdge((String) e.getId());
      //            GraphImportUtils.importEdge(equivGraph, e, graphId+":"+e.getId(), Direction.IN, newParent, newV);

      addNeighborsOf(v, newV, hops - 1);
    }
  }

  ///

  public void importGraph(String srcGraphId, int commitFreq) {
    IdGraph<?> from = srcGraph;

    // TODO: 3: currently copies METADATA nodes from source graphs, which can make graph dirty
    
    int tx = GraphTransaction.begin(graph, commitFreq);
    try {
      for (final Vertex fromVertex : from.getVertices()) {
        importVertex(fromVertex);
        GraphTransaction.commitIfFull(tx);
      }

      for (final Edge fromEdge : from.getEdges()) {
        importEdge(fromEdge, null, null);
        GraphTransaction.commitIfFull(tx);
      }

      GraphTransaction.commit(tx);
    } catch (RuntimeException re) {
      GraphTransaction.rollback(tx);
      throw re;
    }
  }

}
