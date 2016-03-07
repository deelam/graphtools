package net.deelam.enricher.indexing;

import static com.google.common.base.Preconditions.*;

import java.io.IOException;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import net.deelam.graphtools.GraphTransaction;
import net.deelam.graphtools.GraphUri;
import net.deelam.graphtools.GraphUtils;
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
  private final GraphUri dstGraphUri;

  private final IdGraph<?> srcGraph;
  private GraphUri srcGraphUriToShutdown = null;

  @Override
  public void close() throws IOException {
    dstGraphUri.shutdown();
    if (srcGraphUriToShutdown != null)
      srcGraphUriToShutdown.shutdown();
  }

  public GraphCopier(GraphUri srcGraphUri, GraphUri dstGraphUri) throws IOException {
    this.dstGraphUri = dstGraphUri;
    if(dstGraphUri.exists())
      graph = dstGraphUri.getOrOpenGraph(); //openExistingIdGraph(); // graph will be open here so that close() can call shutdown()
    else
      graph=dstGraphUri.createNewIdGraph(false);

    if (srcGraphUri.isOpen()) {
      srcGraph = srcGraphUri.getGraph();
      // do not close graph since I didn't open it
    } else {
      srcGraph = srcGraphUri.openExistingIdGraph();
      srcGraphUriToShutdown = srcGraphUri;//so that graph can be closed
    }
    
    merger = dstGraphUri.createPropertyMerger();
    
    addEdgeFromSrcMetaDataNode(srcGraph);
    importMetadataSubgraph();
  }

  private void addEdgeFromSrcMetaDataNode(IdGraph<?> srcGraph) {
    Vertex mdV = GraphUtils.getMetaDataNode(graph);
    Vertex srcGraphMdV=GraphUtils.getMetaDataNode(srcGraph);
    String srcGraphUri = srcGraphMdV.getProperty(GraphUtils.GRAPHURI_PROP);
    Vertex importedSrcGraphMdV = graph.getVertex(srcGraphUri);
    if(importedSrcGraphMdV==null){
      importedSrcGraphMdV = importVertexWithId(srcGraphMdV, srcGraphUri);
      String edgeId=srcGraphUri+"->"+mdV.getProperty(GraphUtils.GRAPHURI_PROP);
      if(graph.getEdge(edgeId)==null){
        Edge importedEdge = graph.addEdge(edgeId, importedSrcGraphMdV, mdV, "inputGraphTo");
        importedEdge.setProperty(GraphUtils.GRAPH_METADATA_PROP, true);
      }
    }
  }
  
  private void importMetadataSubgraph() {
    Vertex srcGraphMdV = GraphUtils.getMetaDataNode(srcGraph);

    int tx = GraphTransaction.begin(graph);
    try {
      String graphUri = srcGraphMdV.getProperty(GraphUtils.GRAPHURI_PROP);
      Vertex importedMdV = importVertexWithId(srcGraphMdV, graphUri);

      importSubgraphUsingOrigId(srcGraphMdV, importedMdV);
      GraphTransaction.commit(tx);
    } catch (RuntimeException re) {
      GraphTransaction.rollback(tx);
      throw re;
    }
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
    return importVertexWithId(v, nodeId);
  }
  
  public Vertex importVertexWithId(Vertex v, Object nodeId) {
    Vertex newV = graph.getVertex((String) nodeId);
    if (newV == null) {
      newV = graph.addVertex(nodeId);
      merger.mergeProperties(v, newV);
    }
    return newV;
  }

  public Edge importEdge(Edge e, Vertex outV, Vertex inV) {
    String newId = (String) e.getId();
    return importEdgeWithId(e, outV, inV, newId);
  }
  
  public Edge importEdgeWithId(Edge e, Vertex outV, Vertex inV, Object newId) {
    Edge newE = graph.getEdge(newId);
    if (newE == null) {
      if (outV == null)
        outV = importVertex(e.getVertex(Direction.OUT));
      if (inV == null)
        inV = importVertex(e.getVertex(Direction.IN));
      newE = graph.addEdge(newId, outV, inV, e.getLabel());
      merger.mergeProperties(e, newE);
    }
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

  public void importGraph(int commitFreq) {
    IdGraph<?> from = srcGraph;

    int tx = GraphTransaction.begin(graph, commitFreq);
    try {
      for (final Vertex fromVertex : from.getVertices()) {
        // skip METADATA nodes from source graphs, which can make graph dirty
        if(fromVertex.getProperty(GraphUtils.GRAPH_METADATA_PROP)==null){
          importVertex(fromVertex);
          GraphTransaction.commitIfFull(tx);
        }else{
          log.info("Skipping metadata node: {}", fromVertex);
        }
      }

      for (final Edge fromEdge : from.getEdges()) {
        if(fromEdge.getProperty(GraphUtils.GRAPH_METADATA_PROP)==null){
          importEdge(fromEdge, null, null);
          GraphTransaction.commitIfFull(tx);
        }else{
          log.info("Skipping metadata edge: {}", fromEdge);
        }
      }

      GraphTransaction.commit(tx);
    } catch (RuntimeException re) {
      GraphTransaction.rollback(tx);
      throw re;
    }
  }

  public void importSubgraphUsingOrigId(Vertex rootV, Vertex importedRootV) {
    int tx = GraphTransaction.begin(graph);
    try {
      if(importedRootV==null){
        importedRootV=importVertexWithId(rootV, rootV.getId());
      }
      recursiveImport(rootV, importedRootV, rootV);
      GraphTransaction.commit(tx);
    } catch (RuntimeException re) {
      GraphTransaction.rollback(tx);
      throw re;
    }
  }
  
  private void recursiveImport(Vertex v, Vertex importedV, Vertex rootV) {
    for (Direction dir : GraphUtils.BOTHDIR)
      for (Edge e : v.getEdges(dir)) {
        Vertex oppV = e.getVertex(dir.opposite());
        boolean alreadyVisited=(graph.getVertex(oppV.getId())!=null);
        if(!rootV.equals(oppV)){
          Vertex importedOppV=importVertexWithId(oppV, oppV.getId());
          if(dir==Direction.OUT)
            importEdgeWithId(e, importedV, importedOppV, e.getId());
          else
            importEdgeWithId(e, importedOppV, importedV, e.getId());
          
          if(!alreadyVisited)
            recursiveImport(oppV, importedOppV, rootV);
        }
      }
  }
}
