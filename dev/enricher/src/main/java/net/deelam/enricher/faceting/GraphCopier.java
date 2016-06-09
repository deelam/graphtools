package net.deelam.enricher.faceting;

import static com.google.common.base.Preconditions.*;

import java.io.FileNotFoundException;
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

  private IdGraph<?> srcGraph=null;
  private final GraphUri srcGraphUri;
  private GraphUri srcGraphUriToShutdown = null;
  
  private IdGraph<?> getSrcGraph(){
    if(srcGraph==null){
      if (srcGraphUri.isOpen()) {
        srcGraph = srcGraphUri.getGraph();
        // do not close graph since I didn't open it
      } else {
        try {
          srcGraph = srcGraphUri.openExistingIdGraph();
        } catch (FileNotFoundException e) {
          log.error("Cannot find source graph: "+srcGraphUri, e);
        }
        srcGraphUriToShutdown = srcGraphUri;//so that graph can be closed
      }
    }
    return srcGraph;
  }

  @Override
  public void close() throws IOException {
    dstGraphUri.shutdown();
    if (srcGraphUriToShutdown != null)
      srcGraphUriToShutdown.shutdown();
  }

  // for opening existing graph created by GraphCopier
  public GraphCopier(GraphUri dstGraphUri) throws IOException {
    this(null, dstGraphUri);
  }
  
  public GraphCopier(GraphUri sourceGraphUri, GraphUri dstGraphUri) throws IOException {
    this.dstGraphUri = dstGraphUri;
    merger = dstGraphUri.createPropertyMerger();
    
    if(dstGraphUri.exists()){
      graph = dstGraphUri.getOrOpenGraph(); //openExistingIdGraph(); // graph will be open here so that close() can call shutdown()
      origIdPropKey=GraphUtils.getMetaData(graph, METADATA_ORIGID_PROPKEY);
      final String sourceGraphUriStr = getSourceGraphUri();
      if(sourceGraphUri==null){
        if(sourceGraphUriStr==null)
          throw new IllegalStateException(SRC_GRAPHURI+" property value of MetaData node is null!");
        srcGraphUri=new GraphUri(sourceGraphUriStr).readOnly();
      } else {
        if(!sourceGraphUri.asString().equals(sourceGraphUriStr))
          log.warn("Stored srcGraphUri != given parameter: {} != {}", sourceGraphUriStr, sourceGraphUri);
        srcGraphUri=sourceGraphUri;
      }
    } else {
      graph=dstGraphUri.createNewIdGraph(false);
      GraphUtils.setMetaData(graph, GraphUtils.GRAPHBUILDER_PROPKEY, this.getClass().getSimpleName());
      GraphUtils.setMetaData(graph, SRC_GRAPHURI, sourceGraphUri.asString());
      srcGraphUri=sourceGraphUri;
      addEdgeFromSrcMetaDataNode(getSrcGraph());
      importMetadataSubgraph();
    }
  }
  
  private static final String SRC_GRAPHURI = "_SRC_GRAPHURI_";
  
  public String getSourceGraphUri(){
    return GraphUtils.getMetaData(graph, SRC_GRAPHURI);
  }

  private void addEdgeFromSrcMetaDataNode(IdGraph<?> srcGraph) {
    Vertex mdV = GraphUtils.getMetaDataNode(graph);
    Vertex srcGraphMdV=GraphUtils.getMetaDataNode(srcGraph);
    String srcGraphUri = srcGraphMdV.getProperty(GraphUtils.GRAPHURI_PROP);
    Vertex importedSrcGraphMdV = importVertexWithId(srcGraphMdV, srcGraphUri, false);
    
    String edgeId=srcGraphUri+"->"+mdV.getProperty(GraphUtils.GRAPHURI_PROP);
    if(graph.getEdge(edgeId)==null){
      Edge importedEdge = graph.addEdge(edgeId, importedSrcGraphMdV, mdV, "inputGraphTo");
      importedEdge.setProperty(GraphUtils.GRAPH_METADATA_PROP, true);
    }
  }
  
  private void importMetadataSubgraph() {
    Vertex srcGraphMdV = GraphUtils.getMetaDataNode(getSrcGraph());

    int tx = GraphTransaction.begin(graph);
    try {
      String graphUri = srcGraphMdV.getProperty(GraphUtils.GRAPHURI_PROP);
      Vertex importedMdV = importVertexWithId(srcGraphMdV, graphUri, false);

      importSubgraphUsingOrigId(srcGraphMdV, importedMdV);
      GraphTransaction.commit(tx);
    } catch (RuntimeException re) {
      GraphTransaction.rollback(tx);
      throw re;
    }
  }
  
  public Vertex importVertex(String nodeId) {
    Vertex v = getSrcGraph().getVertex(nodeId);
    checkNotNull(v, "Cannot find nodeId=" + nodeId + " in srcGraph");
    return importVertex(v);
  }

  public Edge importEdge(Edge e) {
    return importEdge(e, null, null);
  }

  public Edge importEdge(String edgeId) {
    Edge e = getSrcGraph().getEdge(edgeId);
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
    Edge e = getSrcGraph().getEdge(edgeId);
    checkNotNull(e, "Cannot find edgeId=" + edgeId + " in srcGraph");
    return importEdge(e, importedOutV, importedInV);
  }

  private final PropertyMerger merger;

  public Vertex importVertex(Vertex v) {
    String nodeId = (String) v.getId();
    return importVertexWithId(v, nodeId, false);
  }
  
  public Vertex importVertexWithDifferentId(Vertex v, Object nodeId) {
    return importVertexWithId(v, nodeId, true);
  }
  
  private static final String METADATA_ORIGID_PROPKEY="__origId";
  private String origIdPropKey=null;
  
  public void setOrigIdPropKey(String originalIdPropKeyPrefix) {
    if(origIdPropKey!=null)
      throw new IllegalStateException("OrigIdPropKey already set to "+origIdPropKey);
    if(origIdPropKey==null){
      origIdPropKey=null;
    } else {
      origIdPropKey=originalIdPropKeyPrefix+PropertyMerger.VALUELIST_SUFFIX;
      GraphUtils.setMetaData(graph, METADATA_ORIGID_PROPKEY, origIdPropKey);
    }
  }
  
  public String getOrigId(Vertex v){
    String origId=null;
    if(origIdPropKey!=null)
      origId = v.getProperty(origIdPropKey);
    // may be null if id wasn't changed (i.e., importVertexWithDifferentId() wasn't called)
    if(origId==null)
      return (String) v.getId();
    return origId;
  }
  
  private Vertex importVertexWithId(Vertex v, Object nodeId, boolean storeOrigNodeId) {
    Vertex newV = graph.getVertex((String) nodeId);
    if (newV == null) {
      newV = graph.addVertex(nodeId);
      merger.mergeProperties(v, newV);
      if(storeOrigNodeId){
        if(origIdPropKey==null)
          throw new IllegalStateException("OrigIdPropKey not set; call setOrigIdPropKey() first");
        newV.setProperty(origIdPropKey, v.getId());
      }
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
    Vertex v = getSrcGraph().getVertex(nodeStringId);
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
    IdGraph<?> from = getSrcGraph();

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
        importedRootV=importVertexWithId(rootV, rootV.getId(), false);
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
          Vertex importedOppV=importVertexWithId(oppV, oppV.getId(), false);
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
