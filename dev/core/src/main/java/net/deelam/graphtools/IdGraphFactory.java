/**
 * 
 */
package net.deelam.graphtools;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

import lombok.extern.slf4j.Slf4j;

/**
 * @author deelam
 *
 */
public interface IdGraphFactory {
  String READONLY = "openReadOnly";

  public String getScheme();

  default public void init(GraphUri graphUri){}
  
  /**
   * Open existing or create a new graph
   * @param gUri
   * @return
   */
  public <T extends KeyIndexableGraph> IdGraph<T> open(GraphUri gUri);

  /**
   * Delete graph if it exists
   * @param gUri
   * @throws IOException 
   */
  public void delete(GraphUri gUri) throws IOException;

  /**
   * backs up srcGraphUri to dstGraphUri
   * @throws IOException 
   */
  public void backup(GraphUri srcGraphUri, GraphUri dstGraphUri) throws IOException;
    
  /**
   * 
   * @param gUri
   * @return whether graph exists
   */
  public boolean exists(GraphUri gUri);

  default public void shutdown(GraphUri gUri, IdGraph<?> graph) throws IOException{
    graph.shutdown();
  }

  public PropertyMerger createPropertyMerger();

  default public String asString(GraphUri graphUri){
    return graphUri.origUri;
  }

  static Logger log=LoggerFactory.getLogger(IdGraphFactory.class);
  
  default public void createIndices(GraphUri gUri, IdGraph<?> graph, GraphIndexConstants.PropertyKeys pks){
    checkNotNull(graph);
    checkNotNull(pks);
    pks.getVertexKeys().forEach((propKey,params)->{
      if (!graph.getIndexedKeys(Vertex.class).contains(propKey)) {
        log.info("Creating node key index for {} in graph={}", propKey, graph);
        graph.createKeyIndex(propKey, Vertex.class, params);
      }
    });
    pks.getEdgeKeys().forEach((propKey,params)->{
      if (!graph.getIndexedKeys(Edge.class).contains(propKey)) {
        log.info("Creating edge key index for {} in graph={}", propKey, graph);
        graph.createKeyIndex(propKey, Edge.class, params);
      }
    });
    graph.commit();
  }
}
