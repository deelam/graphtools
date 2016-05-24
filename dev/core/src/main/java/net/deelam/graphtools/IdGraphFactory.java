/**
 * 
 */
package net.deelam.graphtools;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.function.BiConsumer;

import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

/**
 * @author deelam
 *
 */
public interface IdGraphFactory {
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

  public void shutdown(GraphUri gUri, IdGraph<?> graph) throws IOException;

  public PropertyMerger createPropertyMerger();

}
