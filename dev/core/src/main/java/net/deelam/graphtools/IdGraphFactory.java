/**
 * 
 */
package net.deelam.graphtools;

import org.apache.commons.configuration.Configuration;

import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

/**
 * @author deelam
 *
 */
public interface IdGraphFactory {
  public <T extends KeyIndexableGraph> IdGraph<T> open(Configuration conf);
}
