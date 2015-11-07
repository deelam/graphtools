/**
 * 
 */
package graphtools;

import org.apache.commons.configuration.Configuration;

import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

/**
 * @author deelam
 *
 */
public interface IdGraphFactory {
	public IdGraph open(Configuration conf);
}
