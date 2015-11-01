/**
 * 
 */
package graphtools.importer;

import graphtools.GraphRecord;

import java.util.Collection;
import java.util.List;

import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

/**
 * @author dlam
 *
 */
public interface Populator {

	void populateGraph(IdGraph graph, Collection<GraphRecord> gRecords);

}
