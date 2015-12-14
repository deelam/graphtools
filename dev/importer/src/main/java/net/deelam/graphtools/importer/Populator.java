/**
 * 
 */
package net.deelam.graphtools.importer;

import java.util.Collection;

import net.deelam.graphtools.GraphRecord;
import net.deelam.graphtools.GraphRecordMerger;

import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

/**
 * @author deelam
 *
 */
public interface Populator {

  GraphRecordMerger getGraphRecordMerger();
  
  void populateGraph(IdGraph<?> graph, Collection<GraphRecord> gRecords);

}
