/**
 * 
 */
package net.deelam.graphtools.importer;

import java.util.Collection;

import net.deelam.graphtools.GraphRecord;
import net.deelam.graphtools.GraphRecordMerger;
import net.deelam.graphtools.GraphUri;

/**
 * @author deelam
 *
 */
public interface Populator {

  GraphRecordMerger getGraphRecordMerger();
  
  void populateGraph(GraphUri graphUri, Collection<GraphRecord> gRecords);

}
