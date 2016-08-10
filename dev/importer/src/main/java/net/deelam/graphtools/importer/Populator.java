/**
 * 
 */
package net.deelam.graphtools.importer;

import java.io.IOException;
import java.util.Collection;

import net.deelam.graphtools.GraphRecord;
import net.deelam.graphtools.GraphRecordMerger;
import net.deelam.graphtools.GraphUri;

/**
 * @author deelam
 *
 */
public interface Populator {

  String IMPORTER_KEY = "_ingester";

  GraphRecordMerger getGraphRecordMerger();
  
  void populateGraph(GraphUri graphUri, Collection<GraphRecord> gRecords) throws IOException;

  void reinit(GraphUri graphUri, SourceData sourceData) throws IOException;

  void shutdown();

}
