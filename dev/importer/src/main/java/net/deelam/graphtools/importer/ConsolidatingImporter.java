/**
 * 
 */
package net.deelam.graphtools.importer;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.deelam.graphtools.GraphRecord;
import net.deelam.graphtools.GraphTransaction;
import net.deelam.graphtools.GraphRecordMerger;

import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

/**
 * Given sourceData, iterates through ioRecord of type B 
 * and merges GraphRecords with same id by calling the provided Merger before it is added to the graph by Populator,
 * then populates provided graph each time bufferThreshold (number of *unique* ioRecords) is reached.
 * @author deelam
 */
@Slf4j
public class ConsolidatingImporter<B> implements Importer<B> {

  private final Encoder<B> encoder;
  private final GraphRecordMerger merger;
  private final Populator populator;
  private final GraphRecordBuilder<B> grBuilder;

  @Setter
  private int bufferThreshold=1000;
  
  public ConsolidatingImporter(Encoder<B> encoder, GraphRecordMerger merger, Populator populator) {
    super();
    this.encoder = encoder;
    this.merger=merger;
    this.populator = populator;
    grBuilder=new GraphRecordBuilder<>(encoder);
  }

  @Override
  public void importFile(SourceData<B> sourceData, IdGraph<?> graph) throws IOException {
    encoder.reinit(sourceData);
    int tx = GraphTransaction.begin(graph);
    try {
      int gRecCounter = 0;
      Map<String,GraphRecord> gRecordsBuffered=new HashMap<>(bufferThreshold+100);
      B inRecord;
      while ((inRecord = sourceData.getNextRecord()) != null) {
        log.debug("-------------- row={}", inRecord);
        Collection<GraphRecord> gRecords = grBuilder.build(inRecord);
        //log.debug(gRecords.toString());
        gRecCounter += gRecords.size();
        
        // merge records before adding to graph
        mergeRecords(gRecordsBuffered, gRecords);
        
        if (gRecCounter > bufferThreshold) {
          populateAndCommit(graph, tx, gRecordsBuffered);
          gRecCounter = 0;
        }
      }
      populateAndCommit(graph, tx, gRecordsBuffered);
      GraphTransaction.commit(tx);
    } catch (RuntimeException re) {
      GraphTransaction.rollback(tx);
      throw re;
    } finally {
      encoder.close(sourceData);
    }
  }

  private void populateAndCommit(IdGraph<?> graph, int tx, Map<String, GraphRecord> gRecordsBuffered) {
    populator.populateGraph(graph, gRecordsBuffered.values());
    GraphTransaction.commit(tx);
    GraphTransaction.begin(graph); // should be the same tx number
    gRecordsBuffered.clear();
  }

  private void mergeRecords(Map<String, GraphRecord> gRecordsBuffered, Collection<GraphRecord> gRecords) {
    for(GraphRecord gr:gRecords){
      GraphRecord existingGR = gRecordsBuffered.get(gr.getStringId());
      if(existingGR==null){
        gRecordsBuffered.put(gr.getStringId(),gr);
      } else {
        merger.merge(gr, existingGR);
      }
    }
  }

}
