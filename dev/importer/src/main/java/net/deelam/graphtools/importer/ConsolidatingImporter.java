/**
 * 
 */
package net.deelam.graphtools.importer;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.deelam.graphtools.GraphRecord;
import net.deelam.graphtools.GraphTransaction;
import net.deelam.graphtools.GraphUri;

/**
 * Given sourceData, iterates through ioRecord of type B 
 * and merges GraphRecords with same id by calling the provided Merger before it is added to the graph by Populator,
 * then populates provided graph each time bufferThreshold (number of *unique* ioRecords) is reached.
 * @author deelam
 */
@Slf4j
public class ConsolidatingImporter<B> implements Importer<B> {

  @Getter
  private final Encoder<B> encoder;
  @Getter
  private final Populator populator;
  @Getter
  private final GraphRecordBuilder<B> grBuilder;

  @Setter
  private int bufferThreshold=10000;
  
  @Inject
  public ConsolidatingImporter(Encoder<B> encoder, Populator populator) {
    super();
    this.encoder = encoder;
    this.populator = populator;
    grBuilder=new GraphRecordBuilder<>(encoder);
  }

  @Override
  public void importFile(SourceData<B> sourceData, GraphUri graphUri) throws IOException {
    encoder.reinit(sourceData);
    graphUri.createNewIdGraph(true);
    int tx = GraphTransaction.begin(graphUri.getGraph());
    try {
      int gRecCounter = 0;
      Map<String,GraphRecord> gRecordsBuffered=new HashMap<>(bufferThreshold+100);
      B inRecord;
      long recordNum=0;
      while ((inRecord = sourceData.getNextRecord()) != null) {
        ++recordNum;
        log.debug("{}: record={}", recordNum, inRecord);
        try{
          Collection<GraphRecord> gRecords = grBuilder.build(inRecord);
          //log.debug(gRecords.toString());
          gRecCounter += gRecords.size();

          // merge records before adding to graph
          mergeRecords(gRecordsBuffered, gRecords);

          if (gRecCounter > bufferThreshold) {
            log.info("Incremental graph populate and transaction commit");
            populateAndCommit(graphUri, tx, gRecordsBuffered);
            log.info("  commit done.");
            gRecCounter = 0;
          }
        }catch(Exception e){
          log.warn("Skipping record; got exception for recordNum=~"+recordNum+": "+inRecord, e);
        }
      }
      log.info("Last graph populate and transaction commit");
      populateAndCommit(graphUri, tx, gRecordsBuffered);
      GraphTransaction.commit(tx);
      log.info("  commit done.");
    } catch (RuntimeException re) {
      log.warn("Done reading records but got exception during graph population", re);
      GraphTransaction.rollback(tx);
      throw re;
    } finally {
      graphUri.shutdown();
      encoder.close(sourceData);
    }
  }

  private void populateAndCommit(GraphUri graphUri, int tx, Map<String, GraphRecord> gRecordsBuffered) {
    populator.populateGraph(graphUri, gRecordsBuffered.values());
    GraphTransaction.commit(tx);
    GraphTransaction.begin(graphUri.getGraph()); // should be the same tx number
    gRecordsBuffered.clear();
  }

  private void mergeRecords(Map<String, GraphRecord> gRecordsBuffered, Collection<GraphRecord> gRecords) {
    for(GraphRecord gr:gRecords){
      GraphRecord existingGR = gRecordsBuffered.get(gr.getStringId());
      if(existingGR==null){
        gRecordsBuffered.put(gr.getStringId(),gr);
      } else {
        populator.getGraphRecordMerger().merge(gr, existingGR);
      }
    }
  }

}
