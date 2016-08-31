/**
 * 
 */
package net.deelam.graphtools.importer;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.deelam.graphtools.GraphRecord;
import net.deelam.graphtools.GraphUri;

/**
 * Given sourceData, iterates through ioRecord of type B 
 * and merges GraphRecords with same id by calling the provided Merger before it is added to the graph by Populator,
 * then populates provided graph each time bufferThreshold (number of *unique* ioRecords) is reached.
 * @author deelam
 */
@Slf4j
public class Neo4jBatchImporter<B> implements Importer<B> {

  @Getter
  private final Encoder<B> encoder;
  @Getter
  private final Populator populator;
  @Getter
  private final GraphRecordBuilder<B> grBuilder;

  @Setter
  private int bufferThreshold=200000; // TODO: 4: create benchmark to determine best threshold given installation
  
  public Neo4jBatchImporter(Encoder<B> encoder, Populator populator, GraphRecord.Factory grFactory) {
    this.encoder = encoder;
    this.populator = populator;
    grBuilder=new GraphRecordBuilder<>(encoder, grFactory);
  }
  
  @Override
  public String toString() {
    return "importer's encoder="+encoder.getClass().getSimpleName();
  }

  @Override
  public void importFile(SourceData<B> sourceData, GraphUri graphUri, Map<String, Number> metrics) throws IOException {
    encoder.reinit(sourceData);
    populator.reinit(graphUri, sourceData);
    AtomicLong recordCounter = new AtomicLong();
    AtomicLong recordMergeCounter = new AtomicLong();
    AtomicLong createdCounter = new AtomicLong();
    {
      metrics.put("RECORDS", recordCounter);
      metrics.put("RECORDS_MERGED", recordMergeCounter);
      metrics.put("ELEMENTS_CREATED", createdCounter);
    }

    int gRecCounter = 0;
    long recordNum=0;
    try {
      Map<String,GraphRecord> gRecordsBuffered=new HashMap<>(bufferThreshold+100);
      while (true) {
        //log.info("{} {}", inRecord.getClass(), ((net.deelam.graphtools.importer.csv.CsvFileToBeanSourceData) sourceData).getParser().getBeanClass().getSimpleName());
        ++recordNum;
        try{
          B inRecord = sourceData.getNextRecord();
          if(inRecord == null)
            break;
          log.debug("{}: record={}", recordNum, inRecord);
          recordCounter.incrementAndGet();
          Collection<GraphRecord> gRecords = grBuilder.build(inRecord);
          //log.debug(gRecords.toString());
          int grSize = gRecords.size();
          gRecCounter += grSize;
          createdCounter.addAndGet(grSize);
          
          // merge records before adding to graph
          mergeRecords(gRecordsBuffered, gRecords, recordMergeCounter);

          if (gRecCounter > bufferThreshold) {
            log.debug("Incremental graph populate and transaction commit: {}", recordNum);
            populateAndCommit(graphUri, gRecordsBuffered);
            log.debug("  commit done.");
            gRecCounter = 0;
          }
        }catch(Exception e){
          log.info("Skipping record; got exception for recordNum=~"+recordNum+": ", e);
        }
      }
      log.debug("Last graph populate and transaction commit: {}", recordNum);
      populateAndCommit(graphUri, gRecordsBuffered);
      log.debug("  commit done.");
      log.info("Importer counts: {} records", recordNum);
    } catch (RuntimeException re) {
      log.warn("Done reading "+recordNum+" records but got exception during graph population", re);
      throw re;
    } finally {
      populator.shutdown();
      encoder.close(sourceData);
    }
  }

  private void populateAndCommit(GraphUri graphUri, Map<String, GraphRecord> gRecordsBuffered) throws IOException {
    populator.populateGraph(graphUri, gRecordsBuffered.values());
    gRecordsBuffered.clear();
  }

  private void mergeRecords(Map<String, GraphRecord> gRecordsBuffered, Collection<GraphRecord> gRecords, AtomicLong recordMergeCounter) {
    for(GraphRecord gr:gRecords){
      // merges/consolidate root record
      GraphRecord existingGR = gRecordsBuffered.get(gr.getStringId());
      if(existingGR==null){
        gRecordsBuffered.put(gr.getStringId(),gr);
      } else {
        populator.getGraphRecordMerger().merge(gr, existingGR);
        recordMergeCounter.incrementAndGet();
      }
    }
  }

}
