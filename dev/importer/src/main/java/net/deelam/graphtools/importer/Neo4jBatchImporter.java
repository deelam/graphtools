/**
 * 
 */
package net.deelam.graphtools.importer;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import javax.inject.Inject;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.deelam.graphtools.GraphRecord;
import net.deelam.graphtools.GraphTransaction;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

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
  
  @Inject
  public Neo4jBatchImporter(Encoder<B> encoder, Populator populator) {
    super();
    this.encoder = encoder;
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
            log.info("Incremental graph populate and transaction commit: {}", recordNum);
            populateAndCommit(graph, tx, gRecordsBuffered);
            log.info("  commit done.");
            gRecCounter = 0;
          }
        }catch(Exception e){
          log.info("Skipping record; got exception for recordNum=~"+recordNum+": "+inRecord, e);
        }
      }
      log.info("Last graph populate and transaction commit: {}", recordNum);
      populateAndCommit(graph, tx, gRecordsBuffered);
      GraphTransaction.commit(tx);
      ((Neo4jBatchPopulator)populator).shutdown();
      log.info("  commit done.");
    } catch (RuntimeException re) {
      log.warn("Done reading records but got exception during graph population", re);
      GraphTransaction.rollback(tx);
      throw re;
    } finally {
      encoder.close(sourceData);
    }
  }

  private void populateAndCommit(IdGraph<?> graph, int tx, Map<String, GraphRecord> gRecordsBuffered) {
    populator.populateGraph(graph, gRecordsBuffered.values());
    //GraphTransaction.commit(tx);
    //GraphTransaction.begin(graph); // should be the same tx number
    gRecordsBuffered.clear();
  }

  private void mergeRecords(Map<String, GraphRecord> gRecordsBuffered, Collection<GraphRecord> gRecords) {
    for(GraphRecord gr:gRecords){
      // merges/consolidate root record
      mergeRecord(gRecordsBuffered, gr);
      
      // merge/consolidate other endpoints
//      for(Entry<String, Edge> e:gr.getInEdges().entrySet()){
//        Vertex v = e.getValue().getVertex(Direction.OUT);
//        mergeRecord(gRecordsBuffered, (GraphRecord) v);
//      }
//      for(Entry<String, Edge> e:gr.getOutEdges().entrySet()){
//        Vertex v = e.getValue().getVertex(Direction.IN);
//        mergeRecord(gRecordsBuffered, (GraphRecord) v);
//      }
    }
  }

  private void mergeRecord(Map<String, GraphRecord> gRecordsBuffered, GraphRecord gr) {
    GraphRecord existingGR = gRecordsBuffered.get(gr.getStringId());
    if(existingGR==null){
      gRecordsBuffered.put(gr.getStringId(),gr);
    } else {
      populator.getGraphRecordMerger().merge(gr, existingGR);
    }
  }

}
