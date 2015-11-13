/**
 * 
 */
package net.deelam.graphtools.importer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.deelam.graphtools.GraphRecord;
import net.deelam.graphtools.GraphTransaction;

import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

/**
 * Given sourceData, iterates through ioRecord of type B 
 * and populates provided graph each time bufferThreshold (number of ioRecords) is reached.
 * @author deelam
 */
@Slf4j
public class BufferedImporter<B> implements Importer<B> {

  private final Encoder<B> encoder;
  private final Populator populator;
  private final GraphRecordBuilder<B> grBuilder;

  @Setter
  private int bufferThreshold=1000;
  
  public BufferedImporter(Encoder<B> encoder, Populator populator) {
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
      Collection<GraphRecord> gRecordsBuffered=new ArrayList<>(bufferThreshold+100);
      B inRecord;
      while ((inRecord = sourceData.getNextRecord()) != null) {
        log.debug("-------------- row={}", inRecord);
        Collection<GraphRecord> gRecords = grBuilder.build(inRecord);
        //log.debug(gRecords.toString());
        gRecCounter += gRecords.size();
        gRecordsBuffered.addAll(gRecords);
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

  private void populateAndCommit(IdGraph<?> graph, int tx, Collection<GraphRecord> gRecordsBuffered) {
    populator.populateGraph(graph, gRecordsBuffered);
    GraphTransaction.commit(tx);
    GraphTransaction.begin(graph); // should be the same tx number
    gRecordsBuffered.clear();
  }

}
