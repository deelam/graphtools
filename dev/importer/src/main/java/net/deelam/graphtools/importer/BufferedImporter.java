/**
 * 
 */
package net.deelam.graphtools.importer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.deelam.graphtools.GraphRecord;
import net.deelam.graphtools.GraphTransaction;
import net.deelam.graphtools.GraphUri;

import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

/**
 * Given sourceData, iterates through ioRecord of type B 
 * and populates provided graph each time bufferThreshold (number of ioRecords) is reached.
 * @author deelam
 */
@Slf4j
public class BufferedImporter<B> implements Importer<B> {

  @Getter
  private final Encoder<B> encoder;
  @Getter
  private final Populator populator;
  @Getter
  private final GraphRecordBuilder<B> grBuilder;

  @Setter
  private int bufferThreshold=1000;
  
  public BufferedImporter(Encoder<B> encoder, Populator populator, GraphRecord.Factory grFactory) {
    super();
    this.encoder = encoder;
    this.populator = populator;
    grBuilder=new GraphRecordBuilder<>(encoder, grFactory);
  }

  @Override
  public void importFile(SourceData<B> sourceData, GraphUri graphUri) throws IOException {
    encoder.reinit(sourceData);
    graphUri.createNewIdGraph(true);
    int tx = GraphTransaction.begin(graphUri.getGraph());
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
          populateAndCommit(graphUri, tx, gRecordsBuffered);
          gRecCounter = 0;
        }
      }
      populateAndCommit(graphUri, tx, gRecordsBuffered);
      GraphTransaction.commit(tx);
    } catch (RuntimeException re) {
      GraphTransaction.rollback(tx);
      throw re;
    } finally {
      graphUri.shutdown();
      encoder.close(sourceData);
    }
  }

  private void populateAndCommit(GraphUri graphUri, int tx, Collection<GraphRecord> gRecordsBuffered) {
    populator.populateGraph(graphUri, gRecordsBuffered);
    GraphTransaction.commit(tx);
    GraphTransaction.begin(graphUri.getGraph()); // should be the same tx number
    gRecordsBuffered.clear();
  }

}
