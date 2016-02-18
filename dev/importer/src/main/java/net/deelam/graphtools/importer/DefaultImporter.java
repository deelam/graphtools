/**
 * 
 */
package net.deelam.graphtools.importer;

import java.io.IOException;
import java.util.Collection;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.deelam.graphtools.GraphRecord;
import net.deelam.graphtools.GraphTransaction;
import net.deelam.graphtools.GraphUri;

/**
 * Given sourceData, iterates through ioRecord of type B 
 * and populates provided graph.
 * @author deelam
 */
@Slf4j
public class DefaultImporter<B> implements Importer<B> {

  @Getter
  private final Encoder<B> encoder;
  @Getter
  private final Populator populator;
  @Getter
  private final GraphRecordBuilder<B> grBuilder;

  @Setter
  private int commitThreshold=1000;

  public DefaultImporter(Encoder<B> encoder, Populator populator) {
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
      B inRecord;
      while ((inRecord = sourceData.getNextRecord()) != null) {
        log.debug("-------------- row={}", inRecord);
        Collection<GraphRecord> gRecords = grBuilder.build(inRecord);
        log.debug("graphRecords=", gRecords);
        gRecCounter += gRecords.size();
        populator.populateGraph(graphUri, gRecords);
        if (gRecCounter > commitThreshold) {
          GraphTransaction.commit(tx);
          tx = GraphTransaction.begin(graphUri.getGraph());
          gRecCounter = 0;
        }
      }
      GraphTransaction.commit(tx);
    } catch (RuntimeException re) {
      GraphTransaction.rollback(tx);
      throw re;
    } finally {
      graphUri.shutdown();
      encoder.close(sourceData);
    }
  }

}
