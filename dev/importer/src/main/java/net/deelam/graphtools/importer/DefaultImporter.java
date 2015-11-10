/**
 * 
 */
package net.deelam.graphtools.importer;

import java.io.IOException;
import java.util.Collection;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.deelam.graphtools.GraphRecord;
import net.deelam.graphtools.GraphTransaction;

import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

/**
 * Given sourceData, iterates through ioRecord of type R 
 * and populates provided graph.
 * @author deelam
 */
@Slf4j
public class DefaultImporter<B> implements Importer<B> {

  private final Encoder<B> encoder;
  private final Populator populator;
  private final GraphRecordBuilder<B> grBuilder;

  @Setter
  private int commitThreshold;

  public DefaultImporter(Encoder<B> encoder, Populator populator) {
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
      B inRecord;
      while ((inRecord = sourceData.getNextRecord()) != null) {
        log.debug("-------------- row=" + inRecord);
        Collection<GraphRecord> gRecords = grBuilder.build(inRecord);
        //log.debug(gRecords.toString());
        gRecCounter += gRecords.size();
        populator.populateGraph(graph, gRecords);
        if (gRecCounter > commitThreshold) {
          GraphTransaction.commit(tx);
          gRecCounter = 0;
        }
      }
      GraphTransaction.commit(tx);
    } catch (RuntimeException re) {
      GraphTransaction.rollback(tx);
      throw re;
    } finally {
      encoder.close(sourceData);
    }
  }

}
