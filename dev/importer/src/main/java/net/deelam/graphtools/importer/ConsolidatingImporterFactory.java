package net.deelam.graphtools.importer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.deelam.graphtools.GraphRecordImpl;
import net.deelam.graphtools.PropertyMerger;

@RequiredArgsConstructor
@Slf4j
public class ConsolidatingImporterFactory<B> implements ImporterFactory {
  final Encoder<B> encoder;
  final String importerPropertyVal;
  final PropertyMerger propMerger;

  @Override
  public Importer<B> create() {
    log.info("Creating ConsolidatingImporter");
    return new ConsolidatingImporter<B>(encoder,
        new DefaultPopulator(importerPropertyVal, new DefaultGraphRecordMerger(propMerger)),
        new GraphRecordImpl.Factory());
  }
}
