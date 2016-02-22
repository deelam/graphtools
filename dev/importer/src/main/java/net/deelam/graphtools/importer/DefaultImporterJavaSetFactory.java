package net.deelam.graphtools.importer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.deelam.graphtools.GraphRecordImpl;
import net.deelam.graphtools.JavaSetPropertyMerger;

@RequiredArgsConstructor
@Slf4j
public class DefaultImporterJavaSetFactory<B> implements ImporterFactory{
  final Encoder<B> encoder;
  final String importerPropertyVal;
  
  @Override
  public Importer<B> create() {
    log.info("Creating DefaultImporter");
    return new DefaultImporter<B>(encoder, 
        new DefaultPopulator(importerPropertyVal, new DefaultGraphRecordMerger(new JavaSetPropertyMerger())),
        new GraphRecordImpl.Factory()
    );
  }
}
