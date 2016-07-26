package net.deelam.graphtools.importer;

import java.util.function.Supplier;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.deelam.graphtools.GraphRecordImpl;
import net.deelam.graphtools.JavaSetPropertyMerger;

@RequiredArgsConstructor
@Slf4j
public class DefaultImporterJavaSetFactory<B> implements ImporterFactory{
  final Supplier<Encoder<B>> encoderFactory;
  final String importerPropertyVal;
  
  @Override
  public Importer<B> create(SourceData sd) {
    log.info("Creating DefaultImporter");
    return new DefaultImporter<B>(encoderFactory.get(), 
        new DefaultPopulator(importerPropertyVal, new DefaultGraphRecordMerger(new JavaSetPropertyMerger())),
        new GraphRecordImpl.Factory()
    );
  }
}
