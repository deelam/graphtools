package net.deelam.graphtools.importer;

import java.util.function.Supplier;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.deelam.graphtools.GraphRecordImpl;

@RequiredArgsConstructor
@Slf4j
public class Neo4jBatchImporterFactory<B> implements ImporterFactory{
  final Supplier<Encoder<B>> encoderFactory;
  final String importerPropertyVal;
  
  @Override
  public Importer<B> create(SourceData sd) {
    Encoder<B> encoder = encoderFactory.get();
    log.info("Creating Neo4jBatchImporter: "+encoder);
    return new Neo4jBatchImporter<B>(encoder, 
        new Neo4jBatchPopulator(importerPropertyVal),
        new GraphRecordImpl.Factory()
    );
  }
}