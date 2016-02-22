package net.deelam.graphtools.importer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.deelam.graphtools.GraphRecordImpl;

@RequiredArgsConstructor
@Slf4j
public class Neo4jBatchImporterFactory<B> implements ImporterFactory{
  final Encoder<B> encoder;
  final String importerPropertyVal;
  
  @Override
  public Importer<B> create() {
    log.info("Creating Neo4jBatchImporter");
    return new Neo4jBatchImporter<B>(encoder, 
        new Neo4jBatchPopulator(importerPropertyVal),
        new GraphRecordImpl.Factory()
    );
  }
}