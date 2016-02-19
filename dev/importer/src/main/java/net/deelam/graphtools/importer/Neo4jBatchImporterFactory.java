package net.deelam.graphtools.importer;

import lombok.RequiredArgsConstructor;
import net.deelam.graphtools.GraphRecordImpl;

@RequiredArgsConstructor
public class Neo4jBatchImporterFactory<B> implements ImporterFactory{
  final Encoder<B> encoder;
  final String importerPropertyVal;
  
  @Override
  public Importer<B> create() {
    return new Neo4jBatchImporter<B>(encoder, 
        new Neo4jBatchPopulator(importerPropertyVal),
        new GraphRecordImpl.Factory()
    );
  }
}