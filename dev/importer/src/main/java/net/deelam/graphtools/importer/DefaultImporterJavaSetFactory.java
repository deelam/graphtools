package net.deelam.graphtools.importer;

import lombok.RequiredArgsConstructor;
import net.deelam.graphtools.GraphRecordImpl;
import net.deelam.graphtools.JavaSetPropertyMerger;
import net.deelam.graphtools.importer.*;

@RequiredArgsConstructor
public class DefaultImporterJavaSetFactory<B> implements ImporterFactory{
  final Encoder<B> encoder;
  final String importerPropertyVal;
  
  @Override
  public net.deelam.graphtools.importer.Importer<B> create() {
    return new DefaultImporter<B>(encoder, 
        new DefaultPopulator(importerPropertyVal, new DefaultGraphRecordMerger(new JavaSetPropertyMerger())),
        new GraphRecordImpl.Factory()
    );
  }
}
