package net.deelam.graphtools.importer;

import java.util.function.Function;
import java.util.function.Supplier;

import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.deelam.graphtools.GraphRecordImpl;
import net.deelam.graphtools.PropertyMerger;

@RequiredArgsConstructor
@AllArgsConstructor
@Slf4j
public class ConsolidatingImporterFactory<B> implements ImporterFactory {
  final Supplier<Encoder<B>> encoderFactory;
  final String importerPropertyVal;
  final PropertyMerger propMerger;

  Function<SourceData,Integer> bufferSizeFunction=sd->{
    return 10000;
  };
  
  @Override
  public Importer<B> create(SourceData sd) {
    log.info("Creating ConsolidatingImporter");
    ConsolidatingImporter<B> importer = new ConsolidatingImporter<B>(encoderFactory.get(),
        new DefaultPopulator(importerPropertyVal, new DefaultGraphRecordMerger(propMerger)),
        new GraphRecordImpl.Factory());
    
    Integer bufferSize = bufferSizeFunction.apply(sd);
    log.info("Using bufferSize={}", bufferSize);
    importer.setBufferThreshold(bufferSize.intValue());
    
    return importer;
  }
}
