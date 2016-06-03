/**
 * 
 */
package net.deelam.graphtools.importer.csv;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.function.Supplier;

import net.deelam.graphtools.importer.SourceData;
import net.deelam.graphtools.importer.SourceDataFactory;
import lombok.RequiredArgsConstructor;

/**
 * @author deelam
 *
 */
@RequiredArgsConstructor
public class CsvBeanSourceDataFactory<B> implements SourceDataFactory {

  private final Supplier<CsvParser<B>> parserFactory;

  @Override
  public SourceData<B> createFrom(File file) throws FileNotFoundException {
    return new CsvFileToBeanSourceData<B>(file, parserFactory.get());
  }

  @Override
  public SourceData<B> createFrom(Readable readable) {
    return new CsvReadableToBeanSourceData<B>(readable, parserFactory.get());
  }
  
  /**
   * Application is expected to call CsvLineToBeanSourceData.setNextInput() prior to each SourceData.getNextRecord() call.
   * Or call CsvLineToBeanSourceData.parse() directly.
   */
  @Override
  public SourceData<B> create() {
    return new CsvLineToBeanSourceData<B>(parserFactory.get());
  }

}
