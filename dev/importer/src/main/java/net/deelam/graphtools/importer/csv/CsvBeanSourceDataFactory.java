/**
 * 
 */
package net.deelam.graphtools.importer.csv;

import java.io.File;
import java.io.FileNotFoundException;

import net.deelam.graphtools.importer.SourceData;
import net.deelam.graphtools.importer.SourceDataFactory;
import lombok.RequiredArgsConstructor;

/**
 * @author deelam
 *
 */
@RequiredArgsConstructor
public class CsvBeanSourceDataFactory<B> implements SourceDataFactory {

  private final CsvParser<B> parser;

  @Override
  public SourceData<B> createFrom(File file) throws FileNotFoundException {
    return new CsvFileToBeanSourceData<B>(file, parser);
  }

  @Override
  public SourceData<B> createFrom(Readable readable) {
    return new CsvReadableToBeanSourceData<B>(readable, parser);
  }
  
  /**
   * Application is expected to call CsvLineToBeanSourceData.setNextInput() prior to each SourceData.getNextRecord() call.
   * Or call CsvLineToBeanSourceData.parse() directly.
   */
  @Override
  public SourceData<B> create() {
    return new CsvLineToBeanSourceData<B>(parser);
  }

}
