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

  public SourceData<B> createFrom(File file) throws FileNotFoundException {
    return new CsvFileToBeanSourceData<B>(file, parser);
  }

  public SourceData<B> createFrom(Readable readable) {
    //Readable readable=new FileReader(file);
    return new CsvLineToBeanSourceData<B>(readable, parser);
  }
}
