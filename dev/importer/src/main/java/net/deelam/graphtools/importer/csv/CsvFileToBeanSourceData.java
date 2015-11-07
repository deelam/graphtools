/**
 * 
 */
package net.deelam.graphtools.importer.csv;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;

import net.deelam.graphtools.importer.SourceData;

import org.supercsv.exception.SuperCsvException;
import org.supercsv.io.CsvBeanReader;
import org.supercsv.io.ICsvBeanReader;


/**
 * @author deelam
 */
public class CsvFileToBeanSourceData<B> implements SourceData<B> {

  protected final ICsvBeanReader beanReader;
  protected final CsvParser<B> parser;

  public CsvFileToBeanSourceData(File file, CsvParser<B> parser) throws FileNotFoundException {
    Reader fileReader = new BufferedReader(new FileReader(file));
    beanReader = new CsvBeanReader(fileReader, parser.getCsvPreferences());

    this.parser = parser;
  }

  @Override
  public B getNextRecord() throws IOException {
    while (true) { // keep reading next row until valid bean or EOF
      try {
        B bean =
            beanReader.read(parser.getBeanClass(), parser.getCsvFields(),
                parser.getCellProcessors());
        return bean; // bean=null if EOF
      } catch (SuperCsvException e) {
        String rowStr = beanReader.getUntokenizedRow();
        if (!CsvLineToBeanSourceData.shouldIgnore(rowStr, beanReader.getLineNumber(), parser))
          throw e;
        //else try reading next line
      }
    }
  }
}
