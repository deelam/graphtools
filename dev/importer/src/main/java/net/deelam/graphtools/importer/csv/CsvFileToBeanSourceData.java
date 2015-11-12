/**
 * 
 */
package net.deelam.graphtools.importer.csv;

import java.io.*;

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
        if(bean==null) // bean=null if EOF
          return null;
        
        // bean may have been successfully created for a row that should be ignored
        String rowStr = beanReader.getUntokenizedRow();
        if(CsvUtils.shouldIgnore(rowStr, beanReader.getLineNumber(), parser)) {
          continue;
        }else{
          return bean;
        }
      } catch (SuperCsvException e) {
        String rowStr = beanReader.getUntokenizedRow();
        if (!CsvUtils.shouldIgnore(rowStr, beanReader.getLineNumber(), parser))
          throw e;
        //else try reading next line
      }
    }
  }
}
