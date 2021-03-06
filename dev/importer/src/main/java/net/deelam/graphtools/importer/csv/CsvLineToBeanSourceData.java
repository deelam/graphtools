/**
 * 
 */
package net.deelam.graphtools.importer.csv;

import java.io.IOException;

import lombok.extern.slf4j.Slf4j;
import net.deelam.graphtools.importer.SequenceStringReader;
import net.deelam.graphtools.importer.SourceData;

import org.supercsv.io.CsvBeanReader;
import org.supercsv.io.ICsvBeanReader;


/**
 * @author deelam
 */
@Slf4j
public class CsvLineToBeanSourceData<B> implements SourceData<B> {

  private final SequenceStringReader reader = new SequenceStringReader();
  protected final ICsvBeanReader beanReader;

  protected final CsvParser<B> parser;

  public CsvLineToBeanSourceData(CsvParser<B> parser) {
    beanReader = new CsvBeanReader(reader, parser.getCsvPreferences());
    this.parser = parser;
  }

  protected long lineNum = 0;
  
  private String line;
  public void setNextInput(String nextLine){
    if(line!=null){
      log.error("Previous line was not done processing! "+line);
    }
    ++lineNum;
    this.line=nextLine;
  }

  /**
   * Expects setNextInput() be called prior to calling getNextRecord().
   * Alternatively, use parse() directly.
   */
  @Override
  public B getNextRecord() throws IOException {
    B bean=parse(line);
    line=null;
    return bean;
  }
  
  public B parse(String line) throws IOException{
    ++lineNum;
    if (line == null) {
      log.debug("Reached end of file.");
      return null; // end of file
    }
    
    if(CsvUtils.shouldIgnore(line, lineNum, parser)){
      return null;
    }

    reader.reinit(line);
    B bean = beanReader.read(parser.getBeanClass(), parser.getCsvFields(), parser.getCellProcessors());
    return bean;
  }

  @Override
  public int getPercentProcessed() {
    return -1;
  }

}
