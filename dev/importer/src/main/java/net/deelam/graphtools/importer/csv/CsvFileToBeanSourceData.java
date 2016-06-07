/**
 * 
 */
package net.deelam.graphtools.importer.csv;

import java.io.*;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import net.deelam.graphtools.importer.SourceData;

import org.supercsv.exception.SuperCsvException;
import org.supercsv.io.CsvBeanReader;
import org.supercsv.io.ICsvBeanReader;


/**
 * @author deelam
 */
@Slf4j
public class CsvFileToBeanSourceData<B> implements SourceData<B> {

  protected final ICsvBeanReader beanReader;
  @Getter
  protected final CsvParser<B> parser;

  private int totalLines;
  public CsvFileToBeanSourceData(File file, CsvParser<B> parser) throws FileNotFoundException {
    try{
      totalLines=countLines(file);
      log.info("{} has {} totalLines", file, totalLines);
    }catch(IOException e){
      e.printStackTrace();
    }
    
    Reader fileReader = new BufferedReader(new FileReader(file));
    beanReader = new CsvBeanReader(fileReader, parser.getCsvPreferences());
    this.parser = parser;
  }

  private static int countLines(File file) throws IOException {
    try(LineNumberReader lnr=new LineNumberReader(new FileReader(file))){
      while(lnr.skip(Long.MAX_VALUE)>0){}
      int lineCount=lnr.getLineNumber();
      if(lineCount==0)
        return lineCount=1;
      return lineCount;
    }
  }

  @Override
  public String toString() {
    return "parser's beanClass="+parser.getBeanClass().getSimpleName();
  }
  static Object staticToken=new StringBuilder("CsvBeanReader is not thread safe");
  
  @Override
  public B getNextRecord() throws IOException {
    synchronized(staticToken){
      while (true) { // keep reading next row until valid bean or EOF
        try {
          //log.info(" "+parser.getCsvFields().length+"=?"+parser.getCellProcessors().length+"");
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

  @Override
  public int getPercentProcessed() {
    int percent=0;
    log.debug("line={} total={}", beanReader.getLineNumber(), totalLines);
    if(beanReader.getLineNumber()>0 && totalLines>0)
      percent=beanReader.getLineNumber()*100/totalLines;
    return percent;
  }
}
