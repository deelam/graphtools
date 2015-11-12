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

import com.google.common.io.LineReader;


/**
 * @author deelam
 */
@Slf4j
public class CsvReadableToBeanSourceData<B> implements SourceData<B> {

  private final SequenceStringReader reader = new SequenceStringReader();
  protected final ICsvBeanReader beanReader;

  protected final LineReader lr;
  protected final CsvParser<B> parser;

  public CsvReadableToBeanSourceData(Readable readable, CsvParser<B> parser) {
    beanReader = new CsvBeanReader(reader, parser.getCsvPreferences());
    lr = new LineReader(readable);
    this.parser = parser;
  }

  protected long lineNum = 0;

  @Override
  public B getNextRecord() throws IOException {
    String line;
    do {
      line = lr.readLine();
      ++lineNum;
      if (line == null) {
        log.debug("Reached end of file: {}", lr);
        return null; // end of file
      }
    } while (shouldIgnore(line, lineNum, parser));

    reader.reinit(line);
    B bean =
        beanReader.read(parser.getBeanClass(), parser.getCsvFields(), parser.getCellProcessors());
    return bean;
  }

  static boolean shouldIgnore(String rowStr, long lineNum, CsvParser<?> parser) {
    if (rowStr == null)
      return true;

    if (rowStr.length() == 0) {
      log.warn("Skipping blank line (at line {})", lineNum);
      return true;
    }
    if (rowStr.startsWith("#")) {
      log.warn("Skipping comment (at line {}): {}", lineNum, rowStr);
      return true;
    }

    if (parser.shouldIgnore(rowStr)) {
      log.warn("Ignoring line (at line {}): {}", lineNum, rowStr);
      return true;
    }
    return false;
  }

}
