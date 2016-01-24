package net.deelam.graphtools.importer.csv;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CsvUtils {
  
  public static String getFirstField(String rowStr, char delimiter) {
    int firstDelim = rowStr.indexOf(delimiter);
    if (firstDelim < 0)
      return null;
    String field1 = rowStr.substring(0, firstDelim).trim();
    return field1;
  }
  
  public static String getSecondField(String rowStr, char delimiter) {
    int firstDelim = rowStr.indexOf(delimiter);
    if (firstDelim < 0)
      return null;
    
    int secondDelim = rowStr.indexOf(delimiter,firstDelim+1);
    if (secondDelim < 0)
      return null;
    
    String field1 = rowStr.substring(firstDelim+1, secondDelim).trim();
    return field1;
  }

  static boolean shouldIgnore(String rowStr, long lineNum, CsvParser<?> parser) {
    if (rowStr == null)
      return true;
  
    if (rowStr.length() == 0) {
      log.info("Skipping blank line (at line {})", lineNum);
      return true;
    }
    if (rowStr.startsWith("#")) {
      log.info("Skipping comment (at line {}): {}", lineNum, rowStr);
      return true;
    }
  
    if (parser.shouldIgnore(rowStr)) {
      log.warn("Ignoring line (at line {}): {}", lineNum, rowStr);
      return true;
    }
    return false;
  };

}
